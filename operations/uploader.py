"""
쿠팡 WING API 상품 등록기
===========================
기존 CSV 생성 로직(coupang_csv_generator.py)을 API JSON 포맷으로 변환하여
WING API를 통해 직접 상품 등록

사용법:
    from operations.uploader import CoupangAPIUploader
    from core.api.wing_client import CoupangWingClient

    client = CoupangWingClient(vendor_id, access_key, secret_key)
    uploader = CoupangAPIUploader(client)
    result = uploader.upload_product(product_data, outbound_code, return_code)
"""
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional

import re

from core.api.wing_client import CoupangWingClient, CoupangWingError
from core.constants import (
    BOOK_PRODUCT_DEFAULTS, BOOK_CATEGORY_CODE,
    DEFAULT_STOCK,
    determine_delivery_charge_type,
)

# 캐시 디렉토리
CACHE_DIR = Path(__file__).parent.parent / "cache"
CATEGORY_CACHE_FILE = CACHE_DIR / "category_cache.json"

# 쿠팡 바코드로 사용 가능한 ISBN-13 패턴 (978/979로 시작하는 13자리)
_VALID_BARCODE_RE = re.compile(r'^97[89]\d{10}$')

# 검색태그 허용 특수문자 (이외 특수문자 제거)
_SEARCH_TAG_ALLOWED_SPECIAL = set("!@#$%^&*-+;:'.")
_SEARCH_TAG_STRIP_RE = re.compile(r'[^\w\s!@#$%^&*\-+;:\'.]+', re.UNICODE)

logger = logging.getLogger(__name__)

# 반품/교환 기본 정보 (모든 계정 공통)
DEFAULT_RETURN_INFO = {
    "returnChargeName": "북코리아",
    "companyContactNumber": "031-917-0864",
    "returnZipCode": "10417",
    "returnAddress": "경기도 고양시 일산서구 일산로 228",
    "returnAddressDetail": "지층 일마트",
}


class CategoryCache:
    """
    파일 기반 카테고리 캐시

    인메모리 + 파일 영구 저장으로 세션 간 캐시 유지
    """

    def __init__(self, cache_file: Path = CATEGORY_CACHE_FILE):
        self.cache_file = cache_file
        self._cache: Dict[str, str] = {}
        self._dirty = False
        self._load()

    def _load(self):
        """파일에서 캐시 로드"""
        try:
            if self.cache_file.exists():
                with open(self.cache_file, "r", encoding="utf-8") as f:
                    self._cache = json.load(f)
                logger.info(f"카테고리 캐시 로드: {len(self._cache)}개")
        except (json.JSONDecodeError, IOError) as e:
            logger.warning(f"캐시 로드 실패: {e}")
            self._cache = {}

    def save(self):
        """캐시를 파일에 저장"""
        if not self._dirty:
            return

        try:
            self.cache_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self.cache_file, "w", encoding="utf-8") as f:
                json.dump(self._cache, f, ensure_ascii=False, indent=2)
            self._dirty = False
            logger.debug(f"카테고리 캐시 저장: {len(self._cache)}개")
        except IOError as e:
            logger.error(f"캐시 저장 실패: {e}")

    def get(self, key: str) -> Optional[str]:
        """캐시에서 카테고리 코드 조회"""
        return self._cache.get(key)

    def set(self, key: str, value: str):
        """캐시에 카테고리 코드 저장"""
        self._cache[key] = value
        self._dirty = True

    def __contains__(self, key: str) -> bool:
        return key in self._cache

    def __len__(self) -> int:
        return len(self._cache)


class CoupangAPIUploader:
    """쿠팡 WING API를 통한 상품 등록"""

    # 클래스 레벨 캐시 (전체 인스턴스 공유)
    _category_cache: Optional[CategoryCache] = None

    def __init__(self, client: CoupangWingClient, vendor_user_id: str = ""):
        self.client = client
        self.vendor_user_id = vendor_user_id

        # 클래스 레벨 캐시 초기화 (최초 1회만)
        if CoupangAPIUploader._category_cache is None:
            CoupangAPIUploader._category_cache = CategoryCache()

    @property
    def category_cache(self) -> CategoryCache:
        """카테고리 캐시 접근"""
        return CoupangAPIUploader._category_cache

    def recommend_category(self, product_name: str) -> str:
        """
        카테고리 추천 API로 상품 카테고리 코드 조회

        파일 기반 영구 캐시를 활용하여 동일 상품명의 중복 API 호출 방지.
        API 실패 시 기본 카테고리 코드(BOOK_CATEGORY_CODE) 반환.
        """
        cached = self.category_cache.get(product_name)
        if cached:
            return cached

        try:
            result = self.client.recommend_category(product_name)
            data = result.get("data", {})
            code = str(data.get("predictedCategoryId", ""))

            if code and data.get("autoCategorizationPredictionResultType") == "SUCCESS":
                self.category_cache.set(product_name, code)
                self.category_cache.save()  # 즉시 저장
                logger.debug(f"  카테고리 추천: {product_name[:30]} -> {code}")
                return code
        except CoupangWingError as e:
            logger.warning(f"  카테고리 추천 실패: {product_name[:30]} -> {e}")

        self.category_cache.set(product_name, BOOK_CATEGORY_CODE)
        return BOOK_CATEGORY_CODE

    def build_product_payload(
        self,
        product_data: Dict[str, Any],
        outbound_shipping_code: str,
        return_center_code: str,
        category_code: Optional[str] = None,
        return_info: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """
        상품 정보 -> WING API JSON 페이로드 변환

        Args:
            product_data: 상품 정보 딕셔너리
            outbound_shipping_code: 출고지 코드
            return_center_code: 반품지 코드
            category_code: 카테고리 코드 (None이면 추천 API 호출)
            return_info: 반품 주소 정보 (None이면 DEFAULT_RETURN_INFO 사용)
        """
        title = product_data.get("product_name", "")
        publisher = product_data.get("publisher", "")
        author = product_data.get("author", "")
        isbn = product_data.get("isbn", "")
        list_price = product_data.get("original_price", 0)
        sale_price = product_data.get("sale_price", 0)
        image_url = product_data.get("main_image_url", "")
        description = product_data.get("description", "상세페이지 참조")

        # API 문자열 길이 제한 적용
        seller_product_name = title[:100]
        display_product_name = (f"{publisher} {title}" if publisher else title)[:100]
        item_name = title[:150]

        # 카테고리 코드
        if not category_code:
            category_code = self.recommend_category(title)

        # 반품 정보
        ret = return_info or DEFAULT_RETURN_INFO

        # 배송비 설정 — 공급률 + 정가 기반 동적 결정
        margin_rate = int(product_data.get("margin_rate", 65))
        delivery_charge_type, delivery_charge, free_ship_over_amount = \
            determine_delivery_charge_type(margin_rate, list_price)

        # 판매 시작일
        sale_started = datetime.now().strftime("%Y-%m-%dT00:00:00")

        # 상품고시정보 (서적)
        notices = _build_book_notices(title, author, publisher)

        # 필수 속성 + 검색필터 속성
        attributes = _build_book_attributes(isbn, publisher, author, title)

        # 검색 키워드
        search_tags = self._generate_search_tags(product_data)

        # 이미지
        images = []
        if image_url:
            images.append({
                "vendorPath": image_url,
                "imageOrder": 0,
                "imageType": "REPRESENTATION",
            })

        # 상세 설명 HTML
        content_html = _build_content_html(title, author, publisher, description, image_url)

        payload = {
            "displayCategoryCode": int(category_code),
            "sellerProductName": seller_product_name,
            "vendorId": self.client.vendor_id,
            "saleStartedAt": sale_started,
            "saleEndedAt": "2099-12-31T00:00:00",
            "displayProductName": display_product_name,
            "brand": publisher or "기타",
            "generalProductName": title,
            "productGroup": "",
            "deliveryMethod": BOOK_PRODUCT_DEFAULTS["deliveryMethod"],
            "deliveryCompanyCode": "HANJIN",
            "deliveryChargeType": delivery_charge_type,
            "deliveryCharge": delivery_charge,
            "freeShipOverAmount": free_ship_over_amount,
            "deliveryChargeOnReturn": BOOK_PRODUCT_DEFAULTS["deliveryChargeOnReturn"],
            "remoteAreaDeliverable": BOOK_PRODUCT_DEFAULTS["remoteAreaDeliverable"],
            "unionDeliveryType": BOOK_PRODUCT_DEFAULTS["unionDeliveryType"],
            "returnCharge": BOOK_PRODUCT_DEFAULTS["returnCharge"],
            "returnCenterCode": return_center_code,
            "returnChargeName": ret["returnChargeName"],
            "companyContactNumber": ret["companyContactNumber"],
            "returnZipCode": ret["returnZipCode"],
            "returnAddress": ret["returnAddress"],
            "returnAddressDetail": ret["returnAddressDetail"],
            "outboundShippingPlaceCode": int(outbound_shipping_code),
            "vendorUserId": self.vendor_user_id,
            "requested": BOOK_PRODUCT_DEFAULTS["requested"],
            "manufacture": publisher or "기타",
            "bundleInfo": {"bundleType": "SINGLE"},
            "items": [
                {
                    "itemName": item_name,
                    "originalPrice": list_price,
                    "salePrice": sale_price,
                    "maximumBuyCount": 1000,
                    "maximumBuyForPerson": 0,
                    "maximumBuyForPersonPeriod": 1,
                    "outboundShippingTimeDay": BOOK_PRODUCT_DEFAULTS["outboundShippingTimeDay"],
                    "inventoryCount": DEFAULT_STOCK,
                    "unitCount": 1,
                    "adultOnly": BOOK_PRODUCT_DEFAULTS["adultOnly"],
                    "taxType": BOOK_PRODUCT_DEFAULTS["taxType"],
                    "parallelImported": BOOK_PRODUCT_DEFAULTS["parallelImported"],
                    "overseasPurchased": BOOK_PRODUCT_DEFAULTS["overseasPurchased"],
                    "pccNeeded": BOOK_PRODUCT_DEFAULTS["pccNeeded"],
                    "offerCondition": BOOK_PRODUCT_DEFAULTS["offerCondition"],
                    "barcode": isbn if _VALID_BARCODE_RE.match(isbn) else "",
                    "emptyBarcode": not _VALID_BARCODE_RE.match(isbn),
                    "emptyBarcodeReason": "" if _VALID_BARCODE_RE.match(isbn) else "도서 바코드 없음",
                    "modelNo": isbn or "",
                    "externalVendorSku": isbn,
                    "searchTags": search_tags,
                    "images": images,
                    "notices": notices,
                    "attributes": attributes,
                    "certifications": [
                        {
                            "certificationType": "NOT_REQUIRED",
                            "certificationCode": "",
                        }
                    ],
                    "contents": [
                        {
                            "contentsType": "HTML",
                            "contentDetails": [
                                {
                                    "content": content_html,
                                    "detailType": "TEXT",
                                }
                            ],
                        }
                    ],
                }
            ],
        }

        return payload

    def upload_product(self, product_data: Dict, outbound_code: str, return_code: str,
                       dashboard_override: bool = False) -> Dict[str, Any]:
        """
        단일 상품 API 등록 (REGISTER_LOCK 안전장치 적용)

        Returns:
            {"success": bool, "seller_product_id": str, "message": str}
        """
        payload = self.build_product_payload(product_data, outbound_code, return_code)
        product_name = product_data.get("product_name", "")

        try:
            result = self.client.create_product(payload, dashboard_override=dashboard_override)

            # 쿠팡은 200 응답이지만 body에 에러를 담아 보내는 경우가 있음
            code = result.get("code", "")
            if code == "ERROR":
                msg = result.get("message", "알 수 없는 오류")
                logger.error(f"  [FAIL] {product_name[:40]} -> {msg[:200]}")
                return {
                    "success": False,
                    "seller_product_id": "",
                    "message": msg,
                }

            data = result.get("data", "")

            # 중첩 응답: {"code":"200","data":{"code":"SUCCESS","data":427011919}}
            if isinstance(data, dict):
                inner_code = data.get("code", "")
                if inner_code == "ERROR":
                    msg = data.get("message", "알 수 없는 오류")
                    logger.error(f"  [FAIL] {product_name[:40]} -> {msg[:200]}")
                    return {
                        "success": False,
                        "seller_product_id": "",
                        "message": msg,
                    }
                seller_product_id = str(data.get("data", ""))
            else:
                # 평탄 응답: {"code":"SUCCESS","data":427011919}
                seller_product_id = str(data) if data else ""

            logger.info(f"  [OK] {product_name[:40]} -> ID={seller_product_id}")
            return {
                "success": True,
                "seller_product_id": seller_product_id,
                "message": "등록 성공",
            }
        except CoupangWingError as e:
            logger.error(f"  [FAIL] {product_name[:40]} -> {e}")
            return {
                "success": False,
                "seller_product_id": "",
                "message": str(e),
            }

    def build_bundle_payload(
        self,
        bundle_data: Dict[str, Any],
        outbound_shipping_code: str,
        return_center_code: str,
        category_code: Optional[str] = None,
        return_info: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """
        묶음 상품 -> WING API JSON 페이로드 변환

        Args:
            bundle_data: 묶음 상품 정보 딕셔너리
                - bundle_name, publisher, margin_rate, total_list_price,
                  total_sale_price, books: [{title, isbn, author, list_price}],
                  bundle_key
            outbound_shipping_code: 출고지 코드
            return_center_code: 반품지 코드
            category_code: 카테고리 코드 (None이면 추천 API 호출)
            return_info: 반품 주소 정보 (None이면 DEFAULT_RETURN_INFO 사용)
        """
        bundle_name = bundle_data.get("bundle_name", "")
        publisher = bundle_data.get("publisher", "")
        margin_rate = int(bundle_data.get("margin_rate", 65))
        total_list_price = bundle_data.get("total_list_price", 0)
        total_sale_price = bundle_data.get("total_sale_price", 0)
        books = bundle_data.get("books", [])
        bundle_key = bundle_data.get("bundle_key", "")

        # 첫 번째 유효 ISBN (바코드용)
        first_isbn = ""
        for b in books:
            isbn = b.get("isbn", "")
            if _VALID_BARCODE_RE.match(isbn):
                first_isbn = isbn
                break

        # 전체 저자 (중복 제거)
        authors = []
        seen_authors = set()
        for b in books:
            a = b.get("author", "")
            if a and a not in seen_authors:
                authors.append(a)
                seen_authors.add(a)
        all_authors = ", ".join(authors) if authors else ""

        # API 문자열 길이 제한
        seller_product_name = bundle_name[:100]
        display_product_name = (f"{publisher} {bundle_name}" if publisher else bundle_name)[:100]
        item_name = bundle_name[:150]

        # 카테고리 코드
        if not category_code:
            category_code = self.recommend_category(bundle_name)

        # 반품 정보
        ret = return_info or DEFAULT_RETURN_INFO

        # 배송비 설정 — 공급률 + 정가(합산) 기반
        delivery_charge_type, delivery_charge, free_ship_over_amount = \
            determine_delivery_charge_type(margin_rate, total_list_price)

        # 판매 시작일
        sale_started = datetime.now().strftime("%Y-%m-%dT00:00:00")

        # 상품고시정보 (서적)
        notices = _build_book_notices(bundle_name, all_authors, publisher)

        # 필수 속성 (도서 세트여부 = "예")
        attributes = _build_bundle_attributes(first_isbn, publisher, all_authors, bundle_name)

        # 검색 키워드 (묶음 구성 도서에서 추출)
        search_tags = self._generate_bundle_search_tags(bundle_data)

        # 이미지: 묶음은 개별 이미지 없음 (상세 페이지에 목록)
        images = []

        # 상세 설명 HTML (구성 도서 목록 포함)
        content_html = _build_bundle_content_html(bundle_name, all_authors, publisher, books)

        payload = {
            "displayCategoryCode": int(category_code),
            "sellerProductName": seller_product_name,
            "vendorId": self.client.vendor_id,
            "saleStartedAt": sale_started,
            "saleEndedAt": "2099-12-31T00:00:00",
            "displayProductName": display_product_name,
            "brand": publisher or "기타",
            "generalProductName": bundle_name,
            "productGroup": "",
            "deliveryMethod": BOOK_PRODUCT_DEFAULTS["deliveryMethod"],
            "deliveryCompanyCode": "HANJIN",
            "deliveryChargeType": delivery_charge_type,
            "deliveryCharge": delivery_charge,
            "freeShipOverAmount": free_ship_over_amount,
            "deliveryChargeOnReturn": BOOK_PRODUCT_DEFAULTS["deliveryChargeOnReturn"],
            "remoteAreaDeliverable": BOOK_PRODUCT_DEFAULTS["remoteAreaDeliverable"],
            "unionDeliveryType": BOOK_PRODUCT_DEFAULTS["unionDeliveryType"],
            "returnCharge": BOOK_PRODUCT_DEFAULTS["returnCharge"],
            "returnCenterCode": return_center_code,
            "returnChargeName": ret["returnChargeName"],
            "companyContactNumber": ret["companyContactNumber"],
            "returnZipCode": ret["returnZipCode"],
            "returnAddress": ret["returnAddress"],
            "returnAddressDetail": ret["returnAddressDetail"],
            "outboundShippingPlaceCode": int(outbound_shipping_code),
            "vendorUserId": self.vendor_user_id,
            "requested": BOOK_PRODUCT_DEFAULTS["requested"],
            "manufacture": publisher or "기타",
            "bundleInfo": {"bundleType": "SINGLE"},
            "items": [
                {
                    "itemName": item_name,
                    "originalPrice": total_list_price,
                    "salePrice": total_sale_price,
                    "maximumBuyCount": 1000,
                    "maximumBuyForPerson": 0,
                    "maximumBuyForPersonPeriod": 1,
                    "outboundShippingTimeDay": BOOK_PRODUCT_DEFAULTS["outboundShippingTimeDay"],
                    "inventoryCount": DEFAULT_STOCK,
                    "unitCount": 1,
                    "adultOnly": BOOK_PRODUCT_DEFAULTS["adultOnly"],
                    "taxType": BOOK_PRODUCT_DEFAULTS["taxType"],
                    "parallelImported": BOOK_PRODUCT_DEFAULTS["parallelImported"],
                    "overseasPurchased": BOOK_PRODUCT_DEFAULTS["overseasPurchased"],
                    "pccNeeded": BOOK_PRODUCT_DEFAULTS["pccNeeded"],
                    "offerCondition": BOOK_PRODUCT_DEFAULTS["offerCondition"],
                    "barcode": first_isbn,
                    "emptyBarcode": not bool(first_isbn),
                    "emptyBarcodeReason": "" if first_isbn else "도서 세트 바코드 없음",
                    "modelNo": first_isbn or "",
                    "externalVendorSku": bundle_key,
                    "searchTags": search_tags,
                    "images": images,
                    "notices": notices,
                    "attributes": attributes,
                    "certifications": [
                        {
                            "certificationType": "NOT_REQUIRED",
                            "certificationCode": "",
                        }
                    ],
                    "contents": [
                        {
                            "contentsType": "HTML",
                            "contentDetails": [
                                {
                                    "content": content_html,
                                    "detailType": "TEXT",
                                }
                            ],
                        }
                    ],
                }
            ],
        }

        return payload

    def upload_bundle(self, bundle_data: Dict, outbound_code: str, return_code: str,
                      dashboard_override: bool = False) -> Dict[str, Any]:
        """
        묶음 상품 API 등록 (REGISTER_LOCK 안전장치 적용)

        Returns:
            {"success": bool, "seller_product_id": str, "message": str}
        """
        payload = self.build_bundle_payload(bundle_data, outbound_code, return_code)
        bundle_name = bundle_data.get("bundle_name", "")

        try:
            result = self.client.create_product(payload, dashboard_override=dashboard_override)

            code = result.get("code", "")
            if code == "ERROR":
                msg = result.get("message", "알 수 없는 오류")
                logger.error(f"  [FAIL] 묶음 {bundle_name[:40]} -> {msg[:200]}")
                return {
                    "success": False,
                    "seller_product_id": "",
                    "message": msg,
                }

            data = result.get("data", "")

            if isinstance(data, dict):
                inner_code = data.get("code", "")
                if inner_code == "ERROR":
                    msg = data.get("message", "알 수 없는 오류")
                    logger.error(f"  [FAIL] 묶음 {bundle_name[:40]} -> {msg[:200]}")
                    return {
                        "success": False,
                        "seller_product_id": "",
                        "message": msg,
                    }
                seller_product_id = str(data.get("data", ""))
            else:
                seller_product_id = str(data) if data else ""

            logger.info(f"  [OK] 묶음 {bundle_name[:40]} -> ID={seller_product_id}")
            return {
                "success": True,
                "seller_product_id": seller_product_id,
                "message": "등록 성공",
            }
        except CoupangWingError as e:
            logger.error(f"  [FAIL] 묶음 {bundle_name[:40]} -> {e}")
            return {
                "success": False,
                "seller_product_id": "",
                "message": str(e),
            }

    def _generate_bundle_search_tags(self, bundle_data: Dict) -> List[str]:
        """묶음 상품 검색 태그 생성 (구성 도서에서 키워드 추출)"""
        tags = []
        bundle_name = bundle_data.get("bundle_name", "")
        publisher = bundle_data.get("publisher", "")
        books = bundle_data.get("books", [])

        # 출판사
        if publisher:
            tags.append(publisher)

        # 묶음명 키워드
        base_data = {
            "product_name": bundle_name,
            "publisher": publisher,
            "author": "",
            "isbn": "",
        }
        base_tags = self._generate_search_tags(base_data)
        for t in base_tags:
            if t not in tags:
                tags.append(t)

        # 구성 도서 저자 추가
        seen_authors = set()
        for b in books:
            author = b.get("author", "")
            if author:
                clean = re.sub(r'\s*\(.*?\)', '', author).strip()
                if clean and clean not in seen_authors and clean not in tags:
                    seen_authors.add(clean)
                    tags.append(clean)

        # 세트 관련 태그
        set_tags = ["세트", "도서세트", "묶음"]
        for t in set_tags:
            if t not in tags:
                tags.append(t)

        # 중복 제거 + 유효성 검증 + 최대 20개
        seen = set()
        unique_tags = []
        for t in tags:
            t = _SEARCH_TAG_STRIP_RE.sub('', t).strip()[:20]
            if t and t not in seen:
                seen.add(t)
                unique_tags.append(t)
        return unique_tags[:20]

    def upload_batch(
        self,
        products: List[Dict],
        outbound_code: str,
        return_code: str,
    ) -> Dict[str, Any]:
        """
        다수 상품 일괄 등록

        Returns:
            {"total": int, "success": int, "failed": int, "results": list}
        """
        results = []
        success_count = 0
        fail_count = 0

        logger.info(f"일괄 등록 시작: {len(products)}개 상품")

        for i, product_data in enumerate(products, 1):
            product_name = product_data.get("product_name", "")
            logger.info(f"  [{i}/{len(products)}] {product_name[:40]}")

            result = self.upload_product(product_data, outbound_code, return_code)
            results.append(result)

            if result["success"]:
                success_count += 1
            else:
                fail_count += 1

        logger.info(f"일괄 등록 완료: 성공 {success_count}개, 실패 {fail_count}개")

        return {
            "total": len(products),
            "success": success_count,
            "failed": fail_count,
            "results": results,
        }

    def _generate_search_tags(self, product_data: Dict) -> List[str]:
        """
        검색 태그 자동 생성 (최대 20개)

        도서 제목/출판사/저자에서 키워드를 추출하고
        관련 검색어를 조합하여 노출을 극대화
        """
        tags = []
        title = product_data.get("product_name", "")
        publisher = product_data.get("publisher", "")
        author = product_data.get("author", "")
        isbn = product_data.get("isbn", "")

        # 1) 출판사 (가장 중요한 검색어)
        if publisher:
            tags.append(publisher)

        # 2) 저자
        if author:
            # "홍길동 (지은이)" -> "홍길동"
            clean_author = re.sub(r'\s*\(.*?\)', '', author).strip()
            if clean_author and clean_author != publisher:
                tags.append(clean_author)

        # 3) 제목에서 핵심 키워드 추출
        # 불용어 제거
        stopwords = {'the', 'a', 'an', 'of', 'for', 'and', '전', '권', '개', '세트', '셋',
                     '상', '하', '중', '편', '판', '권', '개정', '개정판', '최신판', '신판'}
        title_clean = re.sub(r'[(\[\])\-+/,~·]', ' ', title)
        words = title_clean.split()
        for w in words:
            w = w.strip()
            if len(w) >= 2 and w.lower() not in stopwords and not w.isdigit():
                if w not in tags:
                    tags.append(w)

        # 4) 학년/학교급 태그
        grade_map = {
            '초등': ['초등', '초등학생', '초등교재', '초등문제집'],
            '중등': ['중등', '중학생', '중등교재', '중학교'],
            '중학': ['중등', '중학생', '중학교', '중등교재'],
            '고등': ['고등', '고등학생', '고등교재', '고등학교'],
            '고1': ['고등', '고1', '고등학교1학년'],
            '고2': ['고등', '고2', '고등학교2학년'],
            '고3': ['고등', '고3', '고등학교3학년'],
            '수능': ['수능', '수능대비', '수능교재', '대입'],
            '내신': ['내신', '내신대비', '학교시험'],
            '예비': ['예비', '예비초등', '입학준비'],
        }
        for keyword, related in grade_map.items():
            if keyword in title:
                tags.extend([t for t in related if t not in tags])

        # 5) 과목 태그
        subject_map = {
            '수학': ['수학', '수학문제집', '수학교재', '수학참고서'],
            '국어': ['국어', '국어문제집', '국어교재'],
            '영어': ['영어', '영어교재', '영어공부', 'English'],
            '과학': ['과학', '과학교재', '과학문제집'],
            '사회': ['사회', '사회교재', '사회문제집'],
            '물리': ['물리', '물리학', '과학'],
            '화학': ['화학', '과학'],
            '생물': ['생물', '생명과학', '과학'],
            '지구': ['지구과학', '과학'],
            '한국사': ['한국사', '역사', '국사'],
            '세계사': ['세계사', '역사'],
            '문법': ['문법', '국어문법', '영어문법'],
            '독해': ['독해', '독해력', '읽기'],
            '어휘': ['어휘', '어휘력', '단어'],
            '파닉스': ['파닉스', 'phonics', '영어발음'],
            'phonics': ['파닉스', 'phonics', '영어발음'],
            '미적분': ['미적분', '수학', '고등수학'],
            '확률': ['확률과통계', '확률', '통계', '수학'],
            '기하': ['기하', '수학', '고등수학'],
        }
        for keyword, related in subject_map.items():
            if keyword in title.lower() or keyword in title:
                tags.extend([t for t in related if t not in tags])

        # 6) 시리즈/브랜드명 조합 (출판사+과목)
        if publisher:
            for subj in ['수학', '영어', '국어', '과학', '사회']:
                if subj in title:
                    combo = f"{publisher} {subj}"
                    if combo not in tags:
                        tags.append(combo)
                    break

        # 7) 교재 유형 태그
        type_map = {
            '문제집': ['문제집', '문제풀이'],
            '참고서': ['참고서'],
            '기출': ['기출', '기출문제', '기출문제집'],
            '모의고사': ['모의고사', '모의'],
            '워크북': ['워크북', 'workbook'],
            'workbook': ['워크북', 'workbook'],
            '개념': ['개념서', '개념학습'],
            '유형': ['유형별', '유형문제집'],
            'RPM': ['RPM', '개념원리RPM'],
            '쎈': ['쎈', '쎈수학'],
            '올림피아드': ['올림피아드', '경시대회'],
        }
        for keyword, related in type_map.items():
            if keyword in title:
                tags.extend([t for t in related if t not in tags])

        # 8) 연도 태그
        year_match = re.search(r'20[2-3]\d', title)
        if year_match:
            year = year_match.group()
            tags.extend([t for t in [f"{year}년", f"{year}신간"] if t not in tags])

        # 중복 제거 + 유효성 검증 + 최대 20개
        seen = set()
        unique_tags = []
        for t in tags:
            # 허용되지 않는 특수문자 제거
            t = _SEARCH_TAG_STRIP_RE.sub('', t).strip()
            # 개당 20자 제한
            t = t[:20]
            if t and t not in seen:
                seen.add(t)
                unique_tags.append(t)
        return unique_tags[:20]


def _build_book_notices(title: str, author: str, publisher: str) -> List[Dict]:
    """
    도서 상품고시정보 생성 (서적 카테고리)

    기존 등록 상품에서 확인한 정확한 필드 형식:
    noticeCategoryName = "서적", noticeCategoryDetailName = 문자열
    """
    return [
        {"noticeCategoryName": "서적", "noticeCategoryDetailName": "도서명", "content": title or "상품 상세페이지 참조"},
        {"noticeCategoryName": "서적", "noticeCategoryDetailName": "저자, 출판사", "content": f"{author} / {publisher}" if author or publisher else "상품 상세페이지 참조"},
        {"noticeCategoryName": "서적", "noticeCategoryDetailName": "크기(파일의 용량)", "content": "상품 상세페이지 참조"},
        {"noticeCategoryName": "서적", "noticeCategoryDetailName": "쪽수", "content": "상품 상세페이지 참조"},
        {"noticeCategoryName": "서적", "noticeCategoryDetailName": "제품 구성", "content": "도서"},
        {"noticeCategoryName": "서적", "noticeCategoryDetailName": "발행일", "content": "상품 상세페이지 참조"},
        {"noticeCategoryName": "서적", "noticeCategoryDetailName": "목차 또는 책소개(아동용 학습 교재의 경우 사용연령을 포함)", "content": "상품 상세페이지 참조"},
    ]


def _parse_subject(title: str) -> str:
    """제목에서 학습과목 추출"""
    # 우선순위 순 (구체적 -> 일반적)
    subject_map = [
        # 수학 (구체적 과목명 -> 일반)
        ("미적분", "수학"), ("확률과통계", "수학"), ("확률과 통계", "수학"),
        ("기하", "수학"), ("대수", "수학"), ("공통수학", "수학"),
        ("연산", "수학"), ("구구단", "수학"), ("도형", "수학"),
        ("셈", "수학"), ("곱셈", "수학"), ("나눗셈", "수학"), ("분수", "수학"),
        # 국어 (구체적 -> 일반)
        ("언어와매체", "국어"), ("언어와 매체", "국어"), ("언매", "국어"),
        ("화법과작문", "국어"), ("화법과 작문", "국어"), ("화작", "국어"),
        ("독서", "국어"), ("문학", "국어"), ("문법", "국어"),
        # 과학 (구체적 과목명)
        ("생명과학", "과학"), ("지구과학", "과학"), ("물리학", "과학"),
        ("물리", "과학"), ("화학", "과학"), ("생물", "과학"),
        ("통합과학", "과학"),
        # 사회/역사 (구체적 -> 일반)
        ("한국사", "역사"), ("세계사", "역사"), ("동아시아사", "역사"),
        ("한국지리", "사회"), ("세계지리", "사회"),
        ("경제", "사회"), ("정치와법", "사회"), ("정치와 법", "사회"),
        ("윤리와사상", "사회"), ("윤리와 사상", "사회"),
        ("생활과윤리", "사회"), ("생활과 윤리", "사회"),
        ("사회문화", "사회"), ("사회 문화", "사회"), ("사회·문화", "사회"),
        ("통합사회", "사회"),
        # 일반 과목명
        ("수학", "수학"), ("국어", "국어"), ("영어", "영어"),
        ("과학", "과학"), ("사회", "사회"), ("역사", "역사"),
        # 영어 (다양한 키워드)
        ("독해", "영어"), ("리딩", "영어"), ("Reading", "영어"),
        ("파닉스", "영어"), ("phonics", "영어"), ("Phonics", "영어"),
        ("VOCA", "영어"), ("Voca", "영어"), ("voca", "영어"),
        ("보카", "영어"), ("어휘", "영어"), ("단어", "영어"), ("숙어", "영어"),
        ("grammar", "영어"), ("Grammar", "영어"), ("그래머", "영어"),
        ("listening", "영어"), ("Listening", "영어"), ("리스닝", "영어"),
        ("writing", "영어"), ("Writing", "영어"), ("라이팅", "영어"),
        ("speaking", "영어"), ("Speaking", "영어"), ("스피킹", "영어"),
        ("구문", "영어"), ("영단어", "영어"), ("영문법", "영어"),
        # 전과목/기타
        ("진단평가", "전과목"), ("배치고사", "전과목"), ("방학생활", "전과목"),
        ("전과목", "전과목"),
    ]
    for keyword, subject in subject_map:
        if keyword in title:
            return subject
    return ""


def _parse_grade(title: str) -> str:
    """제목에서 사용학년/단계 추출 (쿠팡 옵션값 형식)"""
    import re as _re

    # 학교급 포함 여부 판별
    has_elem = bool(_re.search(r'초등|초학', title))
    has_mid = bool(_re.search(r'중등|중학|중\d', title))
    has_high = bool(_re.search(r'고등|고교', title))

    # --- 1단계: 명시적 학교급 + 숫자 패턴 ---

    # 초등 N-M (초등 수학 3-1, 초3-1 등)
    m = _re.search(r'초등?\s*(?:\S+\s+)?(\d)[- .](\d)', title)
    if m and has_elem:
        return f"초등 {m.group(1)}-{m.group(2)}"
    m = _re.search(r'초등\s*(\d)[- .](\d)', title)
    if m:
        return f"초등 {m.group(1)}-{m.group(2)}"
    m = _re.search(r'초등\s*(\d)학년', title)
    if m:
        return f"초등 {m.group(1)}"
    m = _re.search(r'초(\d)[- .](\d)', title)
    if m:
        return f"초등 {m.group(1)}-{m.group(2)}"

    # 중등 N-M (중등 수학 1-1, 중학 수학 2-1, 중1-2 등)
    # "중등/중학 + (과목명 등) + N-M" 패턴
    m = _re.search(r'중(?:등|학)\s+\S+\s+(\d)[- .](\d)', title)
    if m:
        return f"중등 {m.group(1)}-{m.group(2)}"
    m = _re.search(r'중(?:등|학)?\s*(\d)[- .](\d)', title)
    if m:
        return f"중등 {m.group(1)}-{m.group(2)}"
    m = _re.search(r'중(?:등|학)?\s*(\d)학년', title)
    if m:
        return f"중등 {m.group(1)}"

    # 고등 N-M
    m = _re.search(r'고(?:등)?\s*(\d)[- .](\d)', title)
    if m:
        return f"고등 {m.group(1)}-{m.group(2)}"
    m = _re.search(r'고(\d)', title)
    if m:
        return f"고등 {m.group(1)}"

    # --- 2단계: 학교급 키워드만 있고 숫자는 별도 ---

    # "예비 N학년"
    m = _re.search(r'예비\s*(\d)학년', title)
    if m:
        g = int(m.group(1))
        if g <= 6:
            return f"초등 {g}"
        return f"중등 {g - 6}"

    # "N학년" (학교급 컨텍스트와 결합)
    m = _re.search(r'(\d)학년', title)
    if m:
        g = int(m.group(1))
        if has_mid and 1 <= g <= 3:
            return f"중등 {g}"
        if has_high and 1 <= g <= 3:
            return f"고등 {g}"
        if g <= 6:
            return f"초등 {g}"

    # 학교급 있지만 학년 없이 N-M만 있는 경우 (바빠 교과서 연산 5-1)
    m = _re.search(r'(\d)[- .]([12])\s*(?:\(|세트|$)', title)
    if m:
        g = int(m.group(1))
        s = m.group(2)
        if has_mid and 1 <= g <= 3:
            return f"중등 {g}-{s}"
        if has_high and 1 <= g <= 3:
            return f"고등 {g}-{s}"
        if g <= 6 and not has_mid and not has_high:
            return f"초등 {g}-{s}"

    # --- 3단계: 수능/예비 ---
    if "수능" in title:
        return "수능"
    if "예비 초등" in title or "예비초등" in title:
        return "예비 초등"
    if "예비 중" in title or "예비중" in title:
        return "예비 중등"

    # --- 4단계: 학교급만 (숫자 없음) ---
    if has_elem:
        return "초등"
    if has_mid:
        return "중등"
    if has_high:
        return "고등"

    # --- 5단계: 고등 전용 과목명으로 추론 ---
    _hs_subjects = [
        "미적분", "확률과통계", "확률과 통계", "기하", "대수", "공통수학",
        "언어와매체", "언어와 매체", "화법과작문", "화법과 작문",
        "생명과학", "지구과학", "물리학", "화학",
        "통합과학", "통합사회",
        "생활과윤리", "생활과 윤리", "윤리와사상", "윤리와 사상",
        "사회문화", "사회 문화", "사회·문화",
        "한국지리", "세계지리", "정치와법", "정치와 법",
        "동아시아사",
    ]
    for hs in _hs_subjects:
        if hs in title:
            return "고등"
    return ""


def _parse_series_name(title: str, publisher: str) -> str:
    """제목에서 시리즈명 추출 (출판사명 제거 후 학년/과목 이전 부분)"""
    import re as _re
    # 출판사명이 제목 앞에 있으면 제거
    t = title
    if publisher and t.startswith(publisher):
        t = t[len(publisher):].strip()
    # 학년 패턴 앞 부분 = 시리즈명
    m = _re.search(r'(초등|중등|중학|고등|고\d|초\d|중\d|수능)', t)
    if m and m.start() > 1:
        series = t[:m.start()].strip().rstrip('-').strip()
        if len(series) >= 2:
            return series[:50]
    return ""


def _parse_semester(title: str) -> str:
    """제목에서 학기구분 추출"""
    import re as _re
    m = _re.search(r'(\d)[- .]([12])\b', title)
    if m:
        return f"{m.group(2)}학기"
    if "1학기" in title:
        return "1학기"
    if "2학기" in title:
        return "2학기"
    return ""


def _dedupe_attributes(attrs: List[Dict]) -> List[Dict]:
    """속성 중복 제거 -- 같은 attributeTypeName이 여러 개면 비어있지 않은 값을 우선 유지"""
    seen: Dict[str, Dict] = {}
    for a in attrs:
        name = a.get("attributeTypeName", "")
        val = a.get("attributeValueName", "")
        if name not in seen or (val and val != ""):
            seen[name] = a
    return list(seen.values())


def _build_book_attributes(isbn: str, publisher: str = "", author: str = "",
                           title: str = "") -> List[Dict]:
    """
    도서 필수 속성 + 검색필터 속성 자동 생성

    필수: 학습과목, 사용학년/단계, ISBN
    검색필터: 시리즈명, 사용연도, 학기구분, 도서세트여부, 도서형태, 발행언어
    """
    import re as _re

    # 자동 파싱
    subject = _parse_subject(title)
    grade = _parse_grade(title)
    series = _parse_series_name(title, publisher)
    semester = _parse_semester(title)

    # 사용연도 추출
    year_match = _re.search(r'(20[2-3]\d)', title)
    use_year = year_match.group(1) if year_match else ""

    attrs = [
        {"attributeTypeName": "학습과목", "attributeValueName": subject or "상세내용 참조"},
        {"attributeTypeName": "사용학년/단계", "attributeValueName": grade or "상세내용 참조"},
        {"attributeTypeName": "ISBN", "attributeValueName": isbn or "상세내용 참조"},
        {"attributeTypeName": "저자", "attributeValueName": author or "상세내용 참조"},
        {"attributeTypeName": "출판사", "attributeValueName": publisher or "상세내용 참조"},
    ]

    # 검색필터 속성 (값이 있을 때만 추가)
    if series:
        attrs.append({"attributeTypeName": "시리즈명", "attributeValueName": series})
    if use_year:
        attrs.append({"attributeTypeName": "사용연도", "attributeValueName": use_year})
    if semester:
        attrs.append({"attributeTypeName": "학기구분", "attributeValueName": semester})
    attrs.append({"attributeTypeName": "도서 세트여부", "attributeValueName": "아니오"})
    attrs.append({"attributeTypeName": "도서형태", "attributeValueName": "종이책"})
    attrs.append({"attributeTypeName": "발행언어", "attributeValueName": "한국어"})
    attrs.append({"attributeTypeName": "e북 포함 여부", "attributeValueName": "아니오"})

    return _dedupe_attributes(attrs)


def _build_bundle_attributes(isbn: str, publisher: str = "", author: str = "",
                              title: str = "") -> List[Dict]:
    """
    묶음 도서 속성 생성 -- 도서 세트여부 = "예"

    단권 속성과 동일하지만 세트여부만 다름
    """
    import re as _re

    subject = _parse_subject(title)
    grade = _parse_grade(title)
    series = _parse_series_name(title, publisher)
    semester = _parse_semester(title)

    year_match = _re.search(r'(20[2-3]\d)', title)
    use_year = year_match.group(1) if year_match else ""

    attrs = [
        {"attributeTypeName": "학습과목", "attributeValueName": subject or "상세내용 참조"},
        {"attributeTypeName": "사용학년/단계", "attributeValueName": grade or "상세내용 참조"},
        {"attributeTypeName": "ISBN", "attributeValueName": isbn or "상세내용 참조"},
        {"attributeTypeName": "저자", "attributeValueName": author or "상세내용 참조"},
        {"attributeTypeName": "출판사", "attributeValueName": publisher or "상세내용 참조"},
    ]

    if series:
        attrs.append({"attributeTypeName": "시리즈명", "attributeValueName": series})
    if use_year:
        attrs.append({"attributeTypeName": "사용연도", "attributeValueName": use_year})
    if semester:
        attrs.append({"attributeTypeName": "학기구분", "attributeValueName": semester})
    attrs.append({"attributeTypeName": "도서 세트여부", "attributeValueName": "예"})
    attrs.append({"attributeTypeName": "도서형태", "attributeValueName": "종이책"})
    attrs.append({"attributeTypeName": "발행언어", "attributeValueName": "한국어"})
    attrs.append({"attributeTypeName": "e북 포함 여부", "attributeValueName": "아니오"})

    return _dedupe_attributes(attrs)


def _build_content_html(title: str, author: str, publisher: str, description: str, image_url: str) -> str:
    """상세 설명 HTML 생성"""
    parts = [f"<h2>{title}</h2>"]

    if author or publisher:
        parts.append(f"<p><b>저자:</b> {author} | <b>출판사:</b> {publisher}</p>")

    if image_url:
        parts.append(f'<p><img src="{image_url}" alt="{title}"></p>')

    if description:
        parts.append(f"<p>{description}</p>")

    return "\n".join(parts)


def _build_bundle_content_html(bundle_name: str, authors: str, publisher: str,
                                books: List[Dict]) -> str:
    """묶음 상품 상세 설명 HTML (구성 도서 목록 포함)"""
    parts = [f"<h2>{bundle_name}</h2>"]

    if authors or publisher:
        parts.append(f"<p><b>저자:</b> {authors} | <b>출판사:</b> {publisher}</p>")

    parts.append(f"<h3>구성 도서 ({len(books)}권)</h3>")
    parts.append("<table border='1' cellpadding='5' cellspacing='0'>")
    parts.append("<tr><th>No.</th><th>도서명</th><th>ISBN</th><th>정가</th></tr>")
    for i, b in enumerate(books, 1):
        title = b.get("title", "")
        isbn = b.get("isbn", "")
        price = b.get("list_price", 0)
        parts.append(f"<tr><td>{i}</td><td>{title}</td><td>{isbn}</td><td>{price:,}원</td></tr>")
    parts.append("</table>")

    total_list = sum(b.get("list_price", 0) for b in books)
    total_sale = int(total_list * 0.9)
    parts.append(f"<p><b>정가 합계:</b> {total_list:,}원 -> <b>판매가:</b> {total_sale:,}원 (10% 할인)</p>")

    return "\n".join(parts)
