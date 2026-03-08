"""
쿠팡 WING Open API 클라이언트
==============================
HMAC-SHA256 인증, Rate Limit(10/sec) 준수, 주요 엔드포인트 지원

사용법:
    client = CoupangWingClient(vendor_id="A00317195", access_key="...", secret_key="...")
    products = client.list_products()
    product = client.get_product(seller_product_id=12345)
"""
import time
import hmac
import hashlib
import urllib.parse
from datetime import datetime, timezone
import logging
from typing import Optional, Dict, Any, List, Callable, Tuple

import requests

logger = logging.getLogger(__name__)

# 재시도 설정
MAX_RETRIES = 3
RETRY_BASE_DELAY = 1.0  # 초
RETRYABLE_STATUS_CODES = (429, 500, 502, 503, 504)


class CoupangWingError(Exception):
    """쿠팡 WING API 오류"""
    def __init__(self, code: str, message: str, status_code: int = 0):
        self.code = code
        self.message = message
        self.status_code = status_code
        super().__init__(f"[{code}] {message}")


class CoupangWingClient:
    """쿠팡 WING Open API 클라이언트"""

    BASE_URL = "https://api-gateway.coupang.com"
    RATE_LIMIT_INTERVAL = 0.1  # 10 calls/sec → 최소 0.1초 간격

    # 엔드포인트 경로
    SELLER_PRODUCTS_PATH = "/v2/providers/seller_api/apis/api/v1/marketplace/seller-products"
    VENDOR_ITEMS_PATH = "/v2/providers/seller_api/apis/api/v1/marketplace/vendor-items"
    CATEGORY_PREDICT_PATH = "/v2/providers/openapi/apis/api/v1/categorization/predict"
    DISPLAY_CATEGORIES_PATH = "/v2/providers/seller_api/apis/api/v1/marketplace/meta/display-categories"
    CATEGORY_META_PATH = "/v2/providers/seller_api/apis/api/v1/marketplace/meta/category-related-metas/display-category-codes"
    SELLER_AUTO_GEN_PATH = "/v2/providers/seller_api/apis/api/v1/marketplace/seller/auto-generated"

    def __init__(self, vendor_id: str, access_key: str, secret_key: str):
        self.vendor_id = vendor_id
        self.access_key = access_key
        self.secret_key = secret_key
        self._last_request_time = 0.0
        self._session = requests.Session()

    def _generate_hmac(self, method: str, path: str, query: str = "") -> str:
        """
        HMAC-SHA256 서명 생성

        서명 대상 문자열:
            {datetime}\n{method}\n{path}\n{query}

        Returns:
            Authorization 헤더 값
        """
        dt = datetime.now(timezone.utc).strftime("%y%m%dT%H%M%SZ")
        message = f"{dt}{method}{path}{query}"

        signature = hmac.new(
            self.secret_key.encode("utf-8"),
            message.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        return f"CEA algorithm=HmacSHA256, access-key={self.access_key}, signed-date={dt}, signature={signature}"

    def _throttle(self):
        """Rate limit 준수: 요청 간 최소 0.1초 간격"""
        now = time.time()
        elapsed = now - self._last_request_time
        if elapsed < self.RATE_LIMIT_INTERVAL:
            time.sleep(self.RATE_LIMIT_INTERVAL - elapsed)
        self._last_request_time = time.time()

    def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict] = None,
        data: Optional[Dict] = None,
        timeout: int = 30,
        retry: bool = True,
    ) -> Dict[str, Any]:
        """
        공통 API 요청 (재시도 로직 포함)

        Args:
            method: HTTP 메서드 (GET, POST, PUT, DELETE)
            path: API 경로
            params: 쿼리 파라미터
            data: 요청 바디 (JSON)
            timeout: 요청 타임아웃 (초)
            retry: 재시도 활성화 여부

        Returns:
            API 응답 JSON

        Raises:
            CoupangWingError: API 오류 발생 시
        """
        max_attempts = MAX_RETRIES if retry else 1
        last_error = None

        for attempt in range(1, max_attempts + 1):
            self._throttle()

            # 쿼리 스트링 구성 (순서 유지 - 쿠팡 API는 원본 순서 사용)
            query = ""
            if params:
                query = "&".join(f"{k}={v}" for k, v in params.items())

            # HMAC 서명 생성
            authorization = self._generate_hmac(method.upper(), path, query)

            url = f"{self.BASE_URL}{path}"
            if query:
                url = f"{url}?{query}"

            headers = {
                "Authorization": authorization,
                "Content-Type": "application/json;charset=UTF-8",
                "X-EXTENDED-TIMEOUT": "90000",
            }

            logger.debug(f"WING API {method} {path} params={params} (시도 {attempt}/{max_attempts})")

            try:
                response = self._session.request(
                    method=method.upper(),
                    url=url,
                    headers=headers,
                    json=data,
                    timeout=timeout,
                )

                # 재시도 가능한 상태 코드 확인
                if response.status_code in RETRYABLE_STATUS_CODES:
                    if attempt < max_attempts:
                        delay = self._calculate_retry_delay(attempt)
                        logger.warning(
                            f"재시도 가능 상태 {response.status_code}, "
                            f"{delay:.1f}초 후 재시도 ({attempt}/{max_attempts})"
                        )
                        time.sleep(delay)
                        continue

                # 응답 처리
                if response.status_code == 200:
                    return self._parse_response(response)

                # 오류 처리
                try:
                    error_body = response.json()
                    code = error_body.get("code", str(response.status_code))
                    message = error_body.get("message", response.text)
                except ValueError:
                    code = str(response.status_code)
                    message = response.text

                raise CoupangWingError(code, message, response.status_code)

            except requests.exceptions.ConnectionError as e:
                last_error = CoupangWingError("NETWORK_ERROR", f"연결 오류: {e}")
                if attempt < max_attempts:
                    delay = self._calculate_retry_delay(attempt)
                    logger.warning(f"연결 오류, {delay:.1f}초 후 재시도 ({attempt}/{max_attempts}): {e}")
                    time.sleep(delay)
                    continue
                raise last_error

            except requests.exceptions.Timeout as e:
                last_error = CoupangWingError("TIMEOUT", f"타임아웃: {e}")
                if attempt < max_attempts:
                    delay = self._calculate_retry_delay(attempt)
                    logger.warning(f"타임아웃, {delay:.1f}초 후 재시도 ({attempt}/{max_attempts})")
                    time.sleep(delay)
                    continue
                raise last_error

            except requests.RequestException as e:
                raise CoupangWingError("NETWORK_ERROR", f"요청 실패: {e}")

        # 모든 재시도 소진
        if last_error:
            raise last_error
        raise CoupangWingError("MAX_RETRIES", "최대 재시도 횟수 초과")

    def _calculate_retry_delay(self, attempt: int) -> float:
        """지수 백오프 대기 시간 계산"""
        import random
        delay = RETRY_BASE_DELAY * (2 ** (attempt - 1))
        # ±25% 지터 추가
        jitter = delay * 0.25 * random.uniform(-1, 1)
        return min(delay + jitter, 30.0)  # 최대 30초

    def _parse_response(self, response: requests.Response) -> Dict[str, Any]:
        """
        API 응답 파싱 (중첩 구조 안전 처리)

        Args:
            response: requests 응답 객체

        Returns:
            파싱된 JSON 딕셔너리
        """
        try:
            result = response.json()
        except ValueError:
            return {"data": response.text}

        # 쿠팡 API는 HTTP 200에서도 에러를 반환할 수 있음
        if isinstance(result, dict):
            code = result.get("code", "")
            if code == "ERROR":
                message = result.get("message", "알 수 없는 오류")
                raise CoupangWingError("API_ERROR", message)

        return result

    def _paginate(
        self,
        fetch_fn: Callable[..., Dict],
        fetch_kwargs: Dict[str, Any],
        nested_keys: Tuple[str, str] = ("items", "items"),
        token_param: str = "next_token",
        use_has_next: bool = False,
        max_pages: int = 0,
        log_label: str = "페이지",
    ) -> List[Dict]:
        """
        공통 페이지네이션 헬퍼.

        Args:
            fetch_fn: 호출할 메서드 (결과 dict 반환)
            fetch_kwargs: fetch_fn에 전달할 기본 kwargs
            nested_keys: data가 dict일 때 아이템 추출 키 (primary, fallback)
            token_param: fetch_fn에 토큰을 전달할 kwarg 이름
            use_has_next: True이면 result["hasNext"]로 종료 판단
            max_pages: 최대 페이지 (0=무제한)
            log_label: 로그용 라벨
        """
        all_data: List[Dict] = []
        token = ""
        page = 0

        while True:
            kwargs = fetch_kwargs.copy()
            if token:
                kwargs[token_param] = token

            result = fetch_fn(**kwargs)
            data = result.get("data", [])

            if isinstance(data, list):
                items = data
            elif isinstance(data, dict):
                items = data.get(nested_keys[0], data.get(nested_keys[1], []))
            else:
                items = []
            all_data.extend(items)

            page += 1
            logger.info(f"  {log_label} {page}: {len(items)}건 (누적 {len(all_data)}건)")

            # 종료 조건
            if use_has_next and not result.get("hasNext", False):
                break

            # 토큰 추출 (상위 → 하위)
            token = result.get("nextToken", "")
            if not token and isinstance(data, dict):
                token = data.get("nextToken", "")
            if not token:
                break
            if max_pages > 0 and page >= max_pages:
                logger.info(f"  최대 페이지({max_pages}) 도달, 중단")
                break

        return all_data

    # ─────────────────────────────────────────────
    # 상품 관리
    # ─────────────────────────────────────────────

    def get_product(self, seller_product_id: int) -> Dict[str, Any]:
        """
        상품 단건 조회

        Args:
            seller_product_id: 쿠팡 판매자 상품 ID

        Returns:
            상품 상세 정보
        """
        path = f"{self.SELLER_PRODUCTS_PATH}/{seller_product_id}"
        return self._request("GET", path)

    def list_products(self, max_per_page: int = 50, max_pages: int = 0) -> List[Dict]:
        """
        전체 상품 목록 조회 (nextToken 자동 페이징)

        Args:
            max_per_page: 페이지당 최대 상품 수 (최대 100)
            max_pages: 최대 페이지 수 (0=무제한)

        Returns:
            전체 상품 리스트
        """
        def _fetch(next_token: str = ""):
            params = {"vendorId": self.vendor_id, "maxPerPage": str(max_per_page)}
            if next_token:
                params["nextToken"] = next_token
            return self._request("GET", self.SELLER_PRODUCTS_PATH, params=params)

        return self._paginate(
            fetch_fn=_fetch,
            fetch_kwargs={},
            nested_keys=("products", "items"),
            token_param="next_token",
            max_pages=max_pages,
            log_label="상품 페이지",
        )

    def create_product(self, product_data: Dict[str, Any], dashboard_override: bool = False) -> Dict[str, Any]:
        """
        상품 등록 (REGISTER_LOCK 안전장치 적용)

        Args:
            product_data: 상품 등록 JSON 데이터
            dashboard_override: 대시보드에서 개별 등록 시에만 True

        Returns:
            등록 결과 (sellerProductId 포함)
        """
        from core.constants import REGISTER_LOCK
        if REGISTER_LOCK and not dashboard_override:
            raise CoupangWingError(
                "REGISTER_LOCKED",
                "상품 등록이 잠겨있습니다 (REGISTER_LOCK=True). "
                "스크립트에서 상품 일괄 등록은 금지됩니다."
            )
        return self._request("POST", self.SELLER_PRODUCTS_PATH, data=product_data)

    def update_product(self, seller_product_id: int, product_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        상품 수정 (승인 필요)

        PUT /seller-products (base path, ID는 body의 sellerProductId로 식별)
        ※ PUT /seller-products/{id}는 GET/DELETE만 지원 — base path로 보내야 함

        Args:
            seller_product_id: 수정할 상품 ID (body에 sellerProductId 자동 포함)
            product_data: 수정 데이터

        Returns:
            수정 결과
        """
        product_data["sellerProductId"] = seller_product_id
        return self._request("PUT", self.SELLER_PRODUCTS_PATH, data=product_data)

    def patch_product(self, seller_product_id: int, product_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        상품 수정 (승인 불필요) - 배송비, 반품비 등 일부 필드만 변경

        PUT /v2/providers/seller_api/apis/api/v1/marketplace/seller-products/{sellerProductId}/partial
        sellerProductId를 제외한 모든 항목은 비필수이기 때문에 원하는 항목만 입력하여 수정 가능

        Args:
            seller_product_id: 수정할 상품 ID
            product_data: 수정 데이터 (변경할 필드만, sellerProductId 필수)

        Returns:
            수정 결과
        """
        path = f"{self.SELLER_PRODUCTS_PATH}/{seller_product_id}/partial"
        return self._request("PUT", path, data=product_data)

    def delete_product(self, seller_product_id: int, dashboard_override: bool = False) -> Dict[str, Any]:
        """
        상품 삭제 (DELETE_LOCK 안전장치 적용)

        Args:
            seller_product_id: 삭제할 상품 ID
            dashboard_override: 대시보드에서 개별 삭제 시에만 True

        Returns:
            삭제 결과
        """
        from core.constants import DELETE_LOCK
        if DELETE_LOCK and not dashboard_override:
            raise CoupangWingError(
                "DELETE_LOCKED",
                "상품 삭제가 잠겨있습니다 (DELETE_LOCK=True). "
                "스크립트에서 상품 일괄 삭제는 금지됩니다."
            )
        path = f"{self.SELLER_PRODUCTS_PATH}/{seller_product_id}"
        return self._request("DELETE", path)

    def get_inflow_status(self) -> Dict[str, Any]:
        """
        상품 등록 현황 조회

        Returns:
            등록 현황 (전체/승인대기/승인완료/반려 등 카운트)
        """
        path = f"{self.SELLER_PRODUCTS_PATH}/inflow-status"
        return self._request("GET", path)

    def get_product_partial(self, seller_product_id: int) -> Dict[str, Any]:
        """
        상품 조회 (승인 불필요 항목만)

        Args:
            seller_product_id: 상품 ID

        Returns:
            승인 불필요 항목 상세 정보
        """
        path = f"{self.SELLER_PRODUCTS_PATH}/{seller_product_id}/partial"
        return self._request("GET", path)

    def approve_product(self, seller_product_id: int) -> Dict[str, Any]:
        """
        상품 승인 요청

        Args:
            seller_product_id: 승인 요청할 상품 ID

        Returns:
            승인 요청 결과
        """
        path = f"{self.SELLER_PRODUCTS_PATH}/{seller_product_id}/approve"
        return self._request("PUT", path)

    def list_products_by_timeframe(
        self,
        vendor_id: str,
        created_at_from: str,
        created_at_to: str,
        max_per_page: int = 50,
        status: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        상품 목록 구간 조회 (생성일 기준)

        Args:
            vendor_id: 벤더 ID
            created_at_from: 시작일시 (ISO 8601, 예: 2026-01-01T00:00:00)
            created_at_to: 종료일시 (ISO 8601)
            max_per_page: 페이지당 상품 수 (최대 100)
            status: 상품 상태 필터 (옵션)

        Returns:
            상품 목록 + 페이징 정보
        """
        path = f"{self.SELLER_PRODUCTS_PATH}/by-timeframe"
        params = {
            "vendorId": vendor_id,
            "createdAtFrom": created_at_from,
            "createdAtTo": created_at_to,
            "maxPerPage": str(max_per_page),
        }
        if status:
            params["status"] = status
        return self._request("GET", path, params=params)

    def get_product_history(
        self,
        seller_product_id: int,
        next_token: Optional[str] = None,
        max_per_page: int = 10,
    ) -> Dict[str, Any]:
        """
        상품 상태변경이력 조회

        Args:
            seller_product_id: 상품 ID
            next_token: 페이징 토큰
            max_per_page: 페이지당 건수 (기본 10)

        Returns:
            상태변경 이력 목록
        """
        path = f"{self.SELLER_PRODUCTS_PATH}/{seller_product_id}/histories"
        params = {"maxPerPage": str(max_per_page)}
        if next_token:
            params["nextToken"] = next_token
        return self._request("GET", path, params=params)

    def get_product_by_sku(self, external_vendor_sku_code: str) -> Dict[str, Any]:
        """
        SKU 코드로 상품 요약 정보 조회

        Args:
            external_vendor_sku_code: 외부 벤더 SKU 코드

        Returns:
            상품 요약 정보
        """
        path = f"{self.VENDOR_ITEMS_PATH}/external-vendor-sku-codes/{external_vendor_sku_code}"
        return self._request("GET", path)

    # ─────────────────────────────────────────────
    # 재고/가격 관리
    # ─────────────────────────────────────────────

    def update_price(self, vendor_item_id: int, new_price: int, force: bool = True,
                     dashboard_override: bool = False) -> Dict[str, Any]:
        """
        옵션별 가격 변경 (PRICE_LOCK 안전장치 적용)

        Args:
            vendor_item_id: 벤더 아이템 ID
            new_price: 새 판매가 (10원 단위)
            force: 가격 변경 비율 제한 해제 (기본 True)
            dashboard_override: 대시보드에서 개별 수정 시에만 True

        Returns:
            업데이트 결과

        Raises:
            CoupangWingError: PRICE_LOCK이 True이고 dashboard_override가 False일 때
        """
        from core.constants import PRICE_LOCK
        if PRICE_LOCK and not dashboard_override:
            raise CoupangWingError(
                "PRICE_LOCKED",
                "가격 변경이 잠겨있습니다 (PRICE_LOCK=True). "
                "스크립트에서 가격 일괄 변경은 금지됩니다. "
                "대시보드 개별 수정만 허용됩니다."
            )
        path = f"{self.VENDOR_ITEMS_PATH}/{vendor_item_id}/prices/{new_price}"
        params = {"forceSalePriceUpdate": "true"} if force else None
        return self._request("PUT", path, params=params)

    def update_quantity(self, vendor_item_id: int, quantity: int) -> Dict[str, Any]:
        """
        옵션별 재고 변경

        Args:
            vendor_item_id: 벤더 아이템 ID
            quantity: 재고 수량

        Returns:
            업데이트 결과
        """
        path = f"{self.VENDOR_ITEMS_PATH}/{vendor_item_id}/quantities/{quantity}"
        return self._request("PUT", path)

    def update_inventory(self, vendor_item_id: int, quantity: int, price: int,
                         dashboard_override: bool = False) -> Dict[str, Any]:
        """
        재고/가격 동시 업데이트 (deprecated - update_price, update_quantity 사용 권장)
        PRICE_LOCK 안전장치 적용

        Args:
            vendor_item_id: 벤더 아이템 ID
            quantity: 재고 수량
            price: 판매가
            dashboard_override: 대시보드에서 개별 수정 시에만 True

        Returns:
            업데이트 결과
        """
        # 가격 먼저, 재고 다음
        price_result = self.update_price(vendor_item_id, price, dashboard_override=dashboard_override)
        quantity_result = self.update_quantity(vendor_item_id, quantity)
        return {"price": price_result, "quantity": quantity_result}

    def get_item_inventory(self, vendor_item_id: int) -> Dict[str, Any]:
        """
        아이템별 수량/가격/상태 조회

        Args:
            vendor_item_id: 벤더 아이템 ID

        Returns:
            아이템 재고/가격/판매상태 정보
        """
        path = f"{self.VENDOR_ITEMS_PATH}/{vendor_item_id}/inventories"
        return self._request("GET", path)

    def update_original_price(self, vendor_item_id: int, original_price: int,
                              dashboard_override: bool = False) -> Dict[str, Any]:
        """
        아이템별 할인율 기준가격 변경 (PRICE_LOCK 안전장치 적용)

        Args:
            vendor_item_id: 벤더 아이템 ID
            original_price: 할인율 기준가격 (원래가격)
            dashboard_override: 대시보드에서 개별 수정 시에만 True

        Returns:
            변경 결과

        Raises:
            CoupangWingError: PRICE_LOCK이 True이고 dashboard_override가 False일 때
        """
        from core.constants import PRICE_LOCK
        if PRICE_LOCK and not dashboard_override:
            raise CoupangWingError(
                "PRICE_LOCKED",
                "기준가격 변경이 잠겨있습니다 (PRICE_LOCK=True). "
                "스크립트에서 가격 일괄 변경은 금지됩니다."
            )
        path = f"{self.VENDOR_ITEMS_PATH}/{vendor_item_id}/original-prices/{original_price}"
        return self._request("PUT", path)

    def stop_item_sale(self, vendor_item_id: int, dashboard_override: bool = False) -> Dict[str, Any]:
        """
        아이템별 판매 중지 (SALE_STOP_LOCK 안전장치 적용)

        Args:
            vendor_item_id: 벤더 아이템 ID
            dashboard_override: 대시보드에서 개별 중지 시에만 True

        Returns:
            중지 결과
        """
        from core.constants import SALE_STOP_LOCK
        if SALE_STOP_LOCK and not dashboard_override:
            raise CoupangWingError(
                "SALE_STOP_LOCKED",
                "판매 중지가 잠겨있습니다 (SALE_STOP_LOCK=True). "
                "스크립트에서 판매 일괄 중지는 금지됩니다."
            )
        path = f"{self.VENDOR_ITEMS_PATH}/{vendor_item_id}/sales/stop"
        return self._request("PUT", path)

    def resume_item_sale(self, vendor_item_id: int) -> Dict[str, Any]:
        """
        아이템별 판매 재개

        Args:
            vendor_item_id: 벤더 아이템 ID

        Returns:
            재개 결과
        """
        path = f"{self.VENDOR_ITEMS_PATH}/{vendor_item_id}/sales/resume"
        return self._request("PUT", path)

    # ─────────────────────────────────────────────
    # 자동생성옵션
    # ─────────────────────────────────────────────

    def enable_auto_option(self, vendor_item_id: int) -> Dict[str, Any]:
        """
        자동생성옵션 활성화 (단일 옵션)

        Args:
            vendor_item_id: 벤더 아이템 ID

        Returns:
            활성화 결과
        """
        path = f"{self.SELLER_AUTO_GEN_PATH}/{vendor_item_id}/enable"
        return self._request("POST", path)

    def enable_auto_option_all(self) -> Dict[str, Any]:
        """
        자동생성옵션 활성화 (전체)

        Returns:
            전체 활성화 결과
        """
        path = f"{self.SELLER_AUTO_GEN_PATH}/enable-all"
        return self._request("POST", path)

    def disable_auto_option(self, vendor_item_id: int) -> Dict[str, Any]:
        """
        자동생성옵션 비활성화 (단일 옵션)

        Args:
            vendor_item_id: 벤더 아이템 ID

        Returns:
            비활성화 결과
        """
        path = f"{self.SELLER_AUTO_GEN_PATH}/{vendor_item_id}/disable"
        return self._request("POST", path)

    def disable_auto_option_all(self) -> Dict[str, Any]:
        """
        자동생성옵션 비활성화 (전체)

        Returns:
            전체 비활성화 결과
        """
        path = f"{self.SELLER_AUTO_GEN_PATH}/disable-all"
        return self._request("POST", path)

    # ─────────────────────────────────────────────
    # 카테고리
    # ─────────────────────────────────────────────

    def recommend_category(self, product_name: str, brand: Optional[str] = None,
                           product_description: Optional[str] = None,
                           attributes: Optional[Dict[str, str]] = None,
                           seller_sku_code: Optional[str] = None) -> Dict[str, Any]:
        """
        카테고리 추천 (상품명 기반 ML 모델)

        Args:
            product_name: 상품명 (필수, 상세할수록 정확)
            brand: 브랜드명
            product_description: 상품 상세설명
            attributes: 상품속성 (예: {"소재": "면", "색상": "검정"})
            seller_sku_code: 판매자 상품코드

        Returns:
            {autoCategorizationPredictionResultType, predictedCategoryId, predictedCategoryName, comment}
        """
        data: Dict[str, Any] = {"productName": product_name}
        if brand:
            data["brand"] = brand
        if product_description:
            data["productDescription"] = product_description
        if attributes:
            data["attributes"] = attributes
        if seller_sku_code:
            data["sellerSkuCode"] = seller_sku_code
        return self._request("POST", self.CATEGORY_PREDICT_PATH, data=data)

    def get_category_meta(self, display_category_code: str) -> Dict[str, Any]:
        """
        카테고리 메타 정보 조회 (필수 속성, 고시정보 등)

        Args:
            display_category_code: 디스플레이 카테고리 코드

        Returns:
            카테고리 메타 정보
        """
        path = f"{self.CATEGORY_META_PATH}/{display_category_code}"
        return self._request("GET", path)

    def get_display_categories(self, display_category_code: str) -> Dict[str, Any]:
        """
        카테고리 조회 (1-depth 하위 카테고리 포함)

        Args:
            display_category_code: 카테고리 코드 (0이면 최상위 1-depth)

        Returns:
            {displayItemCategoryCode, name, status, child: [...]}
        """
        path = f"{self.DISPLAY_CATEGORIES_PATH}/{display_category_code}"
        return self._request("GET", path)

    def list_all_categories(self) -> Dict[str, Any]:
        """
        카테고리 목록 전체 조회 (트리 구조)

        Returns:
            전체 카테고리 트리 {displayItemCategoryCode, name, status, child: [...]}
        """
        return self._request("GET", self.DISPLAY_CATEGORIES_PATH)

    def validate_category(self, display_category_code: str) -> Dict[str, Any]:
        """
        카테고리 유효성 검사 (leaf 카테고리 여부 + 사용 가능 여부)

        Args:
            display_category_code: 노출카테고리코드

        Returns:
            {code: "SUCCESS", data: true/false}
            - data=true: 유효한 leaf 카테고리
            - 400 에러: leaf가 아니면 에러 메시지에 leaf ID 목록 포함
        """
        path = f"{self.DISPLAY_CATEGORIES_PATH}/{display_category_code}/status"
        return self._request("GET", path)

    def check_auto_category_agreed(self) -> Dict[str, Any]:
        """
        카테고리 자동매칭 서비스 동의 확인

        Returns:
            {code: "SUCCESS", data: true/false}
        """
        path = f"/v2/providers/seller_api/apis/api/v1/marketplace/vendors/{self.vendor_id}/check-auto-category-agreed"
        return self._request("GET", path)

    # ─────────────────────────────────────────────
    # 물류 (출고지/반품지)
    # ─────────────────────────────────────────────

    def get_outbound_shipping_places(self, page_num: int = 1, page_size: int = 50) -> Dict[str, Any]:
        """출고지 목록 조회 (marketplace v1 - GET)"""
        path = "/v2/providers/marketplace_openapi/apis/api/v1/vendor/shipping-place/outbound"
        params = {"pageNum": str(page_num), "pageSize": str(page_size)}
        return self._request("GET", path, params=params)

    def get_return_shipping_centers(self, page_num: int = 1, page_size: int = 50) -> Dict[str, Any]:
        """반품지 목록 조회 (openapi v4 - GET)"""
        path = f"/v2/providers/openapi/apis/api/v4/vendors/{self.vendor_id}/returnShippingCenters"
        params = {"pageNum": str(page_num), "pageSize": str(page_size)}
        return self._request("GET", path, params=params)

    def create_outbound_shipping_place(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """출고지 생성 (v5 - POST)"""
        path = f"/v2/providers/openapi/apis/api/v5/vendors/{self.vendor_id}/outboundShippingCenters"
        return self._request("POST", path, data=data)

    def create_return_shipping_center(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """반품지 생성 (v5 - POST)"""
        path = f"/v2/providers/openapi/apis/api/v5/vendors/{self.vendor_id}/returnShippingCenters"
        return self._request("POST", path, data=data)

    # ─────────────────────────────────────────────
    # 발주서/매출
    # ─────────────────────────────────────────────

    def get_ordersheets(
        self,
        created_at_from: str,
        created_at_to: str,
        status: str = "ACCEPT",
        max_per_page: int = 50,
        next_token: str = "",
    ) -> Dict[str, Any]:
        """
        발주서 목록 조회 (일단위 페이징)

        Args:
            created_at_from: 시작일시 (ISO 8601, 예: 2026-01-01)
            created_at_to: 종료일시 (ISO 8601)
            status: 발주 상태 (ACCEPT, INSTRUCT, DEPARTURE, DELIVERING, FINAL_DELIVERY, NONE_TRACKING)
            max_per_page: 페이지당 최대 건수 (기본 50)
            next_token: 페이징 토큰

        Returns:
            발주서 목록 + 페이징 정보
        """
        path = f"/v2/providers/openapi/apis/api/v4/vendors/{self.vendor_id}/ordersheets"
        params = {
            "createdAtFrom": created_at_from,
            "createdAtTo": created_at_to,
            "status": status,
            "maxPerPage": str(max_per_page),
        }
        if next_token:
            params["nextToken"] = next_token
        return self._request("GET", path, params=params)

    def get_all_ordersheets(
        self,
        created_at_from: str,
        created_at_to: str,
        status: str = "ACCEPT",
    ) -> List[Dict]:
        """
        발주서 전체 조회 (자동 페이징)

        Args:
            created_at_from: 시작일시
            created_at_to: 종료일시
            status: 발주 상태

        Returns:
            전체 발주서 리스트
        """
        return self._paginate(
            fetch_fn=self.get_ordersheets,
            fetch_kwargs={
                "created_at_from": created_at_from,
                "created_at_to": created_at_to,
                "status": status,
            },
            nested_keys=("orderSheets", "items"),
            token_param="next_token",
            log_label="발주서 페이지",
        )

    def get_ordersheet_by_shipment(self, shipment_box_id: int) -> Dict[str, Any]:
        """
        발주서 단건 조회 (묶음배송번호)

        Args:
            shipment_box_id: 묶음배송번호

        Returns:
            발주서 상세 정보
        """
        path = f"/v2/providers/openapi/apis/api/v4/vendors/{self.vendor_id}/ordersheets/{shipment_box_id}"
        return self._request("GET", path)

    def get_ordersheet_by_order(self, order_id: int) -> Dict[str, Any]:
        """
        발주서 단건 조회 (주문번호)

        Args:
            order_id: 주문번호

        Returns:
            발주서 상세 정보
        """
        path = f"/v2/providers/openapi/apis/api/v4/vendors/{self.vendor_id}/ordersheets/orderId/{order_id}"
        return self._request("GET", path)

    def get_ordersheet_history(self, shipment_box_id: int) -> Dict[str, Any]:
        """
        발주서 배송상태 히스토리 조회

        Args:
            shipment_box_id: 묶음배송번호

        Returns:
            배송상태 변경 이력
        """
        path = f"/v2/providers/openapi/apis/api/v4/vendors/{self.vendor_id}/ordersheets/{shipment_box_id}/history"
        return self._request("GET", path)

    def acknowledge_ordersheets(self, shipment_box_ids: List[int]) -> Dict[str, Any]:
        """
        발주서 확인 (상품준비중 처리: ACCEPT → INSTRUCT)
        - 최대 50건씩 배치 처리
        - vendorId를 body에 포함 (API 필수)

        Args:
            shipment_box_ids: 묶음배송번호 리스트

        Returns:
            처리 결과 (여러 배치일 경우 마지막 배치 결과 + 통합 responseList)
        """
        path = f"/v2/providers/openapi/apis/api/v4/vendors/{self.vendor_id}/ordersheets/acknowledgement"

        # 50건 초과 시 배치 분할
        batch_size = 50
        all_response_list = []
        last_result = None

        for i in range(0, len(shipment_box_ids), batch_size):
            batch = shipment_box_ids[i:i + batch_size]
            data = {
                "vendorId": self.vendor_id,
                "shipmentBoxIds": batch,
            }
            result = self._request("PUT", path, data=data)
            last_result = result

            # responseList 수집
            if isinstance(result, dict) and "data" in result:
                resp_data = result["data"]
                if isinstance(resp_data, dict) and "responseList" in resp_data:
                    all_response_list.extend(resp_data["responseList"])

        # 통합 결과 반환
        if last_result and isinstance(last_result, dict) and "data" in last_result:
            last_result["data"]["responseList"] = all_response_list
        return last_result

    def upload_invoice(self, invoice_data_list: List[Dict]) -> Dict[str, Any]:
        """
        송장 업로드 (운송장 등록: INSTRUCT → DEPARTURE)

        Args:
            invoice_data_list: 송장 데이터 리스트
                [{shipmentBoxId, orderId, vendorItemId, deliveryCompanyCode, invoiceNumber,
                  splitShipping, preSplitShipped, estimatedShippingDate}]

        Returns:
            업로드 결과
        """
        path = f"/v2/providers/openapi/apis/api/v4/vendors/{self.vendor_id}/orders/invoices"
        # 필수 필드 기본값 보장
        for item in invoice_data_list:
            item.setdefault("splitShipping", False)
            item.setdefault("preSplitShipped", False)
            item.setdefault("estimatedShippingDate", "")
        data = {
            "vendorId": self.vendor_id,
            "orderSheetInvoiceApplyDtos": invoice_data_list,
        }
        return self._request("POST", path, data=data)

    def update_invoice(self, invoice_data_list: List[Dict]) -> Dict[str, Any]:
        """
        송장 업데이트 (운송장 수정)

        Args:
            invoice_data_list: 수정할 송장 데이터 리스트
                [{shipmentBoxId, orderId, vendorItemId, deliveryCompanyCode, invoiceNumber, splitShipping(optional)}]

        Returns:
            업데이트 결과
        """
        path = f"/v2/providers/openapi/apis/api/v4/vendors/{self.vendor_id}/ordersheets/invoices"
        data = {"orderSheetInvoices": invoice_data_list}
        return self._request("PUT", path, data=data)

    def stop_shipment(self, receipt_id: int, cancel_count: int) -> Dict[str, Any]:
        """
        출고중지 완료 처리

        Args:
            receipt_id: 접수번호 (receiptId)
            cancel_count: 취소수량

        Returns:
            처리 결과
        """
        path = f"/v2/providers/openapi/apis/api/v4/vendors/{self.vendor_id}/ordersheets/cancellation/stopShipment"
        data = {"receiptId": receipt_id, "cancelCount": cancel_count}
        return self._request("PUT", path, data=data)

    def complete_shipment(self, receipt_id: int, delivery_company_code: str, invoice_number: str) -> Dict[str, Any]:
        """
        이미출고 완료 처리

        Args:
            receipt_id: 접수번호
            delivery_company_code: 택배사 코드
            invoice_number: 운송장번호

        Returns:
            처리 결과
        """
        path = f"/v2/providers/openapi/apis/api/v4/vendors/{self.vendor_id}/ordersheets/cancellation/completeShipment"
        data = {
            "receiptId": receipt_id,
            "deliveryCompanyCode": delivery_company_code,
            "invoiceNumber": invoice_number,
        }
        return self._request("PUT", path, data=data)

    def cancel_order(
        self,
        order_id: int,
        vendor_item_ids: List[int],
        receipt_counts: List[int],
        cancel_type: str = "VENDOR",
        cancel_reason_category: str = "SOLD_OUT",
        cancel_reason: str = "재고 소진",
    ) -> Dict[str, Any]:
        """
        주문 취소 요청

        Args:
            order_id: 주문번호
            vendor_item_ids: 취소할 옵션ID 리스트
            receipt_counts: 각 옵션별 취소수량 리스트
            cancel_type: 취소유형 (VENDOR/BUYER)
            cancel_reason_category: 취소사유 카테고리
            cancel_reason: 취소사유 상세

        Returns:
            취소 요청 결과
        """
        path = f"/v2/providers/openapi/apis/api/v4/vendors/{self.vendor_id}/ordersheets/cancel"
        order_items = [
            {"vendorItemId": vid, "receiptCount": cnt}
            for vid, cnt in zip(vendor_item_ids, receipt_counts)
        ]
        data = {
            "orderId": order_id,
            "cancelType": cancel_type,
            "cancelReasonCategory": cancel_reason_category,
            "cancelReason": cancel_reason,
            "orderItems": order_items,
        }
        return self._request("PUT", path, data=data)

    def complete_long_term_undelivery(self, shipment_box_id: int, invoice_number: str) -> Dict[str, Any]:
        """
        장기미배송 완료 처리

        Args:
            shipment_box_id: 묶음배송번호
            invoice_number: 운송장번호

        Returns:
            처리 결과
        """
        path = f"/v2/providers/openapi/apis/api/v4/vendors/{self.vendor_id}/ordersheets/{shipment_box_id}/long-term-undelivery"
        data = {"invoiceNumber": invoice_number}
        return self._request("PUT", path, data=data)

    # ─────────────────────────────────────────────
    # 매출 내역
    # ─────────────────────────────────────────────

    def get_revenue_history(self, date_from: str, date_to: str,
                            token: str = "", max_per_page: int = 50) -> Dict[str, Any]:
        """
        매출 내역 단일 페이지 조회

        Args:
            date_from: 인식일 시작 (YYYY-MM-DD)
            date_to: 인식일 종료 (YYYY-MM-DD)
            token: 페이징 토큰
            max_per_page: 페이지당 최대 건수

        Returns:
            매출 내역 응답 (data, hasNext, nextToken)
        """
        path = "/v2/providers/openapi/apis/api/v1/revenue-history"
        params = {
            "vendorId": self.vendor_id,
            "recognitionDateFrom": date_from,
            "recognitionDateTo": date_to,
            "token": token,
            "maxPerPage": str(max_per_page),
        }
        return self._request("GET", path, params=params)

    def get_all_revenue_history(self, date_from: str, date_to: str) -> List[Dict]:
        """
        매출 내역 전체 조회 (자동 페이징)

        Args:
            date_from: 인식일 시작 (YYYY-MM-DD)
            date_to: 인식일 종료 (YYYY-MM-DD)

        Returns:
            전체 매출 내역 리스트 (주문 단위)
        """
        all_data = []
        token = ""
        page = 0
        while True:
            result = self.get_revenue_history(date_from, date_to, token=token)
            data = result.get("data", [])
            if isinstance(data, list):
                all_data.extend(data)
            elif isinstance(data, dict):
                items = data.get("orderItems", data.get("items", []))
                all_data.extend(items)
            page += 1
            logger.info(f"  매출 내역 페이지 {page}: {len(data) if isinstance(data, list) else '?'}건")

            if not result.get("hasNext", False):
                break
            token = result.get("nextToken", "")
            if not token:
                break
        return all_data

    # ─────────────────────────────────────────────
    # 정산 내역
    # ─────────────────────────────────────────────

    SETTLEMENT_HISTORY_PATH = "/v2/providers/marketplace_openapi/apis/api/v1/settlement-histories"

    def get_settlement_history(self, year_month: str) -> List[Dict]:
        """
        정산 내역 조회

        Args:
            year_month: 매출인식 기간 (YYYY-MM)

        Returns:
            정산 내역 리스트
        """
        params = {"revenueRecognitionYearMonth": year_month}
        result = self._request("GET", self.SETTLEMENT_HISTORY_PATH, params=params)

        # API가 리스트를 직접 반환할 수 있음
        if isinstance(result, list):
            return result
        # dict인 경우 data 키 확인
        data = result.get("data", result)
        if isinstance(data, list):
            return data
        return [data] if data else []

    # ─────────────────────────────────────────────
    # 반품/취소 관리
    # ─────────────────────────────────────────────

    def get_return_requests(self, date_from: str, date_to: str,
                            status: str = None, cancel_type: str = None,
                            token: str = "", max_per_page: int = 50) -> Dict[str, Any]:
        """
        반품/취소 요청 목록 조회 (단일 페이지)

        Args:
            date_from: 시작일 (YYYY-MM-DD)
            date_to: 종료일 (YYYY-MM-DD)
            status: 상태 필터 (RU/UC/CC/PR 등)
            cancel_type: 유형 필터 (CANCEL 등)
            token: 페이징 토큰 (timeFrame 모드에서는 무시)
            max_per_page: 페이지당 건수 (timeFrame 모드에서는 무시)

        Returns:
            반품 목록 응답 (data, hasNext, nextToken)
        """
        path = f"/v2/providers/openapi/apis/api/v6/vendors/{self.vendor_id}/returnRequests"
        # API 요구 형식: yyyy-MM-ddTHH:mm
        _from = date_from if "T" in date_from else f"{date_from}T00:00"
        _to = date_to if "T" in date_to else f"{date_to}T23:59"
        params = {
            "searchType": "timeFrame",
            "createdAtFrom": _from,
            "createdAtTo": _to,
        }
        if status:
            params["status"] = status
        if cancel_type:
            params["cancelType"] = cancel_type
        # timeFrame 모드에서는 nextToken/maxPerPage 미지원
        return self._request("GET", path, params=params)

    def get_all_return_requests(self, date_from: str, date_to: str,
                                 status: str = None, cancel_type: str = None) -> List[Dict]:
        """
        반품/취소 요청 전체 조회 (자동 페이징)

        Args:
            date_from: 시작일 (YYYY-MM-DD)
            date_to: 종료일 (YYYY-MM-DD)
            status: 상태 필터
            cancel_type: 유형 필터

        Returns:
            전체 반품 요청 리스트
        """
        return self._paginate(
            fetch_fn=self.get_return_requests,
            fetch_kwargs={
                "date_from": date_from,
                "date_to": date_to,
                "status": status,
                "cancel_type": cancel_type,
            },
            nested_keys=("returnDtoList", "items"),
            token_param="token",
            use_has_next=True,
            log_label="반품 페이지",
        )

    def get_return_request(self, receipt_id: int) -> Dict[str, Any]:
        """
        반품/취소 단건 조회

        Args:
            receipt_id: 접수번호

        Returns:
            반품 상세 정보
        """
        path = f"/v2/providers/openapi/apis/api/v6/vendors/{self.vendor_id}/returnRequests/{receipt_id}"
        return self._request("GET", path)

    def confirm_return_receipt(self, receipt_id: int) -> Dict[str, Any]:
        """
        반품 입고 확인 (판매자 창고 도착 확인)

        Args:
            receipt_id: 접수번호

        Returns:
            처리 결과
        """
        path = f"/v2/providers/openapi/apis/api/v4/vendors/{self.vendor_id}/returnRequests/{receipt_id}/receiveConfirmation"
        data = {
            "vendorId": self.vendor_id,
            "receiptId": receipt_id,
        }
        return self._request("PUT", path, data=data)

    def approve_return_request(self, receipt_id: int, cancel_count: int = 1) -> Dict[str, Any]:
        """
        반품 승인

        Args:
            receipt_id: 접수번호
            cancel_count: 반품접수 수량

        Returns:
            처리 결과
        """
        path = f"/v2/providers/openapi/apis/api/v4/vendors/{self.vendor_id}/returnRequests/{receipt_id}/approval"
        data = {
            "vendorId": self.vendor_id,
            "receiptId": receipt_id,
            "cancelCount": cancel_count,
        }
        return self._request("PUT", path, data=data)

    def get_return_withdrawals(self, date_from: str, date_to: str,
                                size_per_page: int = 50, page_index: int = 1) -> Dict[str, Any]:
        """
        반품 철회 이력 조회 (기간별, 최대 7일)

        Args:
            date_from: 시작일 (YYYY-MM-DD)
            date_to: 종료일 (YYYY-MM-DD)
            size_per_page: 페이지당 건수 (기본 50, 최대 100)
            page_index: 페이지 인덱스 (기본 1)

        Returns:
            철회 이력 응답
        """
        path = f"/v2/providers/openapi/apis/api/v4/vendors/{self.vendor_id}/returnWithdrawRequests"
        params = {
            "dateFrom": date_from,
            "dateTo": date_to,
            "sizePerPage": str(size_per_page),
            "pageIndex": str(page_index),
        }
        return self._request("GET", path, params=params)

    def get_return_withdrawals_by_ids(self, cancel_ids: List[int]) -> Dict[str, Any]:
        """
        반품 철회 이력 조회 (접수번호 기반, 최대 50개)

        Args:
            cancel_ids: 접수번호 리스트

        Returns:
            철회 이력 응답
        """
        path = f"/v2/providers/openapi/apis/api/v4/vendors/{self.vendor_id}/returnWithdrawList"
        data = {"cancelIds": cancel_ids}
        return self._request("POST", path, data=data)

    def create_return_invoice(self, receipt_id: int, delivery_company_code: str,
                               invoice_number: str,
                               delivery_type: str = "RETURN",
                               reg_number: str = "") -> Dict[str, Any]:
        """
        회수 송장 등록 (수동 회수 시)

        Args:
            receipt_id: 접수번호
            delivery_company_code: 택배사 코드 (CJGLS, EPOST, KDEXP 등)
            invoice_number: 운송장번호
            delivery_type: RETURN(반품) 또는 EXCHANGE(교환)
            reg_number: 택배사 회수번호 (선택)

        Returns:
            처리 결과
        """
        path = f"/v2/providers/openapi/apis/api/v4/vendors/{self.vendor_id}/return-exchange-invoices/manual"
        data = {
            "returnExchangeDeliveryType": delivery_type,
            "receiptId": receipt_id,
            "deliveryCompanyCode": delivery_company_code,
            "invoiceNumber": invoice_number,
        }
        if reg_number:
            data["regNumber"] = reg_number
        return self._request("POST", path, data=data)

    # ─────────────────────────────────────────────
    # CS (고객문의)
    # ─────────────────────────────────────────────

    def get_online_inquiries(self, date_from: str, date_to: str,
                              answered_type: str = "ALL",
                              page_num: int = 1, page_size: int = 50) -> Dict[str, Any]:
        """
        상품별 고객문의 조회 (최대 7일)

        Args:
            date_from: 시작일 (YYYY-MM-DD)
            date_to: 종료일 (YYYY-MM-DD)
            answered_type: ALL / ANSWERED / NOANSWER
            page_num: 페이지 번호
            page_size: 페이지 크기 (최대 50)
        """
        path = f"/v2/providers/openapi/apis/api/v5/vendors/{self.vendor_id}/onlineInquiries"
        params = {
            "vendorId": self.vendor_id,
            "inquiryStartAt": date_from,
            "inquiryEndAt": date_to,
            "answeredType": answered_type,
            "pageNum": str(page_num),
            "pageSize": str(page_size),
        }
        return self._request("GET", path, params=params)

    def get_all_online_inquiries(self, date_from: str, date_to: str,
                                   answered_type: str = "ALL") -> List[Dict]:
        """상품별 고객문의 전체 조회 (자동 페이징)"""
        all_items: List[Dict] = []
        page = 1
        while True:
            result = self.get_online_inquiries(date_from, date_to, answered_type, page, 50)
            data = result.get("data", {})
            items = data.get("content", [])
            all_items.extend(items)
            pagination = data.get("pagination", {})
            total_pages = pagination.get("totalPages", 1)
            if page >= total_pages:
                break
            page += 1
        return all_items

    def reply_online_inquiry(self, inquiry_id: int, content: str,
                               reply_by: str) -> Dict[str, Any]:
        """
        상품별 고객문의 답변

        Args:
            inquiry_id: 문의 ID
            content: 답변 내용
            reply_by: 응답자 WING ID
        """
        path = f"/v2/providers/openapi/apis/api/v4/vendors/{self.vendor_id}/onlineInquiries/{inquiry_id}/replies"
        data = {
            "content": content,
            "vendorId": self.vendor_id,
            "replyBy": reply_by,
        }
        return self._request("POST", path, data=data)

    def get_callcenter_inquiries(self, date_from: str = None, date_to: str = None,
                                   status: str = "NONE",
                                   vendor_item_id: str = None,
                                   page_num: int = 1, page_size: int = 30) -> Dict[str, Any]:
        """
        쿠팡 고객센터 문의 조회 (최대 7일)

        Args:
            date_from: 시작일 (YYYY-MM-DD)
            date_to: 종료일 (YYYY-MM-DD)
            status: NONE(전체) / ANSWER / NO_ANSWER / TRANSFER
            vendor_item_id: 옵션 ID (선택)
            page_num: 페이지 번호
            page_size: 페이지 크기 (최대 30)
        """
        path = f"/v2/providers/openapi/apis/api/v5/vendors/{self.vendor_id}/callCenterInquiries"
        params = {
            "vendorId": self.vendor_id,
            "partnerCounselingStatus": status,
            "pageNum": str(page_num),
            "pageSize": str(page_size),
        }
        if date_from:
            params["inquiryStartAt"] = date_from
        if date_to:
            params["inquiryEndAt"] = date_to
        if vendor_item_id:
            params["vendorItemId"] = vendor_item_id
        return self._request("GET", path, params=params)

    def get_all_callcenter_inquiries(self, date_from: str, date_to: str,
                                       status: str = "NONE") -> List[Dict]:
        """쿠팡 고객센터 문의 전체 조회 (자동 페이징)"""
        all_items: List[Dict] = []
        page = 1
        while True:
            result = self.get_callcenter_inquiries(date_from, date_to, status, None, page, 30)
            data = result.get("data", {})
            items = data.get("content", [])
            all_items.extend(items)
            pagination = data.get("pagination", {})
            total_pages = pagination.get("totalPages", 1)
            if page >= total_pages:
                break
            page += 1
        return all_items

    def get_callcenter_inquiry(self, inquiry_id: int) -> Dict[str, Any]:
        """쿠팡 고객센터 문의 단건 조회"""
        path = f"/v2/providers/openapi/apis/api/v5/vendors/callCenterInquiries/{inquiry_id}"
        return self._request("GET", path)

    def reply_callcenter_inquiry(self, inquiry_id: int, content: str,
                                   reply_by: str, parent_answer_id: int) -> Dict[str, Any]:
        """
        쿠팡 고객센터 문의 답변

        Args:
            inquiry_id: 상담번호
            content: 답변 내용 (2~1000자)
            reply_by: 실사용자 WING ID
            parent_answer_id: 부모이관글 answerId
        """
        path = f"/v2/providers/openapi/apis/api/v4/vendors/{self.vendor_id}/callCenterInquiries/{inquiry_id}/replies"
        data = {
            "vendorId": self.vendor_id,
            "inquiryId": str(inquiry_id),
            "content": content,
            "replyBy": reply_by,
            "parentAnswerId": str(parent_answer_id),
        }
        return self._request("POST", path, data=data)

    def confirm_callcenter_inquiry(self, inquiry_id: int, confirm_by: str) -> Dict[str, Any]:
        """
        쿠팡 고객센터 문의 확인 (미확인/TRANSFER 상태)

        Args:
            inquiry_id: 상담번호
            confirm_by: 실사용자 WING ID
        """
        path = f"/v2/providers/openapi/apis/api/v4/vendors/{self.vendor_id}/callCenterInquiries/{inquiry_id}/confirms"
        data = {"confirmBy": confirm_by}
        return self._request("POST", path, data=data)

    # ─────────────────────────────────────────────
    # 연결 테스트
    # ─────────────────────────────────────────────

    def test_connection(self) -> bool:
        """
        API 연결 테스트 (상품 목록 1개 조회로 확인)

        Returns:
            True: 연결 성공, False: 실패
        """
        try:
            params = {"vendorId": self.vendor_id, "maxPerPage": "1"}
            result = self._request("GET", self.SELLER_PRODUCTS_PATH, params=params)
            logger.info(f"WING API 연결 성공: vendor_id={self.vendor_id}")
            return True
        except CoupangWingError as e:
            logger.error(f"WING API 연결 실패: vendor_id={self.vendor_id}, {e}")
            return False

    def __repr__(self):
        return f"<CoupangWingClient(vendor_id='{self.vendor_id}')>"
