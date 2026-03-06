"""알라딘 API 크롤러 (공식 API)"""
import requests
from typing import List, Dict, Optional
from datetime import datetime
import logging
from urllib.parse import quote

# Book 모델의 유틸리티 메서드 사용
from core.models.book import Book

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AladinAPICrawler:
    """
    알라딘 공식 API 크롤러

    API 문서: http://blog.aladin.co.kr/ttb/category/19154755
    """

    BASE_URL = "http://www.aladin.co.kr/ttb/api/"

    def __init__(self, ttb_key: str = None):
        """
        Args:
            ttb_key: 알라딘 TTBKey (발급 필요)
                    발급: https://www.aladin.co.kr/ttb/wblog_manage.aspx
        """
        self.ttb_key = ttb_key

        if not self.ttb_key:
            logger.warning("TTBKey가 없습니다. 발급받으세요: https://www.aladin.co.kr/ttb/wblog_manage.aspx")

        self.session = requests.Session()

    def search_by_keyword(
        self,
        keyword: str,
        max_results: int = 50,
        search_target: str = "Book",
        sort: str = "PublishTime",
        year_filter: int = None,
    ) -> List[Dict]:
        """
        키워드로 도서 검색

        Args:
            keyword: 검색 키워드
            max_results: 최대 결과 수
            search_target: Book, Foreign, Music, DVD, Used, eBook
            sort: 정렬 기준 (PublishTime=최신순, Accuracy=관련도, SalesPoint=판매량)
            year_filter: 출간 연도 필터 (예: 2025 → 2025~현재만 수집, None이면 필터 없음)

        Returns:
            도서 정보 리스트
        """
        if not self.ttb_key:
            logger.error("TTBKey가 필요합니다.")
            return []

        products = []
        start = 1
        max_per_page = 50  # API 최대값

        while len(products) < max_results:
            try:
                url = f"{self.BASE_URL}ItemSearch.aspx"

                params = {
                    "ttbkey": self.ttb_key,
                    "Query": keyword,
                    "QueryType": "Keyword",
                    "SearchTarget": search_target,
                    "Sort": sort,
                    "Start": start,
                    "MaxResults": min(max_per_page, max_results - len(products)),
                    "Cover": "Big",
                    "output": "js",
                    "Version": "20131101"
                }

                logger.info(f"알라딘 API 요청: {keyword} (페이지 {start})")

                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()

                data = response.json()

                if "item" not in data or not data["item"]:
                    logger.info("더 이상 결과가 없습니다.")
                    break

                items = data["item"]

                early_stop = False
                for item in items:
                    product = self._parse_item(item)
                    if not product:
                        continue

                    # 연도 필터: publish_date가 있으면 기준 연도 이전이면 스킵
                    if year_filter and product.get("publish_date"):
                        if product["publish_date"].year < year_filter:
                            # 최신순 정렬이면 이후는 더 오래된 것만 → 조기 종료
                            if sort == "PublishTime":
                                early_stop = True
                                break
                            continue

                    products.append(product)
                    logger.info(f"수집: {product['title'][:40]}")

                if early_stop:
                    logger.info(f"{year_filter}년 이전 도서 도달, 조기 종료")
                    break

                if len(items) < max_per_page:
                    break

                start += len(items)

            except Exception as e:
                logger.error(f"API 요청 오류: {e}")
                break

        logger.info(f"알라딘 검색 완료: 총 {len(products)}개")
        return products

    def fetch_new_releases(
        self,
        category_id: int = 0,
        max_results: int = 200,
        publisher_names: List[str] = None
    ) -> List[Dict]:
        """
        알라딘 ItemList API로 신간 도서 수집

        Args:
            category_id: 카테고리 ID (0=전체)
            max_results: 최대 수집 수 (API 한계: 1000)
            publisher_names: 필터링할 출판사 이름 리스트 (None이면 전체)

        Returns:
            출판사 필터링된 신간 도서 리스트
        """
        if not self.ttb_key:
            logger.error("TTBKey가 필요합니다.")
            return []

        all_items = []
        start = 1
        max_per_page = 50  # API 최대값

        while len(all_items) < max_results:
            try:
                url = f"{self.BASE_URL}ItemList.aspx"

                params = {
                    "ttbkey": self.ttb_key,
                    "QueryType": "ItemNewAll",
                    "SearchTarget": "Book",
                    "Start": start,
                    "MaxResults": min(max_per_page, max_results - len(all_items)),
                    "Cover": "Big",
                    "output": "js",
                    "Version": "20131101",
                }
                if category_id > 0:
                    params["CategoryId"] = category_id

                logger.info(f"알라딘 신간 API 요청 (페이지 {start // max_per_page + 1})")

                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()

                data = response.json()

                if "item" not in data or not data["item"]:
                    logger.info("더 이상 신간이 없습니다.")
                    break

                items = data["item"]

                for item in items:
                    product = self._parse_item(item)
                    if not product:
                        continue

                    # 출판사 필터링
                    if publisher_names:
                        matched = any(
                            self._match_publisher_name(product["publisher"], pn)
                            for pn in publisher_names
                        )
                        if not matched:
                            continue

                    all_items.append(product)

                if len(items) < max_per_page:
                    break

                start += len(items)

            except Exception as e:
                logger.error(f"신간 API 요청 오류: {e}")
                break

        logger.info(f"알라딘 신간 수집 완료: 총 {len(all_items)}개")
        return all_items

    # 출판사 이름 별칭 맵: DB이름 → [알라딘에서 사용하는 이름들]
    PUBLISHER_ALIASES = {
        "능률교육": ["NE능률", "NE능률(참고서)", "NE Build&Grow"],
        "크라운": ["크라운출판사", "크라운Publishing"],
        "EBS": ["한국교육방송공사", "EBS한국교육방송공사"],
        "한국교육방송공사": ["EBS"],
        "동아": ["동아출판", "동아출판(사전)"],
        "영진": ["영진닷컴", "영진.com(영진닷컴)", "영진문화사"],
        "이퓨쳐": ["e-future", "이퓨처"],
        "지학사": ["지학사(참고서)"],
        "이투스": ["이투스북"],
    }

    @classmethod
    def get_search_names(cls, publisher_name: str) -> List[str]:
        """검색에 사용할 이름 목록 반환 (원래 이름 + 별칭)"""
        names = [publisher_name]
        aliases = cls.PUBLISHER_ALIASES.get(publisher_name, [])
        names.extend(aliases)
        return names

    @classmethod
    def _match_publisher_name(cls, api_publisher: str, target_name: str) -> bool:
        """출판사명 매칭 (부분 일치 + 별칭 허용)"""
        api_publisher = api_publisher.strip()
        target_name = target_name.strip()

        if target_name in api_publisher or api_publisher in target_name:
            return True

        # 별칭 매칭
        aliases = cls.PUBLISHER_ALIASES.get(target_name, [])
        for alias in aliases:
            if alias in api_publisher or api_publisher in alias:
                return True

        return False

    def search_by_isbn(self, isbn: str) -> Optional[Dict]:
        """
        ISBN으로 도서 검색

        Args:
            isbn: ISBN-10 또는 ISBN-13

        Returns:
            도서 정보 또는 None
        """
        if not self.ttb_key:
            logger.error("TTBKey가 필요합니다.")
            return None

        try:
            url = f"{self.BASE_URL}ItemLookUp.aspx"

            params = {
                "ttbkey": self.ttb_key,
                "itemIdType": "ISBN",
                "ItemId": isbn,
                "Cover": "Big",
                "output": "js",
                "Version": "20131101"
            }

            logger.info(f"알라딘 ISBN 검색: {isbn}")

            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()

            if "item" not in data or not data["item"]:
                logger.warning(f"ISBN {isbn}을 찾을 수 없습니다.")
                return None

            item = data["item"][0]
            product = self._parse_item(item)

            if product:
                logger.info(f"찾음: {product['title']}")

            return product

        except Exception as e:
            logger.error(f"ISBN 검색 오류: {e}")
            return None

    def _parse_item(self, item: Dict) -> Dict:
        """API 응답을 표준 형식으로 변환 (연도 추출 포함)"""
        try:
            # ISBN (13자리 우선, 없으면 10자리)
            isbn = item.get("isbn13", "") or item.get("isbn", "")

            # 제목
            title = item.get("title", "")

            # 연도 추출 (Book 모델의 메서드 사용)
            year = Book.extract_year(title)

            # 제목 정규화 (연도 제거)
            normalized_title = Book.normalize_title(title, year)

            # 시리즈명 추출 (묶음 SKU용)
            normalized_series = Book.extract_series(normalized_title)

            # 가격 (정가) - priceStandard가 정가, priceSales는 이미 10% 할인된 판매가
            # 도서정가제: 판매가 = 정가 × 0.9 이므로 반드시 정가(priceStandard)를 저장해야 함
            price = item.get("priceStandard", 0)  # 정가
            if price == 0:
                price = item.get("priceSales", 15000)  # fallback (정가 없으면 판매가라도)

            # 출간일
            pub_date = item.get("pubDate", "")
            publish_date = None
            if pub_date:
                try:
                    publish_date = datetime.strptime(pub_date, "%Y-%m-%d").date()
                except:
                    pass

            result = {
                "isbn": isbn,
                "title": title,
                "author": item.get("author", ""),
                "publisher": item.get("publisher", ""),
                "original_price": int(price),
                "category": item.get("categoryName", "도서"),
                "subcategory": "",
                "image_url": item.get("cover", "").replace("/coversum/", "/cover500/").replace("/cover/", "/cover500/"),
                "description": item.get("description", ""),
                "kyobo_url": item.get("link", ""),  # 알라딘 링크
                "publish_date": publish_date,
                "crawled_at": datetime.utcnow(),

                # 연도 및 정규화 정보 (V2 추가)
                "year": year,
                "normalized_title": normalized_title,
                "normalized_series": normalized_series,

                # 추가 정보
                "page_count": item.get("subInfo", {}).get("itemPage", 0) if isinstance(item.get("subInfo"), dict) else 0,
                "sales_point": item.get("salesPoint", 0),
            }

            # 로그에 연도 정보 포함
            if year:
                logger.debug(f"  Year extracted: {year} from '{title}'")
                logger.debug(f"  Normalized: '{normalized_title}'")
                logger.debug(f"  Series: '{normalized_series}'")

            return result

        except Exception as e:
            logger.error(f"아이템 파싱 오류: {e}")
            return None
