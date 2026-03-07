"""비즈니스 상수 — 매직넘버 중앙 관리"""

# ─────────────────────────────────────────────
# 상품 상태 코드 (WING API)
# ─────────────────────────────────────────────
STATUS_MAP = {
    "APPROVED": "승인완료(판매중)",
    "DRAFT": "임시저장",
    "PENDING": "승인대기",
    "DELETED": "삭제됨",
    "REJECTED": "반려됨",
    "UNKNOWN": "알 수 없음",
}

# ─────────────────────────────────────────────
# 도서정가제 (한국 법률)
# ─────────────────────────────────────────────
BOOK_DISCOUNT_RATE = 0.9  # 정가 × 0.9 = 판매가

# 원가율 (정가 → 판매가 역산) — 1.11 하드코딩 금지!
ORIGINAL_PRICE_RATIO = 1 / BOOK_DISCOUNT_RATE  # ≈ 1.1111...


def calc_original_price(sale_price: int) -> int:
    """판매가 → 정가(original_price) 계산 (도서정가제)

    주의: 1.11 하드코딩 대신 이 함수 사용
    """
    return round(sale_price * ORIGINAL_PRICE_RATIO)


def validate_prices(sale_price: int, original_price: int, tolerance: int = 10) -> bool:
    """판매가↔정가 정합성 검증

    Args:
        sale_price: 판매가
        original_price: 정가 (original_price)
        tolerance: 허용 오차 (원)

    Returns:
        True if 정합성 통과
    """
    expected_original = calc_original_price(sale_price)
    return abs(original_price - expected_original) <= tolerance


# ─────────────────────────────────────────────
# 쿠팡 수수료
# ─────────────────────────────────────────────
COUPANG_FEE_RATE = 0.11  # 판매가의 11%

# ─────────────────────────────────────────────
# 배송비
# ─────────────────────────────────────────────
# 기본 배송비
DEFAULT_SHIPPING_COST = 2000
# 반품비
DEFAULT_RETURN_CHARGE = 2500
# 무료배송 기준         
FREE_SHIPPING_THRESHOLD = 2000
# 목표 마진 최소값
TARGET_MARGIN_MIN = 1300
# 목표 마진 최대값
TARGET_MARGIN_MAX = 2000
# 조건부 무료배송 기준
CONDITIONAL_FREE_THRESHOLD = 20000
# 조건부 무료배송 기준 (67%)
CONDITIONAL_FREE_THRESHOLD_67 = 25000
# 조건부 무료배송 기준 (70%)
CONDITIONAL_FREE_THRESHOLD_70 = 30000
# 조건부 무료배송 기준 (73%)
CONDITIONAL_FREE_THRESHOLD_73 = 60000

# ─────────────────────────────────────────────
# 재고
# ─────────────────────────────────────────────
# 기본 재고
# 기본 리드타임
# 저장재고 임계값
DEFAULT_STOCK = 1000
# 기본 리드타임
DEFAULT_LEAD_TIME = 2
# 저장재고 임계값
LOW_STOCK_THRESHOLD = 3

# ─────────────────────────────────────────────
# API
# ─────────────────────────────────────────────
# API 쓰로틀 시간
API_THROTTLE_SECONDS = 1.0  
# 쿠팡 WING API 요청 제한
COUPANG_WING_RATE_LIMIT = 0.1

# ─────────────────────────────────────────────
# 쿠팡 WING API - 도서 상품 등록 기본값
# ─────────────────────────────────────────────
BOOK_PRODUCT_DEFAULTS = {
    # 배송방법
    "deliveryMethod": "SEQUENCIAL",
    # 배송비 유형
    "deliveryChargeType": "CONDITIONAL_FREE",
    # 배송비
    "deliveryCharge": DEFAULT_SHIPPING_COST,
    # 조건부 무료배송 기준
    "freeShipOverAmount": CONDITIONAL_FREE_THRESHOLD,
    # 반품비
    "deliveryChargeOnReturn": DEFAULT_RETURN_CHARGE,
    # 배송방법
    "unionDeliveryType": "UNION_DELIVERY",
    # 원격지 배송 가능 여부
    "remoteAreaDeliverable": "N",
    # 반품비
    "returnCharge": DEFAULT_RETURN_CHARGE,
    # 요청 여부
    "requested": True,
    # 성인 전용 여부
    "adultOnly": "EVERYONE",
    # 세율 유형
    "taxType": "FREE",
    # 병렬 수입 여부
    "parallelImported": "NOT_PARALLEL_IMPORTED",
    # 해외 구매 여부
    "overseasPurchased": "NOT_OVERSEAS_PURCHASED",
    # PCC 필요 여부
    "pccNeeded": False,
    # 제품 상태
    "offerCondition": "NEW",
    # 출고 배송 시간
    "outboundShippingTimeDay": 1,
    # 최대 구매 인원
    "maximumBuyForPerson": 0,
}

# ─────────────────────────────────────────────
# 도서 카테고리 코드
# ─────────────────────────────────────────────
BOOK_CATEGORY_CODE = "76236"
BOOK_CATEGORY_MAP = {
    # 도서 카테고리 코드
    "76236": "고등교재",
    # 도서 카테고리 코드
    "76239": "기타교재",
    # 도서 카테고리 코드
    "76243": "수험서",
    # 도서 카테고리 코드
    "35171": "고등교재",
    # 도서 카테고리 코드
    "76001": "국내도서",
}

# ─────────────────────────────────────────────
# WING API 계정별 환경변수 매핑
# ─────────────────────────────────────────────
WING_ACCOUNT_ENV_MAP = {
    "007-book": "COUPANG_007BOOK",
    "007-bm":   "COUPANG_007BM",
    "007-ez":   "COUPANG_007EZ",
    "002-bm":   "COUPANG_002BM",
    "big6ceo":  "COUPANG_BIG6CEO",
}

# ─────────────────────────────────────────────
# WING 로그인 계정 환경변수 매핑
# ─────────────────────────────────────────────
WING_LOGIN_ENV_MAP = {
    "007-book": 1,
    "007-ez": 2,
    "007-bm": 3,
    "002-bm": 4,
    "big6ceo": 5,
}

# ─────────────────────────────────────────────
# 동기화 설정
# ─────────────────────────────────────────────
SYNC_CONFIG = {
    "batch_size": 50,
    "retry_max_attempts": 3,
    "retry_base_delay": 1.0,
    "stale_hours": 24,
}

# ─────────────────────────────────────────────
# 타임아웃 설정
# ─────────────────────────────────────────────
TIMEOUT_CONFIG = {
    "api_request": 30,
    "db_connect": 30,
}

# ─────────────────────────────────────────────
# 자동 크롤링 설정
# ─────────────────────────────────────────────
AUTO_CRAWL_CONFIG = {
    # 크롤링 시간       
    "crawl_hour": 3,
    # 최대 크롤링 거래처별 상품 수
    "max_per_publisher": 50,
    # 년도 필터
    "year_filter": 2025,
    # 확인 간격
    "check_interval": 30,
    # 최대 안전 상품 수 
    "max_items_safety": 200,
    # 최대 크롤링 거래처별 상품 수
    "max_per_publisher": 50,
    # 년도 필터
    "year_filter": 2025,
    # 확인 간격
    "check_interval": 30,
    # 최대 안전 상품 수
    "max_items_safety": 200,
}

# ─────────────────────────────────────────────
# 크롤링 최소 가격
# ─────────────────────────────────────────────
CRAWL_MIN_PRICE = 5000
# ─────────────────────────────────────────────
# 크롤링 제외 키워드
# ─────────────────────────────────────────────
CRAWL_EXCLUDE_KEYWORDS = [
    "사전", "잡지", "월간지", "자습서", "평가문제집",
]

# ─────────────────────────────────────────────
# 가격 설정
# ─────────────────────────────────────────────
PRICE_CONFIG = {
    # 목표 마진 최소값              
    "min_margin": 1300,
    # 목표 마진 최대값
    "target_margin": 2000,
    # 최소 마진율
    "min_margin_rate": 0.05,
    # 묶음 임계값
    "bundle_threshold": 2000,
}


# ─────────────────────────────────────────────
# 배송비 결정 함수
# ─────────────────────────────────────────────
def determine_customer_shipping_fee(margin_rate: int, list_price: int) -> int:
    """공급률(매입률)과 정가 기준으로 고객 부담 배송비 결정"""
    if margin_rate <= 50:
        return 0
    if margin_rate <= 55:
        return 0 if list_price >= 15000 else DEFAULT_SHIPPING_COST
    if margin_rate <= 60:
        return 0 if list_price >= 18000 else DEFAULT_SHIPPING_COST
    if margin_rate <= 62:
        return 0 if list_price >= 18000 else 2000
    if margin_rate <= 65:
        if list_price >= 20500:
            return 0
        if 18000 <= list_price <= 20000:
            return 1000
        return DEFAULT_SHIPPING_COST
    if margin_rate <= 70:
        if 18500 <= list_price <= 29000:
            return 1000
        if 15000 <= list_price <= 18000:
            return 2000
        return DEFAULT_SHIPPING_COST
    return DEFAULT_SHIPPING_COST


def determine_delivery_charge_type(margin_rate: int, list_price: int) -> tuple:
    """배송비 유형 + 금액 + 무료배송 기준 결정 (WING API용)"""
    customer_fee = determine_customer_shipping_fee(margin_rate, list_price)

    if customer_fee == 0:
        return ("FREE", 0, 0)
    if margin_rate > 70:
        return ("CONDITIONAL_FREE", DEFAULT_SHIPPING_COST, CONDITIONAL_FREE_THRESHOLD_73)
    if margin_rate > 67:
        return ("CONDITIONAL_FREE", customer_fee, CONDITIONAL_FREE_THRESHOLD_70)
    if margin_rate > 65:
        return ("CONDITIONAL_FREE", customer_fee, CONDITIONAL_FREE_THRESHOLD_67)
    return ("CONDITIONAL_FREE", customer_fee, CONDITIONAL_FREE_THRESHOLD)


# ─────────────────────────────────────────────
# 거래처(총판) ↔ 출판사 매핑
# ─────────────────────────────────────────────
DISTRIBUTOR_MAP = {
    "제일": ["비상교육", "수경"],
    "대성": ["이투스", "희망"],
    "일신": ["한국교육방송", "EBS", "좋은책신사고", "동아"],
    "서부": ["마더텅", "개념원리", "능률교육", "꿈틀", "쏠티북스"],
    "북전": ["키출판사", "에듀윌"],
    "동아": ["에듀원", "에듀플라자", "베스트", "쎄듀"],
    "강우사": ["디딤돌", "미래엔"],
    "대원": ["폴리북스", "팩토", "매스티안", "소마"],
}

# ─────────────────────────────────────────────
# 시리즈 매핑
# ─────────────────────────────────────────────
SERIES_TO_PUBLISHER = {
    "완자": "비상교육", "오투": "비상교육", "한끝": "비상교육",
    "개념+유형": "비상교육", "개념 + 유형": "비상교육",
    "만렙": "비상교육", "내공의힘": "비상교육",
    "쎈": "좋은책신사고", "라이트쎈": "좋은책신사고", "베이직쎈": "좋은책신사고",
    "일품": "좋은책신사고", "쎈개념연산": "좋은책신사고",
    "수능특강": "EBS", "수능완성": "EBS",
    "개념원리": "개념원리", "RPM": "개념원리", "알피엠": "개념원리",
    "능률 Voca": "능률교육", "능률보카": "능률교육", "GRAMMAR JOY": "능률교육",
    "GRAMMER JOY": "능률교육",
    "디딤돌": "디딤돌", "최상위수학": "디딤돌", "최상위": "디딤돌",
    "자이스토리": "미래엔",
    "마더텅": "마더텅",
    "동아 백점": "동아", "백점": "동아",
    "키출판사": "키출판사",
    "에듀윌": "에듀윌",
    "마플": "이투스", "마플교과서": "이투스", "수학의바이블": "이투스",
    "100발 100중": "에듀원", "백발백중": "에듀원",
}

# 역방향 매핑 (출판사→거래처)
_PUBLISHER_TO_DISTRIBUTOR = {}
for _dist, _pubs in DISTRIBUTOR_MAP.items():
    for _pub in _pubs:
        _PUBLISHER_TO_DISTRIBUTOR[_pub] = _dist


# ─────────────────────────────────────────────
# 출판사 매입률 정보
# ─────────────────────────────────────────────
PUBLISHERS = [
    {"name": "마린북스", "margin": 40, "min_free_shipping": 15000},
    {"name": "아카데미소프트", "margin": 40, "min_free_shipping": 15000},
    {"name": "렉스미디어", "margin": 40, "min_free_shipping": 15000},
    {"name": "해람북스", "margin": 40, "min_free_shipping": 15000},
    {"name": "웰북", "margin": 40, "min_free_shipping": 15000},
    {"name": "크라운", "margin": 55, "min_free_shipping": 15000},
    {"name": "영진", "margin": 55, "min_free_shipping": 15000},
    {"name": "매스티안", "margin": 55, "min_free_shipping": 15000},
    {"name": "소마", "margin": 60, "min_free_shipping": 18000},
    {"name": "씨투엠에듀", "margin": 60, "min_free_shipping": 18000},
    {"name": "이퓨처", "margin": 60, "min_free_shipping": 18000},
    {"name": "사회평론", "margin": 60, "min_free_shipping": 18000},
    {"name": "길벗", "margin": 60, "min_free_shipping": 18000},
    {"name": "이지스퍼블리싱", "margin": 60, "min_free_shipping": 18000},
    {"name": "이지스에듀", "margin": 60, "min_free_shipping": 18000},
    {"name": "나눔에이엔티", "margin": 60, "min_free_shipping": 18000},
    {"name": "배움", "margin": 60, "min_free_shipping": 18000},
    {"name": "혜지원", "margin": 60, "min_free_shipping": 18000},
    {"name": "디지털북스", "margin": 60, "min_free_shipping": 18000},
    {"name": "생각의집", "margin": 60, "min_free_shipping": 18000},
    {"name": "예림당", "margin": 60, "min_free_shipping": 18000},
    {"name": "개념원리", "margin": 65, "min_free_shipping": 20500},
    {"name": "개념원리수학연구소", "margin": 65, "min_free_shipping": 20500},
    {"name": "이투스", "margin": 65, "min_free_shipping": 20500},
    {"name": "이투스북", "margin": 65, "min_free_shipping": 20500},
    {"name": "비상교육", "margin": 65, "min_free_shipping": 20500},
    {"name": "능률교육", "margin": 65, "min_free_shipping": 20500},
    {"name": "지학사", "margin": 65, "min_free_shipping": 20500},
    {"name": "수경출판사", "margin": 65, "min_free_shipping": 20500},
    {"name": "쏠티북스", "margin": 65, "min_free_shipping": 20500},
    {"name": "마더텅", "margin": 65, "min_free_shipping": 20500},
    {"name": "한빛미디어", "margin": 65, "min_free_shipping": 20500},
    {"name": "시대고시", "margin": 65, "min_free_shipping": 20500},
    {"name": "성안당", "margin": 65, "min_free_shipping": 20500},
    {"name": "다락원", "margin": 65, "min_free_shipping": 20500},
    {"name": "에이콘", "margin": 65, "min_free_shipping": 20500},
    {"name": "쎄듀", "margin": 65, "min_free_shipping": 20500},
    {"name": "에듀윌", "margin": 65, "min_free_shipping": 20500},
    {"name": "디딤돌", "margin": 65, "min_free_shipping": 20500},
    {"name": "꿈을담는틀", "margin": 65, "min_free_shipping": 20500},
    {"name": "미래엔에듀", "margin": 65, "min_free_shipping": 20500},
    {"name": "미래엔", "margin": 65, "min_free_shipping": 20500},
    {"name": "키출판사", "margin": 65, "min_free_shipping": 20500},
    {"name": "에듀원", "margin": 62, "min_free_shipping": 18000},
    {"name": "에듀플라자", "margin": 62, "min_free_shipping": 18000},
    {"name": "베스트콜렉션", "margin": 62, "min_free_shipping": 18000},
    {"name": "동아", "margin": 67, "min_free_shipping": 0},
    {"name": "좋은책신사고", "margin": 70, "min_free_shipping": 0},
    {"name": "한국교육방송공사", "margin": 73, "min_free_shipping": 0},
    {"name": "EBS", "margin": 73, "min_free_shipping": 0},
]


def get_publisher_info(publisher_name: str):
    """출판사 정보 조회 (margin, min_free_shipping 포함)"""
    if not publisher_name:
        return None
    for p in PUBLISHERS:
        if p["name"] in publisher_name or publisher_name in p["name"]:
            return p
    return None


def get_publisher_names() -> list:
    """취급 출판사 이름 리스트"""
    return [p["name"] for p in PUBLISHERS]


def resolve_distributor(publisher_name: str) -> str:
    """출판사명 → 거래처명"""
    if not publisher_name:
        return "일반"
    if publisher_name in _PUBLISHER_TO_DISTRIBUTOR:
        return _PUBLISHER_TO_DISTRIBUTOR[publisher_name]
    for pub in sorted(_PUBLISHER_TO_DISTRIBUTOR.keys(), key=len, reverse=True):
        if pub in publisher_name or publisher_name in pub:
            return _PUBLISHER_TO_DISTRIBUTOR[pub]
    return "일반"


def match_publisher_from_text(text: str, pub_names: list) -> str:
    """상품명/옵션명에서 출판사 매칭"""
    if not text:
        return ""
    for pn in pub_names:
        if pn in text:
            return pn
    for series in sorted(SERIES_TO_PUBLISHER.keys(), key=len, reverse=True):
        if series in text:
            return SERIES_TO_PUBLISHER[series]
    return ""


# ─────────────────────────────────────────────
# 사은품/증정품 필터
# ─────────────────────────────────────────────
GIFT_FILTER_KEYWORDS = ['사은품', '선물', '증정', '증정품', '부록', '사은', '임지']


def is_gift_item(item_name: str) -> bool:
    """상품명/옵션명이 사은품·증정품인지 판별"""
    if not item_name:
        return False
    return any(kw in item_name for kw in GIFT_FILTER_KEYWORDS)


# ─────────────────────────────────────────────
# 2026 대한민국 공휴일
# ─────────────────────────────────────────────
KOREAN_HOLIDAYS_2026 = [
    ("2026-01-01", "신정"),
    ("2026-02-16", "설날 연휴"),
    ("2026-02-17", "설날"),
    ("2026-02-18", "설날 연휴"),
    ("2026-03-01", "삼일절"),
    ("2026-03-02", "삼일절 대체공휴일"),
    ("2026-05-05", "어린이날"),
    ("2026-05-24", "부처님오신날"),
    ("2026-05-25", "부처님오신날 대체공휴일"),
    ("2026-06-06", "현충일"),
    ("2026-08-15", "광복절"),
    ("2026-08-17", "광복절 대체공휴일"),
    ("2026-09-24", "추석 연휴"),
    ("2026-09-25", "추석"),
    ("2026-09-26", "추석 연휴"),
    ("2026-09-28", "추석 대체공휴일"),
    ("2026-10-03", "개천절"),
    ("2026-10-05", "개천절 대체공휴일"),
    ("2026-10-09", "한글날"),
    ("2026-12-25", "성탄절"),
]
