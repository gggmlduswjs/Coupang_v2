"""비즈니스 상수 — 매직넘버 중앙 관리"""

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
DEFAULT_SHIPPING_COST = 2000
DEFAULT_RETURN_CHARGE = 2500
FREE_SHIPPING_THRESHOLD = 2000
TARGET_MARGIN_MIN = 1300
TARGET_MARGIN_MAX = 2000
CONDITIONAL_FREE_THRESHOLD = 20000
CONDITIONAL_FREE_THRESHOLD_67 = 25000
CONDITIONAL_FREE_THRESHOLD_70 = 30000
CONDITIONAL_FREE_THRESHOLD_73 = 60000

# ─────────────────────────────────────────────
# 재고
# ─────────────────────────────────────────────
DEFAULT_STOCK = 1000
DEFAULT_LEAD_TIME = 2
LOW_STOCK_THRESHOLD = 3

# ─────────────────────────────────────────────
# 안전장치 (스크립트 일괄 실행 차단)
# ─────────────────────────────────────────────
PRICE_LOCK = True
DELETE_LOCK = True
SALE_STOP_LOCK = True
REGISTER_LOCK = True

# ─────────────────────────────────────────────
# API
# ─────────────────────────────────────────────
API_THROTTLE_SECONDS = 1.0
COUPANG_WING_RATE_LIMIT = 0.1

# ─────────────────────────────────────────────
# 쿠팡 WING API - 도서 상품 등록 기본값
# ─────────────────────────────────────────────
BOOK_PRODUCT_DEFAULTS = {
    "deliveryMethod": "SEQUENCIAL",
    "deliveryChargeType": "CONDITIONAL_FREE",
    "deliveryCharge": DEFAULT_SHIPPING_COST,
    "freeShipOverAmount": CONDITIONAL_FREE_THRESHOLD,
    "deliveryChargeOnReturn": DEFAULT_RETURN_CHARGE,
    "unionDeliveryType": "UNION_DELIVERY",
    "remoteAreaDeliverable": "N",
    "returnCharge": DEFAULT_RETURN_CHARGE,
    "requested": True,
    "adultOnly": "EVERYONE",
    "taxType": "FREE",
    "parallelImported": "NOT_PARALLEL_IMPORTED",
    "overseasPurchased": "NOT_OVERSEAS_PURCHASED",
    "pccNeeded": False,
    "offerCondition": "NEW",
    "outboundShippingTimeDay": 1,
    "maximumBuyForPerson": 0,
}

# 도서 카테고리 코드
BOOK_CATEGORY_CODE = "76236"
BOOK_CATEGORY_MAP = {
    "76236": "고등교재",
    "76239": "기타교재",
    "76243": "수험서",
    "35171": "고등교재",
    "76001": "국내도서",
}

# WING API 계정별 환경변수 매핑
WING_ACCOUNT_ENV_MAP = {
    "007-book": "COUPANG_007BOOK",
    "007-bm":   "COUPANG_007BM",
    "007-ez":   "COUPANG_007EZ",
    "002-bm":   "COUPANG_002BM",
    "big6ceo":  "COUPANG_BIG6CEO",
}

# WING 로그인 계정 환경변수 매핑
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

TIMEOUT_CONFIG = {
    "api_request": 30,
    "db_connect": 30,
}

AUTO_CRAWL_CONFIG = {
    "crawl_hour": 3,
    "max_per_publisher": 50,
    "year_filter": 2025,
    "check_interval": 30,
    "max_items_safety": 200,
}

CRAWL_MIN_PRICE = 5000
CRAWL_EXCLUDE_KEYWORDS = [
    "사전", "잡지", "월간지", "자습서", "평가문제집",
]

PRICE_CONFIG = {
    "min_margin": 1300,
    "target_margin": 2000,
    "min_margin_rate": 0.05,
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
