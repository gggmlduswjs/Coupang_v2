"""쿠팡 공식 템플릿 Ver.4.5에 맞춘 CSV 생성기"""
import csv
from pathlib import Path
from typing import List, Dict
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# 쿠팡 공식 템플릿 Ver.4.5 컬럼 정의 (113개)
# 구매옵션/검색옵션은 동일한 컬럼명이 반복되므로
# 위치(순서) 기반으로 처리
# ─────────────────────────────────────────────

COLUMNS = (
    # ── 기본정보 (9개) ──
    ["카테고리", "등록상품명", "판매시작일", "판매종료일",
     "상품상태", "상태설명", "브랜드", "제조사", "검색어"]

    # ── 구매옵션 (12개: 옵션유형1~6, 옵션값1~6) ──
    + [f"옵션유형{i}" for i in range(1, 7)
       for _ in ("유형", "값")]  # placeholder, 아래서 정정

    # ── 검색옵션 (40개: 옵션유형1~20, 옵션값1~20) ──
    # ...
)

# 정확한 컬럼명 리스트 (템플릿 순서 그대로 113개)
def _build_columns():
    cols = []

    # 기본정보 (9)
    cols += ["카테고리", "등록상품명", "판매시작일", "판매종료일",
             "상품상태", "상태설명", "브랜드", "제조사", "검색어"]

    # 구매옵션 (12: 유형1,값1,...,유형6,값6)
    for i in range(1, 7):
        cols.append(f"옵션유형{i}")
        cols.append(f"옵션값{i}")

    # 검색옵션 (40: 유형1,값1,...,유형20,값20)
    # 컬럼명이 구매옵션과 동일하지만 위치가 다름
    for i in range(1, 21):
        cols.append(f"옵션유형{i}")
        cols.append(f"옵션값{i}")

    # 구성정보 (13)
    cols += ["판매가격", "할인율기준가", "재고수량", "출고리드타임",
             "인당최대구매수량", "최대구매수량기간(일)",
             "성인상품(19)", "과세여부", "병행수입여부", "해외구매대행",
             "업체상품코드", "모델번호", "바코드"]

    # 인증정보 3세트 (각 4개 = 12)
    for _ in range(3):
        cols.append("인증∙신고 등 정보유형")
        cols.append("인증∙신고 등 정보값")
        cols.append("인증∙이동통신 사전승낙서 또는 인증대리점 인증서")
        cols.append("인증∙판매점 사전승낙 인증마크 또는 이동통신사 대리점 인증마크")

    # 주문 추가메시지 (1)
    cols.append("주문 추가메시지")

    # 상품고시정보 (15: 카테고리 + 값1~14)
    cols.append("상품고시정보 카테고리")
    for i in range(1, 15):
        cols.append(f"상품고시정보값{i}")

    # 이미지 (3)
    cols += ["대표(옵션)이미지", "추가이미지", "상태이미지(중고상품)"]

    # 상세 설명 (1)
    cols.append("상세 설명")

    # 구비서류 (7)
    for i in range(1, 8):
        cols.append(f"구비서류값{i}")

    return cols


TEMPLATE_COLUMNS = _build_columns()  # 113개


class CoupangCSVGenerator:
    """쿠팡 공식 템플릿 Ver.4.5 기반 CSV 생성"""

    def __init__(self, output_dir: str = "data/uploads"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def generate_csv(
        self,
        products: List[Dict],
        account_name: str = "default"
    ) -> str:
        """
        쿠팡 공식 템플릿 형식의 CSV 생성

        동일한 컬럼명이 반복되므로(구매옵션/검색옵션, 인증정보 3세트)
        csv 모듈로 위치 기반 직접 작성

        Args:
            products: 상품 정보 리스트
            account_name: 계정 이름

        Returns:
            생성된 CSV 파일 경로
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"coupang_official_{account_name}_{timestamp}.csv"
        filepath = self.output_dir / filename

        with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)

            # 헤더 (113개 컬럼, 중복명 포함)
            writer.writerow(TEMPLATE_COLUMNS)

            # 데이터 행
            for product in products:
                row = self._create_row(product)
                writer.writerow(row)

        logger.info(f"CSV 생성: {filepath} ({len(products)}개 상품)")
        return str(filepath)

    def _create_row(self, product: Dict) -> list:
        """템플릿 Ver.4.5 순서에 맞춰 행 데이터 생성 (113개 값)"""
        row = []

        # ── 기본정보 (9) ──
        row.append("도서")                                          # 카테고리
        row.append(product.get("product_name", ""))                 # 등록상품명
        row.append("")                                              # 판매시작일 (빈값=내일)
        row.append("")                                              # 판매종료일 (빈값=2099-12-31)
        row.append("새상품")                                        # 상품상태
        row.append("")                                              # 상태설명
        row.append(product.get("publisher", "기타"))                 # 브랜드
        row.append(product.get("publisher", "기타"))                 # 제조사
        row.append(self._generate_keywords(product))                # 검색어

        # ── 구매옵션 (12: 유형1,값1,...,유형6,값6) ──
        # 도서는 단일상품이므로 모두 빈값
        for _ in range(12):
            row.append("")

        # ── 검색옵션 (40: 유형1,값1,...,유형20,값20) ──
        # 도서는 대부분 불필요, 모두 빈값
        for _ in range(40):
            row.append("")

        # ── 구성정보 (13) ──
        row.append(product.get("sale_price", 0))                    # 판매가격
        row.append(product.get("original_price", 0))                # 할인율기준가
        row.append(10)                                              # 재고수량
        row.append(2)                                               # 출고리드타임 (2일)
        row.append(0)                                               # 인당최대구매수량 (0=무제한)
        row.append(1)                                               # 최대구매수량기간(일)
        row.append("N")                                             # 성인상품(19)
        row.append("Y")                                             # 과세여부
        row.append("N")                                             # 병행수입여부
        row.append("N")                                             # 해외구매대행
        row.append(product.get("isbn", ""))                         # 업체상품코드
        row.append("")                                              # 모델번호
        row.append(product.get("isbn", ""))                         # 바코드

        # ── 인증정보 1세트 (4) ──
        row.append("인증대상아님")                                   # 인증∙신고 등 정보유형
        row.append("")                                              # 인증∙신고 등 정보값
        row.append("")                                              # 인증∙이동통신 사전승낙서...
        row.append("")                                              # 인증∙판매점 사전승낙...

        # ── 인증정보 2세트 (4) ──
        for _ in range(4):
            row.append("")

        # ── 인증정보 3세트 (4) ──
        for _ in range(4):
            row.append("")

        # ── 주문 추가메시지 (1) ──
        row.append("")

        # ── 상품고시정보 (15: 카테고리 + 값1~14) ──
        row.append("도서")                                          # 상품고시정보 카테고리
        row.append(product.get("product_name", ""))                 # 값1: 도서명
        author = product.get("author", "저자미상")
        publisher = product.get("publisher", "")
        row.append(f"{author} / {publisher}")                       # 값2: 저자/출판사
        row.append("상세페이지 참조")                                # 값3: 크기/페이지수
        row.append("")                                              # 값4: 출간일
        row.append("도서")                                          # 값5: 구성
        desc = product.get("description", "")
        row.append(desc if desc else "상세페이지 참조")              # 값6: 목차/책소개
        for _ in range(8):                                          # 값7~14: 빈값
            row.append("")

        # ── 이미지 (3) ──
        row.append(product.get("main_image_url", ""))               # 대표(옵션)이미지
        row.append("")                                              # 추가이미지
        row.append("")                                              # 상태이미지(중고상품)

        # ── 상세 설명 (1) ──
        row.append(product.get("main_image_url", ""))               # 상세 설명 (이미지 URL)

        # ── 구비서류 (7) ──
        for _ in range(7):
            row.append("")

        return row

    def _generate_keywords(self, product: Dict) -> str:
        """검색어 생성 (/ 구분, 최대 20개)"""
        keywords = []
        title = product.get("product_name", "")

        # 학교급별
        if "초등" in title:
            keywords.extend(["초등", "초등학생", "초등교재"])
        if "중등" in title or "중학" in title:
            keywords.extend(["중등", "중학생", "중등교재"])
        if "고등" in title or "고1" in title or "고2" in title or "고3" in title:
            keywords.extend(["고등", "고등학생", "고등교재"])
        if "수능" in title:
            keywords.extend(["수능", "수능대비", "수능교재"])

        # 과목별
        subject_map = {
            "수학": ["수학", "수학문제집", "수학교재"],
            "국어": ["국어", "국어문제집", "독해력"],
            "영어": ["영어", "영어문법", "영어교재"],
            "과학": ["과학", "과학교재"],
            "사회": ["사회", "사회교재"],
        }
        for subject, kws in subject_map.items():
            if subject in title:
                keywords.extend(kws)

        # 학년
        for grade in ["1학년", "2학년", "3학년", "4학년", "5학년", "6학년"]:
            if grade in title:
                keywords.append(grade)

        # 시리즈/브랜드 키워드
        brand_keywords = ["개념원리", "쎈", "마더텅", "EBS", "비상", "신사고"]
        for bk in brand_keywords:
            if bk in title:
                keywords.append(bk)

        # 출판사 (검색어에서 카테고리/상품명/브랜드/제조사와 중복X 권장이지만
        # 도서의 경우 출판사 검색이 유용하므로 포함)
        publisher = product.get("publisher", "")
        if publisher and publisher not in keywords:
            keywords.append(publisher)

        # 중복 제거 및 최대 20개
        keywords = list(dict.fromkeys(keywords))[:20]
        return "/".join(keywords)

    def generate_batch_csvs(
        self,
        products: List[Dict],
        accounts: List[str]
    ) -> Dict[str, str]:
        """
        여러 계정용 CSV 일괄 생성

        Args:
            products: 상품 정보 리스트
            accounts: 계정 이름 리스트

        Returns:
            {account_name: csv_filepath}
        """
        result = {}

        for account_name in accounts:
            filepath = self.generate_csv(products, account_name)
            result[account_name] = filepath

        logger.info(f"일괄 CSV 생성 완료: {len(accounts)}개 계정")
        return result


# 테스트 함수
def test_generate():
    """테스트 CSV 생성 및 컬럼 수 검증"""
    test_products = [
        {
            "product_name": "개념원리 수학 (상) 2026학년도",
            "original_price": 17100,
            "sale_price": 15390,
            "isbn": "9788961336512",
            "publisher": "개념원리",
            "author": "이홍섭",
            "main_image_url": "https://image.aladin.co.kr/product/12345/cover500/test.jpg",
            "description": "수학의 기본 개념과 원리를 체계적으로 학습할 수 있는 교재입니다."
        },
    ]

    print(f"템플릿 컬럼 수: {len(TEMPLATE_COLUMNS)}개")

    generator = CoupangCSVGenerator()
    filepath = generator.generate_csv(test_products, "test")

    # 검증: 생성된 CSV 읽기
    with open(filepath, "r", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        header = next(reader)
        data_row = next(reader)

    print(f"CSV 헤더 컬럼 수: {len(header)}개")
    print(f"CSV 데이터 컬럼 수: {len(data_row)}개")
    assert len(header) == len(data_row), "헤더와 데이터 컬럼 수 불일치!"
    assert len(header) == len(TEMPLATE_COLUMNS), f"템플릿({len(TEMPLATE_COLUMNS)})과 헤더({len(header)}) 불일치!"

    print(f"\nCSV 파일: {filepath}")
    print("검증 통과!")


if __name__ == "__main__":
    test_generate()
