"""도서 원본 데이터 모델"""
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from datetime import datetime
from core.database import Base
from core.constants import BOOK_DISCOUNT_RATE
import re


class Book(Base):
    """도서 원본 데이터 (알라딘 API)"""
    __tablename__ = "books"

    id = Column(Integer, primary_key=True, index=True)

    # 기본 정보
    isbn = Column(String(13), unique=True, nullable=False, index=True)
    title = Column(String(500), nullable=False)
    author = Column(String(200))  # 저자 (알라딘 API)
    publisher_id = Column(Integer, ForeignKey('publishers.id'), index=True)

    # 가격
    list_price = Column(Integer, nullable=False)  # 정가

    # 연도 및 정규화 (묶음 SKU용)
    year = Column(Integer, index=True)  # 2024, 2025 등
    normalized_title = Column(String(500))  # 연도 제거된 제목
    normalized_series = Column(String(200), index=True)  # 시리즈명

    # 메타데이터
    sales_point = Column(Integer, default=0, index=True)  # 알라딘 판매 포인트
    crawled_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    publisher = relationship("Publisher", back_populates="books")
    products = relationship("Product", back_populates="book")

    def __repr__(self):
        return f"<Book(isbn='{self.isbn}', title='{self.title[:30]}')>"

    @staticmethod
    def extract_year(title: str) -> int:
        """
        제목에서 연도 추출

        예:
            "2025 수능완성 국어영역" → 2025
            "개념원리 수학(상) 2024" → 2024
            "EBS 고등 예비과정 24년도" → 2024
            "고2 수학" → None (학년은 연도가 아님)

        주의:
            - "2학년", "3학년" 등 학년 표현은 연도로 인식하지 않음
            - 2자리 연도는 명시적 연도 표현("년", "학년도", 아포스트로피)만 인식
        """
        # 1순위: 4자리 연도 패턴 (2020~2030)
        patterns = [
            r'(202[0-9])',  # 2020~2029
            r'(203[0])',    # 2030
        ]

        for pattern in patterns:
            match = re.search(pattern, title)
            if match:
                return int(match.group(1))

        # 2순위: 2자리 연도 + 명시적 접미사 (24년도, 24학년도, '24 등)
        # "2학년", "3학년" 등 학년 표현 제외를 위해 20~30만 매칭
        explicit_year_patterns = [
            r"'([2][0-9])\b",           # '24, '25 형태
            r"\b([2][0-9])년도\b",       # 24년도, 25년도
            r"\b([2][0-9])학년도\b",     # 24학년도
            r"\b([2][0-9])년\b",         # 24년 (단, 학년 앞은 제외)
        ]

        for pattern in explicit_year_patterns:
            match = re.search(pattern, title)
            if match:
                year_suffix = int(match.group(1))
                # 20~30 범위만 유효 (2020~2030)
                if 20 <= year_suffix <= 30:
                    return 2000 + year_suffix

        return None

    @staticmethod
    def normalize_title(title: str, year: int = None) -> str:
        """
        제목 정규화 (연도 제거)

        예:
            "2025 수능완성 국어영역" → "수능완성 국어영역"
            "개념원리 수학(상) 2024" → "개념원리 수학(상)"
        """
        normalized = title

        if year:
            # 4자리 연도 제거
            normalized = re.sub(rf'\b{year}\b', '', normalized)
            # 2자리 연도 제거 (24년, 24학년도 등)
            year_suffix = year % 100
            normalized = re.sub(rf'\b{year_suffix}(?:년도?|학년도)?\b', '', normalized)

        # 공백 정리
        normalized = re.sub(r'\s+', ' ', normalized).strip()

        return normalized

    @staticmethod
    def extract_series(normalized_title: str) -> str:
        """
        시리즈명 추출 (묶음용)

        예:
            "개념원리 수학(상)" → "개념원리 수학"
            "EBS 수능완성 국어영역" → "EBS 수능완성"
        """
        # 괄호 제거
        series = re.sub(r'\([^)]*\)', '', normalized_title)

        # 영역/과목 제거
        series = re.sub(r'(국어|영어|수학|과학|사회|역사|지리|생물|화학|물리|지구과학)영역', '', series)

        # 권수 제거
        series = re.sub(r'[상중하]권?', '', series)
        series = re.sub(r'\d+권', '', series)

        # 공백 정리
        series = re.sub(r'\s+', ' ', series).strip()

        return series

    def process_metadata(self):
        """메타데이터 자동 처리"""
        # 연도 추출
        if not self.year:
            self.year = self.extract_year(self.title)

        # 제목 정규화
        if not self.normalized_title:
            self.normalized_title = self.normalize_title(self.title, self.year)

        # 시리즈명 추출
        if not self.normalized_series and self.normalized_title:
            self.normalized_series = self.extract_series(self.normalized_title)

    @property
    def sale_price(self):
        """판매가 (도서정가제)"""
        return int(self.list_price * BOOK_DISCOUNT_RATE)

    def calculate_margin(self, publisher=None):
        """마진 계산 (출판사 정보 필요)"""
        if not publisher and self.publisher:
            publisher = self.publisher

        if publisher:
            return publisher.calculate_margin(self.list_price)

        return None
