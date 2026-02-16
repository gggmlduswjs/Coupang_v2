"""묶음 SKU 자동 생성 모듈"""
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from typing import Dict, List, Tuple
from core.database import SessionLocal
from core.models import Publisher, Book, BundleSKU
from sqlalchemy import func
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BundleGenerator:
    """
    묶음 SKU 자동 생성기

    저마진 도서들을 동일 시리즈+연도로 묶어서
    무료배송 가능한 세트 상품 생성
    """

    def __init__(self, db_session=None):
        """
        Args:
            db_session: SQLAlchemy 세션 (없으면 자동 생성)
        """
        self.db = db_session or SessionLocal()
        self._own_session = db_session is None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._own_session:
            self.db.close()

    def find_bundleable_books(
        self,
        min_books: int = 2,
        max_books: int = 10,
        require_year: bool = True
    ) -> List[Dict]:
        """
        묶음 가능한 도서 그룹 찾기

        Args:
            min_books: 최소 묶음 권수
            max_books: 최대 묶음 권수
            require_year: 연도 필수 여부

        Returns:
            [
                {
                    'publisher_id': int,
                    'normalized_series': str,
                    'year': int,
                    'book_count': int,
                    'books': [Book, ...]
                },
                ...
            ]
        """
        # 시리즈+연도별 그룹핑
        query = self.db.query(
            Book.publisher_id,
            Book.normalized_series,
            Book.year,
            func.count(Book.id).label('book_count')
        ).filter(
            Book.normalized_series.isnot(None),
            Book.normalized_series != ''
        )

        if require_year:
            query = query.filter(Book.year.isnot(None))

        groups = query.group_by(
            Book.publisher_id,
            Book.normalized_series,
            Book.year
        ).having(
            func.count(Book.id) >= min_books,
            func.count(Book.id) <= max_books
        ).all()

        bundleable_groups = []

        for group in groups:
            # 그룹에 속한 도서들 조회
            books_query = self.db.query(Book).filter(
                Book.publisher_id == group.publisher_id,
                Book.normalized_series == group.normalized_series
            )

            if group.year:
                books_query = books_query.filter(Book.year == group.year)

            books = books_query.all()

            bundleable_groups.append({
                'publisher_id': group.publisher_id,
                'normalized_series': group.normalized_series,
                'year': group.year,
                'book_count': group.book_count,
                'books': books
            })

        return bundleable_groups

    def create_bundle(
        self,
        books: List[Book],
        publisher: Publisher,
        year: int,
        normalized_series: str
    ) -> BundleSKU:
        """
        묶음 SKU 생성

        Args:
            books: 묶을 Book 리스트
            publisher: Publisher 인스턴스
            year: 연도
            normalized_series: 시리즈명

        Returns:
            BundleSKU 인스턴스
        """
        if not books:
            raise ValueError("묶을 도서가 없습니다")

        if len(books) < 2:
            raise ValueError("묶음은 최소 2권 이상 필요합니다")

        # 중복 체크
        bundle_key = f"{publisher.id}_{normalized_series}_{year}"
        existing = self.db.query(BundleSKU).filter(
            BundleSKU.bundle_key == bundle_key
        ).first()

        if existing:
            logger.warning(f"이미 존재하는 묶음: {bundle_key}")
            return existing

        # 묶음 생성 (모델의 클래스 메서드 사용)
        bundle = BundleSKU.create_bundle(books, publisher, year, normalized_series)

        self.db.add(bundle)
        self.db.commit()

        logger.info(f"묶음 생성: {bundle.bundle_name} ({len(books)}권, 순마진 {bundle.net_margin:,}원)")

        return bundle

    def auto_generate_bundles(
        self,
        min_margin: int = 2000,
        min_books: int = 2,
        max_books: int = 5
    ) -> Dict:
        """
        자동 묶음 생성

        Args:
            min_margin: 최소 순마진 (원)
            min_books: 최소 묶음 권수
            max_books: 최대 묶음 권수

        Returns:
            {
                'total_groups': int,
                'created': int,
                'skipped': int,
                'bundles': [BundleSKU, ...],
                'errors': [str, ...]
            }
        """
        result = {
            'total_groups': 0,
            'created': 0,
            'skipped': 0,
            'bundles': [],
            'errors': []
        }

        # 묶음 가능한 그룹 찾기
        groups = self.find_bundleable_books(min_books, max_books, require_year=True)
        result['total_groups'] = len(groups)

        logger.info(f"묶음 가능 그룹: {len(groups)}개")

        for group in groups:
            try:
                # 출판사 조회
                publisher = self.db.query(Publisher).get(group['publisher_id'])

                if not publisher:
                    result['errors'].append(f"출판사 없음: {group['publisher_id']}")
                    result['skipped'] += 1
                    continue

                # 묶음 마진 계산
                books = group['books']
                total_list_price = sum(book.list_price for book in books)
                margin_info = publisher.calculate_margin(total_list_price)

                # 최소 마진 체크
                if margin_info['net_margin'] < min_margin:
                    logger.debug(f"마진 부족: {group['normalized_series']} (순마진 {margin_info['net_margin']:,}원)")
                    result['skipped'] += 1
                    continue

                # 묶음 생성
                bundle = self.create_bundle(
                    books,
                    publisher,
                    group['year'],
                    group['normalized_series']
                )

                result['bundles'].append(bundle)
                result['created'] += 1

            except Exception as e:
                error_msg = f"묶음 생성 오류: {group['normalized_series']} - {str(e)}"
                logger.error(error_msg)
                result['errors'].append(error_msg)
                result['skipped'] += 1

        return result

    def get_bundle_candidates_report(
        self,
        min_books: int = 2,
        max_books: int = 5
    ) -> str:
        """
        묶음 후보 리포트 생성

        Args:
            min_books: 최소 묶음 권수
            max_books: 최대 묶음 권수

        Returns:
            포맷된 리포트 문자열
        """
        groups = self.find_bundleable_books(min_books, max_books, require_year=True)

        report = []
        report.append("\n" + "="*60)
        report.append("묶음 SKU 후보 리포트")
        report.append("="*60)

        report.append(f"\n총 후보 그룹: {len(groups)}개")

        if not groups:
            report.append("\n묶음 가능한 그룹이 없습니다.")
            report.append("\n조건:")
            report.append(f"  - 최소 {min_books}권")
            report.append(f"  - 최대 {max_books}권")
            report.append("  - 동일 출판사")
            report.append("  - 동일 시리즈")
            report.append("  - 동일 연도")
            report.append("\n" + "="*60)
            return "\n".join(report)

        # 출판사별 통계
        by_publisher = {}
        for group in groups:
            pub_id = group['publisher_id']
            if pub_id not in by_publisher:
                by_publisher[pub_id] = []
            by_publisher[pub_id].append(group)

        report.append("\n[출판사별 묶음 후보]")
        for pub_id, pub_groups in by_publisher.items():
            publisher = self.db.query(Publisher).get(pub_id)
            if publisher:
                report.append(f"\n{publisher.name} (매입률 {publisher.margin_rate}%)")
                for group in pub_groups[:3]:  # 최대 3개만 표시
                    total_price = sum(book.list_price for book in group['books'])
                    margin_info = publisher.calculate_margin(total_price)

                    report.append(f"  - {group['normalized_series']} ({group['year']}년)")
                    report.append(f"    권수: {group['book_count']}권, 총 정가: {total_price:,}원")
                    report.append(f"    순마진: {margin_info['net_margin']:,}원")

                if len(pub_groups) > 3:
                    report.append(f"  ... 외 {len(pub_groups) - 3}개 그룹")

        report.append("\n" + "="*60)

        return "\n".join(report)


def test_bundle_generator():
    """묶음 생성기 테스트"""
    print("\n" + "="*60)
    print("묶음 SKU 생성기 테스트")
    print("="*60)

    with BundleGenerator() as generator:
        # 묶음 후보 조회
        print("\n[묶음 가능 그룹 검색]")
        groups = generator.find_bundleable_books(min_books=2, max_books=5)

        print(f"찾은 그룹: {len(groups)}개")

        if groups:
            for i, group in enumerate(groups[:5], 1):
                publisher = generator.db.query(Publisher).get(group['publisher_id'])
                total_price = sum(book.list_price for book in group['books'])

                print(f"\n{i}. {group['normalized_series']} ({group['year']}년)")
                print(f"   출판사: {publisher.name if publisher else 'Unknown'}")
                print(f"   권수: {group['book_count']}권")
                print(f"   총 정가: {total_price:,}원")

                if publisher:
                    margin_info = publisher.calculate_margin(total_price)
                    print(f"   순마진: {margin_info['net_margin']:,}원")

        # 리포트 출력
        report = generator.get_bundle_candidates_report()
        print(report)

    print("\n" + "="*60)
    print("테스트 완료")
    print("="*60)


if __name__ == "__main__":
    test_bundle_generator()
