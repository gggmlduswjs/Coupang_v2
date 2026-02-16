"""마진 계산 및 수익성 분석 모듈"""
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from typing import Dict, List
from core.database import SessionLocal
from core.models import Publisher, Book, Product
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MarginCalculator:
    """
    마진 계산 및 수익성 분석기

    도서정가제 준수하며 출판사별 공급률 기반으로
    수익성을 자동 판단하고 업로드 전략 제시
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

    def analyze_book(self, book: Book, publisher: Publisher = None) -> Dict:
        """
        단일 도서 마진 분석

        Args:
            book: Book 인스턴스
            publisher: Publisher 인스턴스 (없으면 자동 조회)

        Returns:
            {
                'book_id': int,
                'isbn': str,
                'title': str,
                'publisher': str,
                'list_price': int,
                'sale_price': int,
                'supply_cost': int,
                'coupang_fee': int,
                'margin_per_unit': int,
                'net_margin': int,
                'shipping_policy': str,
                'can_upload_single': bool,
                'profitability': str,
                'recommendation': str
            }
        """
        # Publisher 조회
        if not publisher:
            if book.publisher_id:
                publisher = self.db.query(Publisher).get(book.publisher_id)

        if not publisher:
            logger.warning(f"출판사를 찾을 수 없습니다: book_id={book.id}, isbn={book.isbn}")
            return None

        # 마진 계산
        margin_info = publisher.calculate_margin(book.list_price)
        shipping_policy = publisher.determine_shipping_policy(book.list_price)
        can_upload = publisher.can_upload_single(book.list_price)

        # 수익성 등급
        net_margin = margin_info['net_margin']
        if net_margin >= 5000:
            profitability = 'excellent'
            recommendation = '무료배송 단권 업로드 강력 권장'
        elif net_margin >= 2000:
            profitability = 'good'
            recommendation = '무료배송 단권 업로드 권장'
        elif net_margin >= 0:
            profitability = 'acceptable'
            recommendation = '유료배송 단권 업로드 가능'
        else:
            profitability = 'poor'
            recommendation = '묶음 SKU 필수 (단권 손실)'

        return {
            'book_id': book.id,
            'isbn': book.isbn,
            'title': book.title,
            'publisher': publisher.name,
            'publisher_id': publisher.id,
            'list_price': book.list_price,
            'sale_price': margin_info['sale_price'],
            'supply_cost': margin_info['supply_cost'],
            'coupang_fee': margin_info['coupang_fee'],
            'margin_per_unit': margin_info['margin_per_unit'],
            'net_margin': net_margin,
            'shipping_cost': margin_info['shipping_cost'],
            'shipping_policy': shipping_policy,
            'can_upload_single': can_upload,
            'profitability': profitability,
            'recommendation': recommendation
        }

    def create_product_from_analysis(self, book: Book, publisher: Publisher = None) -> Product:
        """
        마진 분석 결과로부터 Product 생성

        Args:
            book: Book 인스턴스
            publisher: Publisher 인스턴스

        Returns:
            Product 인스턴스
        """
        if not publisher:
            publisher = self.db.query(Publisher).get(book.publisher_id)

        if not publisher:
            raise ValueError(f"출판사를 찾을 수 없습니다: book_id={book.id}")

        # Product 생성 (모델의 클래스 메서드 사용)
        product = Product.create_from_book(book, publisher)

        return product

    def batch_analyze_books(self, book_ids: List[int] = None) -> Dict:
        """
        다수 도서 일괄 분석

        Args:
            book_ids: 분석할 Book ID 리스트 (없으면 미처리 전체)

        Returns:
            {
                'total': int,
                'analyzed': int,
                'by_profitability': {
                    'excellent': [],
                    'good': [],
                    'acceptable': [],
                    'poor': []
                },
                'by_shipping': {
                    'free': [],
                    'paid': [],
                    'bundle_required': []
                },
                'summary': {
                    'uploadable_single': int,
                    'requires_bundle': int,
                    'total_margin': int
                }
            }
        """
        # 도서 조회
        query = self.db.query(Book)

        if book_ids:
            query = query.filter(Book.id.in_(book_ids))
        else:
            # Product가 없는 도서만 (미분석)
            from sqlalchemy import exists
            query = query.filter(
                ~exists().where(Product.book_id == Book.id)
            )

        books = query.all()

        results = {
            'total': len(books),
            'analyzed': 0,
            'by_profitability': {
                'excellent': [],
                'good': [],
                'acceptable': [],
                'poor': []
            },
            'by_shipping': {
                'free': [],
                'paid': [],
                'bundle_required': []
            },
            'summary': {
                'uploadable_single': 0,
                'requires_bundle': 0,
                'total_margin': 0
            }
        }

        for book in books:
            analysis = self.analyze_book(book)

            if not analysis:
                continue

            results['analyzed'] += 1

            # 수익성별 분류
            profitability = analysis['profitability']
            results['by_profitability'][profitability].append(analysis)

            # 배송정책별 분류
            shipping = analysis['shipping_policy']
            results['by_shipping'][shipping].append(analysis)

            # 통계
            if analysis['can_upload_single']:
                results['summary']['uploadable_single'] += 1
            else:
                results['summary']['requires_bundle'] += 1

            results['summary']['total_margin'] += analysis['net_margin']

        return results

    def get_profitability_report(self, analysis_results: Dict) -> str:
        """
        수익성 분석 리포트 생성

        Args:
            analysis_results: batch_analyze_books() 결과

        Returns:
            포맷된 리포트 문자열
        """
        report = []
        report.append("\n" + "="*60)
        report.append("마진 분석 리포트")
        report.append("="*60)

        total = analysis_results['total']
        analyzed = analysis_results['analyzed']

        report.append(f"\n총 도서: {total}개")
        report.append(f"분석 완료: {analyzed}개")

        # 수익성 통계
        report.append("\n[수익성 분류]")
        for level in ['excellent', 'good', 'acceptable', 'poor']:
            count = len(analysis_results['by_profitability'][level])
            if count > 0:
                percentage = count / analyzed * 100
                report.append(f"  {level:12s}: {count:3d}개 ({percentage:5.1f}%)")

        # 배송정책 통계
        report.append("\n[배송 정책]")
        for policy in ['free', 'paid', 'bundle_required']:
            count = len(analysis_results['by_shipping'][policy])
            if count > 0:
                percentage = count / analyzed * 100
                report.append(f"  {policy:16s}: {count:3d}개 ({percentage:5.1f}%)")

        # 요약
        summary = analysis_results['summary']
        report.append("\n[요약]")
        report.append(f"  단권 업로드 가능: {summary['uploadable_single']}개")
        report.append(f"  묶음 SKU 필요: {summary['requires_bundle']}개")
        report.append(f"  예상 총 마진: {summary['total_margin']:,}원")

        if analyzed > 0:
            avg_margin = summary['total_margin'] / analyzed
            report.append(f"  평균 마진: {avg_margin:,.0f}원")

        report.append("\n" + "="*60)

        return "\n".join(report)


def test_margin_calculator():
    """마진 계산기 테스트"""
    print("\n" + "="*60)
    print("마진 계산기 테스트")
    print("="*60)

    with MarginCalculator() as calc:
        # 출판사별 샘플 가격 테스트
        test_cases = [
            ("gaennyeom", 15000, "개념원리 65%"),
            ("gilbut", 30000, "길벗 60%"),
            ("EBS", 10000, "EBS 73%"),
            ("goodbook", 40000, "좋은책신사고 70%"),
        ]

        print("\n[출판사별 마진 테스트]")
        for pub_name, price, desc in test_cases:
            publisher = calc.db.query(Publisher).filter(
                Publisher.name == pub_name
            ).first()

            if publisher:
                margin_info = publisher.calculate_margin(price)
                policy = publisher.determine_shipping_policy(price)

                print(f"\n{desc} - 정가 {price:,}원")
                print(f"  판매가: {margin_info['sale_price']:,}원")
                print(f"  순마진: {margin_info['net_margin']:,}원")
                print(f"  배송정책: {policy}")
                print(f"  단권 업로드: {'가능' if publisher.can_upload_single(price) else '불가'}")

    print("\n" + "="*60)
    print("테스트 완료")
    print("="*60)


if __name__ == "__main__":
    test_margin_calculator()
