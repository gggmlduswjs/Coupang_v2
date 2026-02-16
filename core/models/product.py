"""단권 상품 모델"""
from sqlalchemy import Column, Integer, String, Boolean, Text, Float, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from datetime import datetime
from core.database import Base
from core.constants import DEFAULT_SHIPPING_COST, DEFAULT_STOCK


class Product(Base):
    """단권 상품 (마진/배송 정보 포함)"""
    __tablename__ = "products"

    id = Column(Integer, primary_key=True, index=True)

    # 도서 참조
    book_id = Column(Integer, ForeignKey('books.id'), nullable=False, index=True)
    isbn = Column(String(13), nullable=False, index=True)

    # 가격 (도서정가제)
    list_price = Column(Integer, nullable=False)  # 정가
    sale_price = Column(Integer, nullable=False)  # 판매가 (정가 x 0.9)

    # 마진 분석
    supply_rate = Column(Float, nullable=False)  # 공급률 (0.27~0.60)
    margin_per_unit = Column(Integer, nullable=False)  # 권당 마진 (원)
    shipping_cost = Column(Integer, default=DEFAULT_SHIPPING_COST)  # 배송비 (원)
    net_margin = Column(Integer, nullable=False)  # 순마진 (마진 - 배송비)

    # 배송 정책
    shipping_policy = Column(String(20), nullable=False, index=True)  # 'free', 'paid', 'bundle_required'
    can_upload_single = Column(Boolean, default=True)  # 단권 업로드 가능 여부

    # 상태
    status = Column(String(20), default='ready', index=True)  # ready, uploaded, excluded
    exclude_reason = Column(Text)  # 제외 사유

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    book = relationship("Book", back_populates="products")
    listings = relationship("Listing", back_populates="product")

    def __repr__(self):
        return f"<Product(isbn='{self.isbn}', net_margin={self.net_margin}, policy='{self.shipping_policy}')>"

    @classmethod
    def create_from_book(cls, book, publisher):
        """Book과 Publisher로부터 Product 생성"""
        # 마진 계산
        margin_info = publisher.calculate_margin(book.list_price)

        # 배송 정책 결정
        shipping_policy = publisher.determine_shipping_policy(book.list_price)
        can_upload_single = publisher.can_upload_single(book.list_price)

        product = cls(
            book_id=book.id,
            isbn=book.isbn,
            list_price=book.list_price,
            sale_price=margin_info['sale_price'],
            supply_rate=publisher.supply_rate,
            margin_per_unit=margin_info['margin_per_unit'],
            shipping_cost=margin_info['shipping_cost'],
            net_margin=margin_info['net_margin'],
            shipping_policy=shipping_policy,
            can_upload_single=can_upload_single,
            status='ready'
        )

        # 단권 업로드 불가능하면 제외 사유 기록
        if not can_upload_single:
            product.status = 'excluded'
            product.exclude_reason = f'순마진 부족 ({margin_info["net_margin"]:,}원 < 0원). 묶음 SKU 필요.'

        return product

    @property
    def can_upload(self):
        """업로드 가능한 상품인지 (ready + 단권가능)"""
        return self.status == 'ready' and self.can_upload_single

    @property
    def is_profitable(self):
        """수익성 있는 상품인지"""
        return self.net_margin >= 0

    @property
    def is_free_shipping_eligible(self):
        """무료배송 가능 상품인지"""
        return self.shipping_policy == 'free'
