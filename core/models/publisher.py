"""출판사 모델"""
from sqlalchemy import Column, Integer, String, Boolean, Text, Float, DateTime
from sqlalchemy.orm import relationship
from datetime import datetime
from core.database import Base
from core.constants import (
    BOOK_DISCOUNT_RATE, COUPANG_FEE_RATE, DEFAULT_SHIPPING_COST,
    FREE_SHIPPING_THRESHOLD, determine_customer_shipping_fee,
)


class Publisher(Base):
    """출판사 정보"""
    __tablename__ = "publishers"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), unique=True, nullable=False, index=True)
    margin_rate = Column(Integer, nullable=False)  # 매입률 (40~73)
    min_free_shipping = Column(Integer, nullable=False)  # 무료배송 기준 (원)
    supply_rate = Column(Float, nullable=False)  # 공급률 (0.27~0.60)
    is_active = Column(Boolean, default=True, index=True)
    notes = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    books = relationship("Book", back_populates="publisher")
    bundle_skus = relationship("BundleSKU", back_populates="publisher")

    def __repr__(self):
        return f"<Publisher(name='{self.name}', margin_rate={self.margin_rate}%)>"

    @property
    def margin_percentage(self):
        """매입률 → 마진율 변환"""
        return 100 - self.margin_rate

    def calculate_margin(self, list_price: int) -> dict:
        """
        마진 계산 (공급률 + 정가 기준 배송비 결정)

        순마진 = 판매가 - 공급가 - 수수료 - 셀러부담배송비
        셀러부담배송비 = 실제택배비(2,300) - 고객부담배송비
        """
        sale_price = int(list_price * BOOK_DISCOUNT_RATE)
        supply_cost = int(list_price * self.supply_rate)
        coupang_fee = int(sale_price * COUPANG_FEE_RATE)
        margin_per_unit = sale_price - supply_cost - coupang_fee

        # 고객 부담 배송비 결정 (공급률 + 정가 기반)
        customer_shipping_fee = determine_customer_shipping_fee(self.margin_rate, list_price)
        seller_shipping_cost = DEFAULT_SHIPPING_COST - customer_shipping_fee

        # 순마진 = 마진 - 셀러 부담 배송비
        net_margin = margin_per_unit - seller_shipping_cost

        if customer_shipping_fee == 0:
            shipping_policy = 'free'
        else:
            shipping_policy = 'paid'

        return {
            'sale_price': sale_price,
            'supply_cost': supply_cost,
            'coupang_fee': coupang_fee,
            'margin_per_unit': margin_per_unit,
            'net_margin': net_margin,
            'shipping_cost': seller_shipping_cost,
            'customer_shipping_fee': customer_shipping_fee,
            'shipping_policy': shipping_policy,
        }

    def determine_shipping_policy(self, list_price: int) -> str:
        """배송 정책 자동 판단"""
        return self.calculate_margin(list_price)['shipping_policy']

    def can_upload_single(self, list_price: int) -> bool:
        """단권 업로드 가능 여부 (순마진 >= 0)"""
        return self.calculate_margin(list_price)['net_margin'] >= 0
