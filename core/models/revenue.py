"""매출/정산 내역 모델"""
from sqlalchemy import Column, Integer, BigInteger, String, Float, Text, ForeignKey, Date, DateTime, UniqueConstraint, Index
from sqlalchemy.orm import relationship
from datetime import datetime
from core.database import Base


class RevenueHistory(Base):
    """주문-아이템 단위 매출 내역 (Revenue History API 원본)"""

    __tablename__ = "revenue_history"
    __table_args__ = (
        UniqueConstraint("account_id", "order_id", "vendor_item_id", name="uix_account_order_item"),
        Index("ix_rev_account_date", "account_id", "recognition_date"),
        Index("ix_rev_recognition", "recognition_date"),
        Index("ix_rev_listing", "listing_id"),
        Index("ix_rev_sale_type", "sale_type"),
        Index("ix_rev_sale_date", "sale_date"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts.id"), nullable=False)

    # 주문 정보
    order_id = Column(BigInteger, nullable=False)
    sale_type = Column(String(50), nullable=False)  # SALE / REFUND
    sale_date = Column(Date, nullable=False)
    recognition_date = Column(Date, nullable=False)
    settlement_date = Column(Date)

    # 상품 정보
    product_id = Column(BigInteger)  # 쿠팡 노출상품 ID
    product_name = Column(String(500))
    vendor_item_id = Column(BigInteger)
    vendor_item_name = Column(String(500))

    # 금액 정보
    sale_price = Column(Integer, default=0)          # 총 판매가 (수량 반영)
    quantity = Column(Integer, default=0)
    coupang_discount = Column(Integer, default=0)    # 쿠팡지원할인
    sale_amount = Column(Integer, default=0)         # 매출금액
    seller_discount = Column(Integer, default=0)     # 판매자할인쿠폰
    service_fee = Column(Integer, default=0)         # 서비스이용료
    service_fee_vat = Column(Integer, default=0)
    service_fee_ratio = Column(Float)                # 서비스이용율(%)
    settlement_amount = Column(Integer, default=0)   # 정산금액
    delivery_fee_amount = Column(Integer, default=0)
    delivery_fee_settlement = Column(Integer, default=0)

    # 매칭
    listing_id = Column(Integer, ForeignKey("listings.id"))

    # Relationships
    account = relationship("Account", backref="revenue_history")
    listing = relationship("Listing", back_populates="revenue_history")

    created_at = Column(DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f"<RevenueHistory(order={self.order_id}, item={self.vendor_item_id}, type={self.sale_type})>"


class SettlementHistory(Base):
    """계정별 월간 정산 내역 (Settlement History API 원본)"""

    __tablename__ = "settlement_history"
    __table_args__ = (
        UniqueConstraint("account_id", "year_month", "settlement_type", "settlement_date",
                         name="uix_account_month_type_date"),
        Index("ix_settle_account_month", "account_id", "year_month"),
        Index("ix_settle_month", "year_month"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts.id"), nullable=False)
    year_month = Column(String(7), nullable=False)  # YYYY-MM

    # 정산 유형/상태
    settlement_type = Column(String(20))     # MONTHLY/WEEKLY/ADDITIONAL/RESERVE
    settlement_date = Column(String(10))     # 정산(예정)일
    settlement_status = Column(String(20))   # DONE/SUBJECT

    # 매출인식 기간
    revenue_date_from = Column(String(10))
    revenue_date_to = Column(String(10))

    # 금액 정보
    total_sale = Column(Integer, default=0)                  # 총판매액
    service_fee = Column(Integer, default=0)                 # 판매수수료
    settlement_target_amount = Column(Integer, default=0)    # 정산대상액
    settlement_amount = Column(Integer, default=0)           # 지급액
    last_amount = Column(Integer, default=0)                 # 유보금
    pending_released_amount = Column(Integer, default=0)     # 보류해제금
    seller_discount_coupon = Column(Integer, default=0)      # 판매자할인쿠폰
    downloadable_coupon = Column(Integer, default=0)         # 다운로드쿠폰
    seller_service_fee = Column(Integer, default=0)          # 판매자서비스수수료
    courantee_fee = Column(Integer, default=0)               # 보증수수료
    deduction_amount = Column(Integer, default=0)            # 차감금액
    debt_of_last_week = Column(Integer, default=0)           # 전주차이월금
    final_amount = Column(Integer, default=0)                # 최종지급액

    # 계좌 정보
    bank_name = Column(String(50))
    bank_account = Column(String(50))

    # 원본 JSON
    raw_json = Column(Text)

    created_at = Column(DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f"<SettlementHistory(month={self.year_month}, type={self.settlement_type}, status={self.settlement_status})>"
