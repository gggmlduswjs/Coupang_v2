"""발주서(주문) 모델 — WING Ordersheet API 데이터"""
from sqlalchemy import Column, Integer, BigInteger, String, Boolean, ForeignKey, DateTime, Text, UniqueConstraint, Index
from sqlalchemy.orm import relationship
from datetime import datetime
from core.database import Base


class Order(Base):
    """쿠팡 발주서(주문) 단위 데이터"""

    __tablename__ = "orders"
    __table_args__ = (
        UniqueConstraint("account_id", "shipment_box_id", "vendor_item_id", name="uix_order_shipment_item"),
        Index("ix_order_account_date", "account_id", "ordered_at"),
        Index("ix_order_account_status", "account_id", "status"),
        Index("ix_order_order_id", "order_id"),
        Index("ix_order_account_listing", "account_id", "listing_id"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts.id"), nullable=False)

    # 주문 식별
    shipment_box_id = Column(BigInteger, nullable=False)  # 묶음배송번호
    order_id = Column(BigInteger, nullable=False)          # 주문번호
    vendor_item_id = Column(BigInteger)                    # 옵션ID

    # 주문 상태
    status = Column(String(30))  # ACCEPT/INSTRUCT/DEPARTURE/DELIVERING/FINAL_DELIVERY/NONE_TRACKING

    # 일시
    ordered_at = Column(DateTime)    # 주문일시
    paid_at = Column(DateTime)       # 결제일시

    # 주문자/수취인
    orderer_name = Column(String(100))    # 주문자명
    receiver_name = Column(String(100))   # 수취인명
    receiver_addr = Column(String(500))   # 수취인주소
    receiver_post_code = Column(String(10))  # 우편번호

    # 상품 정보
    product_id = Column(BigInteger)          # 쿠팡 productId
    seller_product_id = Column(BigInteger)   # 등록상품ID
    seller_product_name = Column(String(500))  # 등록상품명
    vendor_item_name = Column(String(500))     # 노출상품명

    # 수량/금액
    shipping_count = Column(Integer, default=0)        # 구매수량
    cancel_count = Column(Integer, default=0)          # 취소수량
    hold_count_for_cancel = Column(Integer, default=0)  # 환불대기수량
    sales_price = Column(Integer, default=0)     # 개당 판매가
    order_price = Column(Integer, default=0)     # 결제금액
    discount_price = Column(Integer, default=0)  # 할인금액
    shipping_price = Column(Integer, default=0)  # 배송비

    # 배송 정보
    delivery_company_name = Column(String(50))   # 택배사
    invoice_number = Column(String(50))          # 운송장번호
    shipment_type = Column(String(50))           # THIRD_PARTY/CGF/CGF LITE

    # 완료 정보
    delivered_date = Column(DateTime)   # 배송완료일
    confirm_date = Column(DateTime)     # 구매확정일

    # 기타
    refer = Column(String(50))       # 결제위치
    canceled = Column(Boolean, default=False)  # 취소여부

    # 내부 매칭
    listing_id = Column(Integer, ForeignKey("listings.id"))

    # Relationships
    account = relationship("Account", backref="orders")
    listing = relationship("Listing", back_populates="orders")

    # 원본 데이터
    raw_json = Column(Text)

    # 타임스탬프
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return f"<Order(shipment={self.shipment_box_id}, order={self.order_id}, status={self.status})>"
