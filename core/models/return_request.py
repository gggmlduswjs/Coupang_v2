"""반품/취소 요청 모델 — WING Return Request API 데이터"""
from sqlalchemy import Column, Integer, BigInteger, String, Boolean, ForeignKey, DateTime, Text, UniqueConstraint, Index
from sqlalchemy.orm import relationship
from datetime import datetime
from core.database import Base


class ReturnRequest(Base):
    """쿠팡 반품/취소 접수 단위 데이터"""

    __tablename__ = "return_requests"
    __table_args__ = (
        UniqueConstraint("account_id", "receipt_id", name="uix_return_account_receipt"),
        Index("ix_return_account_created", "account_id", "created_at_api"),
        Index("ix_return_account_status", "account_id", "receipt_status"),
        Index("ix_return_order_id", "order_id"),
        Index("ix_return_listing_id", "listing_id"),
        Index("ix_return_receipt_type", "receipt_type"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts.id"), nullable=False)

    # 접수 식별
    receipt_id = Column(BigInteger, nullable=False)   # 접수번호
    order_id = Column(BigInteger)                     # 주문번호
    payment_id = Column(BigInteger)                   # 결제번호

    # 유형/상태
    receipt_type = Column(String(50))    # RETURN / CANCEL
    receipt_status = Column(String(40))  # RELEASE_STOP_UNCHECKED / RETURNS_UNCHECKED / VENDOR_WAREHOUSE_CONFIRM / REQUEST_COUPANG_CHECK / RETURNS_COMPLETED

    # API 일시
    created_at_api = Column(DateTime)    # 접수시간
    modified_at_api = Column(DateTime)   # 상태변경시간

    # 요청자 정보
    requester_name = Column(String(100))
    requester_phone = Column(String(50))
    requester_address = Column(String(500))
    requester_address_detail = Column(String(200))
    requester_zip_code = Column(String(10))

    # 사유
    cancel_reason_category1 = Column(String(100))
    cancel_reason_category2 = Column(String(100))
    cancel_reason = Column(Text)

    # 수량
    cancel_count_sum = Column(Integer)

    # 배송
    return_delivery_id = Column(BigInteger)
    return_delivery_type = Column(String(50))
    release_stop_status = Column(String(30))

    # 귀책
    fault_by_type = Column(String(50))   # COUPANG / VENDOR / CUSTOMER / WMS / GENERAL

    # 환불
    pre_refund = Column(Boolean)
    complete_confirm_type = Column(String(30))
    complete_confirm_date = Column(DateTime)

    # 사유 코드
    reason_code = Column(String(50))
    reason_code_text = Column(String(200))

    # 금액
    return_shipping_charge = Column(Integer)  # 반품배송비 units (양수=셀러부담)
    enclose_price = Column(Integer)           # 동봉배송비 units

    # JSON 상세 데이터
    return_items_json = Column(Text)      # returnItems 배열 JSON
    return_delivery_json = Column(Text)   # returnDeliveryDtos 배열 JSON
    raw_json = Column(Text)               # 전체 원본 JSON

    # 내부 매칭
    listing_id = Column(Integer, ForeignKey("listings.id"))

    # Relationships
    account = relationship("Account", backref="return_requests")
    listing = relationship("Listing", back_populates="return_requests")

    # 타임스탬프
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return f"<ReturnRequest(receipt={self.receipt_id}, type={self.receipt_type}, status={self.receipt_status})>"
