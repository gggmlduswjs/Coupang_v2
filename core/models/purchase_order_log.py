"""발주서 다운로드 이력 — 발주 스냅샷 저장 + 이력 추적"""
from sqlalchemy import Column, Integer, BigInteger, String, DateTime, Index
from datetime import datetime
from core.database import Base


class PurchaseOrderLog(Base):
    """발주서 다운로드 시 주문 스냅샷 기록"""

    __tablename__ = "purchase_order_logs"
    __table_args__ = (
        Index("ix_polog_batch", "batch_id"),
        Index("ix_polog_ordered", "ordered_at"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    batch_id = Column(String(36), nullable=False)
    shipment_box_id = Column(BigInteger, nullable=False)
    order_id = Column(BigInteger, nullable=True)
    account_id = Column(Integer, nullable=False)
    vendor_item_id = Column(BigInteger, nullable=True)
    book_title = Column(String(500), nullable=True)
    isbn = Column(String(50), nullable=True)
    publisher = Column(String(100), nullable=True)
    distributor = Column(String(50), nullable=True)
    quantity = Column(Integer, default=1)
    ordered_at = Column(DateTime, default=datetime.utcnow)
