"""배송리스트 다운로드 이력 — 중복 송장 발급 방지"""
from sqlalchemy import Column, Integer, BigInteger, DateTime, Index
from datetime import datetime
from core.database import Base


class DeliveryListLog(Base):
    """배송리스트 다운로드 시 묶음배송번호 기록"""

    __tablename__ = "delivery_list_logs"
    __table_args__ = (
        Index("ix_dllog_shipment", "shipment_box_id"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    shipment_box_id = Column(BigInteger, nullable=False)
    account_id = Column(Integer, nullable=False)
    downloaded_at = Column(DateTime, default=datetime.utcnow)
