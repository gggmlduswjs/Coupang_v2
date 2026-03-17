"""배송리스트 다운로드 이력 — 중복 송장 발급 방지 + 송장 매칭용 데이터"""
from sqlalchemy import Column, Integer, BigInteger, String, DateTime, Boolean, Index
from datetime import datetime
from core.database import Base


class DeliveryListLog(Base):
    """배송리스트 다운로드 시 묶음배송번호 + 매칭 데이터 기록"""

    __tablename__ = "delivery_list_logs"
    __table_args__ = (
        Index("ix_dllog_shipment", "shipment_box_id", unique=True),
        Index("ix_dllog_receiver", "receiver_name"),
        Index("ix_dllog_batch", "batch_id"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    shipment_box_id = Column(BigInteger, nullable=False)
    account_id = Column(Integer, nullable=False)
    order_id = Column(BigInteger, nullable=True)
    vendor_item_id = Column(BigInteger, nullable=True)
    receiver_name = Column(String(100), nullable=True)
    receiver_addr = Column(String(500), nullable=True)  # 수취인 주소 (동명이인 구분용)
    buyer_name = Column(String(100), nullable=True)
    seq_no = Column(Integer, nullable=True)  # 배송리스트 순번
    batch_id = Column(String(36), nullable=True)  # 같은 다운로드 이벤트 묶음
    downloaded_at = Column(DateTime, default=datetime.utcnow)
    registered = Column(Boolean, default=False, nullable=False)  # 송장 등록 완료 여부
