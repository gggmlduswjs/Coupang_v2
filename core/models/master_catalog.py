"""마스터 카탈로그 모델 — 쿠팡 노출상품ID 기준 크로스-계정 마스터"""
from sqlalchemy import Column, Integer, String, DateTime, Text, Index, ForeignKey
from sqlalchemy import JSON as JSONB  # SQLite 호환 (PostgreSQL에선 JSONB 사용)
from sqlalchemy.orm import relationship
from datetime import datetime
from core.database import Base


class MasterCatalog(Base):
    """
    쿠팡 노출상품ID 기준 크로스-계정 마스터 카탈로그

    - 쿠팡데이터분석 master_products에서 이관
    - books 테이블과 ISBN으로 soft link
    - listings.master_catalog_id FK 대상
    """

    __tablename__ = "master_catalog"
    __table_args__ = (
        Index("ix_master_isbn", "isbn"),
        Index("ix_master_type", "product_type"),
    )

    id = Column(Integer, primary_key=True)
    isbn = Column(String(50), default='')                           # ISBN/바코드
    canonical_name = Column(String(500), nullable=False)            # 노출상품명 정식
    coupang_catalog_id = Column(String(50), unique=True, nullable=False)  # 노출상품ID (핵심 키)
    category = Column(String(200), default='')
    publisher = Column(String(200), default='')                     # 제조사
    brand = Column(String(200), default='')
    base_price = Column(Integer)                                    # 기준가
    product_type = Column(String(20), default='단품')                # 단품/세트
    set_composition = Column(JSONB, default=list)                   # 세트 구성
    adult_only = Column(String(5), default='')                      # Y/N
    model_number = Column(String(100), default='')
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    listings = relationship("Listing", back_populates="master_catalog")
    sync_logs = relationship("SyncLog", back_populates="master_catalog")

    def __repr__(self):
        return f"<MasterCatalog(catalog_id='{self.coupang_catalog_id}', name='{self.canonical_name[:40]}')>"


class SyncLog(Base):
    """계정 간 동기화 이력 (수정/이미지복제 등)"""

    __tablename__ = "sync_log"
    __table_args__ = (
        Index("ix_sync_master", "master_catalog_id"),
    )

    id = Column(Integer, primary_key=True)
    master_catalog_id = Column(Integer, ForeignKey("master_catalog.id"), nullable=True)
    source_account_id = Column(Integer, nullable=True)
    target_account_id = Column(Integer, nullable=True)
    action = Column(String(100), nullable=False)
    before_value = Column(Text, default='')
    after_value = Column(Text, default='')
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    master_catalog = relationship("MasterCatalog", back_populates="sync_logs")

    def __repr__(self):
        return f"<SyncLog(action='{self.action}', master={self.master_catalog_id})>"
