"""노출 로그 모델 (dataclass에서 SQLAlchemy 모델로 변환)"""
from sqlalchemy import Column, Integer, String, Boolean, DateTime, ForeignKey, Index
from datetime import datetime
from core.database import Base


class ExposureLog(Base):
    """키워드별 상품 노출 순위 로그"""

    __tablename__ = "exposure_logs"
    __table_args__ = (
        Index("ix_exposure_account_keyword", "account_id", "keyword"),
        Index("ix_exposure_checked_at", "checked_at"),
        Index("ix_exposure_inventory_product", "inventory_product_id"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    inventory_product_id = Column(Integer, ForeignKey("inventory_products.id"), nullable=False)
    account_id = Column(Integer, ForeignKey("accounts.id"), nullable=False)
    keyword = Column(String(200), nullable=False)
    found = Column(Boolean, default=False)
    exposure_rank = Column(Integer, nullable=True)
    page = Column(Integer, nullable=True)
    matched_by = Column(String(50))
    checked_at = Column(DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f"<ExposureLog(keyword='{self.keyword}', rank={self.exposure_rank}, found={self.found})>"
