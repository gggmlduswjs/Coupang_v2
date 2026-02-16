"""카탈로그 매칭 모델 (dataclass에서 SQLAlchemy 모델로 변환)"""
from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, Index
from datetime import datetime
from core.database import Base


class CatalogMatch(Base):
    """카탈로그 매칭 후보 및 결과"""

    __tablename__ = "catalog_matches"
    __table_args__ = (
        Index("ix_catalog_account", "account_id"),
        Index("ix_catalog_inventory_product", "inventory_product_id"),
        Index("ix_catalog_status", "status"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    inventory_product_id = Column(Integer, ForeignKey("inventory_products.id"), nullable=False)
    account_id = Column(Integer, ForeignKey("accounts.id"), nullable=False)

    # 후보 상품 정보
    candidate_product_id = Column(String(50))
    candidate_vendor_item_id = Column(String(50))
    candidate_name = Column(String(500))
    candidate_price = Column(Integer)
    candidate_review_count = Column(Integer)
    candidate_rating = Column(Float)
    candidate_url = Column(String(500))
    candidate_category = Column(String(200))

    # 점수
    name_score = Column(Float, default=0.0)
    price_score = Column(Float, default=0.0)
    category_score = Column(Float, default=0.0)
    review_bonus = Column(Float, default=0.0)
    total_score = Column(Float, default=0.0)

    # 매칭 결과
    confidence = Column(String(20), default="낮음")
    rank = Column(Integer)
    status = Column(String(20), default="대기")  # 대기/승인/거부

    matched_at = Column(DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f"<CatalogMatch(inventory={self.inventory_product_id}, score={self.total_score}, status='{self.status}')>"
