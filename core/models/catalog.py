"""카탈로그 매칭 모델 — listing_id 기준 (구 inventory_product_id)"""
from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, Index
from sqlalchemy.orm import relationship
from datetime import datetime
from core.database import Base


class CatalogMatch(Base):
    """카탈로그 매칭 후보 및 결과"""

    __tablename__ = "catalog_matches"
    __table_args__ = (
        Index("ix_catalog_listing", "listing_id"),
        Index("ix_catalog_account", "account_id"),
        Index("ix_catalog_score", "total_score"),
        Index("ix_catalog_status", "status"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    listing_id = Column(Integer, ForeignKey("listings.id"), nullable=False)
    account_id = Column(Integer, ForeignKey("accounts.id"), nullable=False)

    # 후보 상품 정보
    candidate_product_id = Column(String(50), default='')
    candidate_vendor_item_id = Column(String(50), default='')
    candidate_name = Column(String(500), default='')
    candidate_price = Column(Integer)
    candidate_review_count = Column(Integer)
    candidate_rating = Column(Float)
    candidate_url = Column(String(500), default='')
    candidate_category = Column(String(200), default='')

    # 점수
    name_score = Column(Float, default=0.0)
    price_score = Column(Float, default=0.0)
    category_score = Column(Float, default=0.0)
    review_bonus = Column(Float, default=0.0)
    total_score = Column(Float, default=0.0)

    # 매칭 결과
    confidence = Column(String(20), default="낮음")
    rank = Column(Integer, default=0)
    status = Column(String(20), default="대기")  # 대기/승인/거부

    matched_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    listing = relationship("Listing", back_populates="catalog_matches")

    def __repr__(self):
        return f"<CatalogMatch(listing={self.listing_id}, score={self.total_score}, status='{self.status}')>"
