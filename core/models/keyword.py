"""검색 키워드/스냅샷/검색결과 모델 (dataclass에서 SQLAlchemy 모델로 변환)"""
from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, ForeignKey, Index
from datetime import datetime
from core.database import Base


class Keyword(Base):
    """검색 키워드"""

    __tablename__ = "keywords"

    id = Column(Integer, primary_key=True, autoincrement=True)
    keyword = Column(String(200), unique=True, nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships (lazy import via string)
    snapshots = None  # relationship defined after Snapshot

    def __repr__(self):
        return f"<Keyword(keyword='{self.keyword}')>"


class Snapshot(Base):
    """검색 결과 스냅샷 (수집 단위)"""

    __tablename__ = "snapshots"
    __table_args__ = (
        Index("ix_snapshot_keyword", "keyword_id"),
        Index("ix_snapshot_collected_at", "collected_at"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    keyword_id = Column(Integer, ForeignKey("keywords.id"), nullable=False)
    collected_at = Column(DateTime, default=datetime.utcnow)
    source = Column(String(50))  # playwright / html_import
    page_count = Column(Integer, default=0)
    total_products = Column(Integer, default=0)

    def __repr__(self):
        return f"<Snapshot(keyword={self.keyword_id}, source='{self.source}', products={self.total_products})>"


class SearchResult(Base):
    """검색 결과 개별 상품 (Product에서 이름 변경 — 충돌 방지)"""

    __tablename__ = "search_results"
    __table_args__ = (
        Index("ix_searchresult_snapshot", "snapshot_id"),
        Index("ix_searchresult_keyword", "keyword_id"),
        Index("ix_searchresult_vendor_item", "vendor_item_id"),
        Index("ix_searchresult_product_id_coupang", "product_id_coupang"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    snapshot_id = Column(Integer, ForeignKey("snapshots.id"), nullable=False)
    keyword_id = Column(Integer, ForeignKey("keywords.id"), nullable=False)

    # 노출 순서
    exposure_order = Column(Integer)
    vendor_item_id = Column(String(50))
    ad_type = Column(String(20))  # ad / organic
    organic_rank = Column(Integer)

    # 상품 정보
    product_name = Column(String(500))
    original_price = Column(Integer, default=0)
    discount_rate = Column(Float, default=0.0)
    sale_price = Column(Integer, default=0)
    rating = Column(Float, default=0.0)
    review_count = Column(Integer, default=0)
    url = Column(String(500))
    product_id_coupang = Column(String(50))  # 쿠팡 product ID
    item_id = Column(String(50))

    # 배송/출처
    source_type = Column(String(50))  # rocket / marketplace 등
    delivery_type = Column(String(50))
    arrival_date = Column(String(50))
    free_shipping = Column(Boolean, default=False)
    cashback = Column(String(50))

    # 키워드 분석
    keyword_in_name = Column(Boolean, default=False)
    keyword_position = Column(Integer)

    # 카테고리/이미지
    category = Column(String(200))
    image_count = Column(Integer, default=0)
    sku = Column(String(100))

    def __repr__(self):
        return f"<SearchResult(order={self.exposure_order}, name='{self.product_name[:30] if self.product_name else ''}')>"
