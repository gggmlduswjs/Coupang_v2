"""삭제된 상품 아카이브 모델 — listings에서 이동된 쿠팡 삭제 상품"""
from sqlalchemy import Column, Integer, BigInteger, String, DateTime, Text
from datetime import datetime
from core.database import Base


class DeletedListing(Base):
    """
    쿠팡에서 삭제된 상품 아카이브

    - listings 테이블과 동일 구조 (FK 제외)
    - 삭제 확인 시각 + 삭제 사유 추가
    """

    __tablename__ = "deleted_listings"

    id = Column(Integer, primary_key=True)
    account_id = Column(Integer, nullable=False, index=True)

    # --- 쿠팡 API 필드 (listing에서 복사) ---
    coupang_product_id = Column(BigInteger, nullable=False)
    vendor_item_id     = Column(BigInteger)
    product_name       = Column(String(500))
    coupang_status     = Column(String(20))
    original_price     = Column(Integer, default=0)
    sale_price         = Column(Integer, default=0)
    supply_price       = Column(Integer)
    stock_quantity     = Column(Integer, default=0)
    display_category_code = Column(String(20))
    delivery_charge_type  = Column(String(20))
    delivery_charge       = Column(Integer)
    free_ship_over_amount = Column(Integer)
    return_charge         = Column(Integer)
    brand                 = Column(String(200))

    # --- ISBN ---
    isbn = Column(Text)

    # --- 내부 매칭 ID (참조용, FK 없음) ---
    product_id = Column(Integer)
    bundle_id  = Column(Integer)

    # --- 원본 메타 ---
    raw_json         = Column(Text)
    detail_synced_at = Column(DateTime)
    synced_at        = Column(DateTime)
    original_created_at = Column(DateTime)  # listing 원본 created_at

    # --- 삭제 메타 ---
    deleted_reason = Column(String(200), default="쿠팡 삭제 확인")
    deleted_at     = Column(DateTime, default=datetime.utcnow)  # 아카이브 시각

    def __repr__(self):
        return f"<DeletedListing(account={self.account_id}, pid={self.coupang_product_id})>"
