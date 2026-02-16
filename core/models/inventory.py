"""재고 상품 모델 (dataclass에서 SQLAlchemy 모델로 변환)"""
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Index
from datetime import datetime
from core.database import Base


class InventoryProduct(Base):
    """Wing Excel 임포트 기반 재고 상품"""

    __tablename__ = "inventory_products"
    __table_args__ = (
        Index("ix_inventory_account", "account_id"),
        Index("ix_inventory_seller_product_id", "seller_product_id"),
        Index("ix_inventory_barcode", "barcode"),
        Index("ix_inventory_status", "status"),
        Index("ix_inventory_wing_product_id", "wing_product_id"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts.id"), nullable=False)
    seller_product_id = Column(String(50))
    product_name = Column(String(500))
    sale_price = Column(Integer, default=0)
    original_price = Column(Integer, default=0)
    status = Column(String(20), default="판매중")
    category = Column(String(200))
    brand = Column(String(200))
    barcode = Column(String(50))
    stock_qty = Column(Integer, default=0)
    wing_product_id = Column(String(50))
    memo = Column(String(500))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return f"<InventoryProduct(account={self.account_id}, name='{self.product_name[:30] if self.product_name else ''}')>"
