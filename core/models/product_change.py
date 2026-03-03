"""API 상품 수정 이력 모델"""
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Text, Index
from core.database import Base


class ProductChange(Base):
    """API 상품 수정 이력 (등록/수정/삭제/상태변경)"""

    __tablename__ = "product_changes"
    __table_args__ = (
        Index("ix_changes_account", "account_id"),
        Index("ix_changes_spid", "seller_product_id"),
        Index("ix_changes_changed", "changed_at"),
    )

    id = Column(Integer, primary_key=True)
    account_id = Column(Integer, ForeignKey("accounts.id"), nullable=False)
    seller_product_id = Column(String(50), nullable=False)          # SPID
    action = Column(String(50), nullable=False)                     # 등록/수정/삭제/상태변경
    field = Column(String(100), default='')                         # 수정 필드명
    before_value = Column(Text, default='')
    after_value = Column(Text, default='')
    result_code = Column(String(50), default='')                    # API 응답 코드
    changed_at = Column(DateTime, nullable=False)

    def __repr__(self):
        return f"<ProductChange(spid='{self.seller_product_id}', action='{self.action}')>"
