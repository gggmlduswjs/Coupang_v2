"""계정 모델"""
from sqlalchemy import Column, Integer, String, Boolean, DateTime, Text
from sqlalchemy.orm import relationship
from datetime import datetime
from core.database import Base


class Account(Base):
    """통합 계정 (Coupong accounts + 쿠팡데이터분석 seller_accounts)"""

    __tablename__ = "accounts"

    id = Column(Integer, primary_key=True, index=True)
    account_code = Column(String(20), unique=True, nullable=False)    # 007-ez
    account_name = Column(String(50), nullable=False, index=True)     # 007-EZ
    email = Column(String(100), default='')
    is_active = Column(Boolean, default=True)
    status = Column(String(20), default='활성')                        # 활성/비활성/복구중/확인필요
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # WING API 필드
    vendor_id = Column(String(20), nullable=True, index=True)
    wing_access_key = Column(String(100), nullable=True)
    wing_secret_key = Column(String(100), nullable=True)
    wing_api_enabled = Column(Boolean, default=False)
    outbound_shipping_code = Column(String(50), nullable=True)
    return_center_code = Column(String(50), nullable=True)

    # 쿠팡데이터분석 seller_accounts 통합
    memo = Column(Text, default='')

    # Relationships
    listings = relationship("Listing", back_populates="account")

    def __repr__(self):
        return f"<Account(code='{self.account_code}', name='{self.account_name}')>"

    @property
    def has_wing_api(self) -> bool:
        """WING API 사용 가능 여부"""
        return bool(self.vendor_id and self.wing_access_key and self.wing_secret_key and self.wing_api_enabled)
