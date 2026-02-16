"""계정 모델"""
from sqlalchemy import Column, Integer, String, Boolean, DateTime
from sqlalchemy.orm import relationship
from datetime import datetime
from core.database import Base


class Account(Base):
    """쿠팡 계정 정보"""

    __tablename__ = "accounts"

    id = Column(Integer, primary_key=True, index=True)
    account_name = Column(String(50), unique=True, nullable=False, index=True)
    email = Column(String(100), nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # WING API 필드
    vendor_id = Column(String(20), nullable=True, index=True)
    wing_access_key = Column(String(100), nullable=True)
    wing_secret_key = Column(String(100), nullable=True)
    wing_api_enabled = Column(Boolean, default=False)
    outbound_shipping_code = Column(String(50), nullable=True)  # 출고지 코드
    return_center_code = Column(String(50), nullable=True)      # 반품지 코드

    # Relationships
    listings = relationship("Listing", back_populates="account")

    def __repr__(self):
        return f"<Account(name='{self.account_name}', vendor_id='{self.vendor_id}')>"

    @property
    def has_wing_api(self) -> bool:
        """WING API 사용 가능 여부"""
        return bool(self.vendor_id and self.wing_access_key and self.wing_secret_key and self.wing_api_enabled)
