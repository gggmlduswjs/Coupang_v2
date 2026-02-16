"""상품 등록 현황 모델 — 쿠팡 API 미러 + 내부 매칭 FK"""
from sqlalchemy import (
    Column, Integer, BigInteger, String, ForeignKey, DateTime, Text, Index,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship
from datetime import datetime
from core.database import Base
from core.constants import LOW_STOCK_THRESHOLD


class Listing(Base):
    """
    계정별 쿠팡 상품 미러 (API -> DB 동기화)

    - 쿠팡 API 필드를 그대로 저장
    - product_id / bundle_id FK로 내부 상품과 매칭
    """

    __tablename__ = "listings"
    __table_args__ = (
        UniqueConstraint("account_id", "coupang_product_id", name="uix_account_coupang_pid"),
        Index("ix_listing_account_status", "account_id", "coupang_status"),
        Index("ix_listing_account_vendor_item", "account_id", "vendor_item_id"),
        Index("ix_listing_isbn", "isbn"),
        Index("ix_listing_product_id", "product_id"),
        Index("ix_listing_bundle_id", "bundle_id"),
        Index("ix_listing_synced_at", "synced_at"),
    )

    id = Column(Integer, primary_key=True)
    account_id = Column(Integer, ForeignKey("accounts.id"), nullable=False, index=True)

    # --- 쿠팡 API 필드 (list_products + get_product + get_item_inventory) ---
    coupang_product_id = Column(BigInteger, nullable=False)          # sellerProductId
    vendor_item_id     = Column(BigInteger)                          # items[0].vendorItemId
    product_name       = Column(String(500))                         # sellerProductName
    coupang_status     = Column(String(20), default='pending')       # active/paused/sold_out/pending
    original_price     = Column(Integer, default=0)                  # originalPrice (정가)
    sale_price         = Column(Integer, default=0)                  # salePrice (판매가)
    supply_price       = Column(Integer)                             # supplyPrice (공급가)
    stock_quantity     = Column(Integer, default=0)                  # amountInStock
    display_category_code = Column(String(20))                       # displayCategoryCode
    delivery_charge_type  = Column(String(20))                       # FREE/NOT_FREE/CONDITIONAL_FREE
    delivery_charge       = Column(Integer)                          # deliveryCharge
    free_ship_over_amount = Column(Integer)                          # freeShipOverAmount
    return_charge         = Column(Integer)                          # returnCharge
    brand                 = Column(String(200))                      # brand

    # --- ISBN (API에서 추출, 세트는 쉼표 구분 복수) ---
    isbn = Column(Text)  # 단권: "9788961057455", 세트: "9788961057455,9788961057462"

    # --- 내부 매칭 (nullable — 매칭 안 된 상품은 NULL) ---
    product_id = Column(Integer, ForeignKey("products.id"))          # 단권 매칭
    bundle_id  = Column(Integer, ForeignKey("bundle_skus.id"))       # 묶음 매칭

    # --- 동기화 메타 ---
    raw_json         = Column(Text)          # API 상세 응답 캐시
    detail_synced_at = Column(DateTime)      # 상세 조회 시각
    synced_at        = Column(DateTime)      # 마지막 동기화 시각
    created_at       = Column(DateTime, default=datetime.utcnow)
    updated_at       = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # --- Relationships ---
    account = relationship("Account", back_populates="listings")
    product = relationship("Product", back_populates="listings")
    bundle = relationship("BundleSKU", back_populates="listings")
    analysis_results = relationship("AnalysisResult", back_populates="listing")
    orders = relationship("Order", back_populates="listing")
    revenue_history = relationship("RevenueHistory", back_populates="listing")
    return_requests = relationship("ReturnRequest", back_populates="listing")
    ad_performances = relationship("AdPerformance", back_populates="listing")

    def __repr__(self):
        return f"<Listing(account={self.account_id}, pid={self.coupang_product_id}, status='{self.coupang_status}')>"

    # --- 타입 판단 프로퍼티 ---

    @property
    def is_single(self):
        """단권 상품인지"""
        return self.product_id is not None

    @property
    def is_bundle(self):
        """묶음 상품인지"""
        return self.bundle_id is not None

    # --- 상태 프로퍼티 ---

    @property
    def is_active(self):
        """활성 상태인지"""
        return self.coupang_status == 'active'

    @property
    def is_pending(self):
        """대기 중인지"""
        return self.coupang_status == 'pending'

    @property
    def has_price_diff(self):
        """내부 Product의 sale_price와 쿠팡 가격이 다른지"""
        if self.product and self.product.sale_price:
            return self.sale_price != self.product.sale_price
        return False

    @property
    def is_low_stock(self):
        """재고가 부족한지"""
        return (self.stock_quantity or 0) <= LOW_STOCK_THRESHOLD

    @property
    def can_update(self):
        """API 업데이트 가능 여부 (vendor_item_id 필수)"""
        return bool(self.vendor_item_id) and self.coupang_status == 'active'
