"""SQLAlchemy 모델"""
from core.models.account import Account
from core.models.publisher import Publisher
from core.models.book import Book
from core.models.product import Product
from core.models.bundle import BundleSKU, BundleItem
from core.models.listing import Listing
from core.models.deleted_listing import DeletedListing
from core.models.master_catalog import MasterCatalog, SyncLog
from core.models.product_change import ProductChange
from core.models.analysis_result import AnalysisResult
from core.models.revenue import RevenueHistory, SettlementHistory
from core.models.ad import AdSpend, AdPerformance
from core.models.order import Order
from core.models.return_request import ReturnRequest
from core.models.keyword import Keyword, Snapshot, SearchResult
from core.models.exposure import ExposureLog
from core.models.catalog import CatalogMatch

# DEPRECATED: listings로 통합됨 — 하위 호환용
from core.models.inventory import InventoryProduct

__all__ = [
    "Account", "Publisher", "Book", "Product",
    "BundleSKU", "BundleItem", "Listing", "DeletedListing",
    "MasterCatalog", "SyncLog", "ProductChange",
    "AnalysisResult", "RevenueHistory", "SettlementHistory",
    "AdSpend", "AdPerformance", "Order", "ReturnRequest",
    "Keyword", "Snapshot", "SearchResult",
    "ExposureLog", "CatalogMatch",
    "InventoryProduct",  # deprecated
]
