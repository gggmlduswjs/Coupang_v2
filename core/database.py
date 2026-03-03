"""데이터베이스 연결 및 세션 관리 (PostgreSQL / SQLite)"""
import os
import logging
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

_logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent


# ─── URL 결정 ───

def _resolve_database_url() -> str:
    """DATABASE_URL 결정: 환경변수 → Streamlit secrets → 기본 SQLite"""

    # 1) 환경변수
    url = os.environ.get("DATABASE_URL")
    if url:
        return url

    # 2) Streamlit secrets (Streamlit Cloud 배포 시)
    try:
        import streamlit as st
        if hasattr(st, "secrets"):
            if "supabase" in st.secrets:
                url = st.secrets["supabase"]["database_url"]
                if url:
                    return url
            if "DATABASE_URL" in st.secrets:
                url = st.secrets["DATABASE_URL"]
                if url:
                    return url
    except Exception:
        pass

    # 3) core/config.py 설정
    try:
        from core.config import settings
        if settings.supabase_database_url:
            return settings.supabase_database_url
        return settings.database_url
    except Exception:
        pass

    # 4) 기본 SQLite
    default_db = ROOT / "data" / "coupang.db"
    return f"sqlite:///{default_db}"


def _is_postgresql(url: str) -> bool:
    return url.startswith(("postgresql://", "postgres://"))


def _create_engine_for_url(url: str):
    """URL 기반 엔진 생성"""
    if _is_postgresql(url):
        _logger.info("PostgreSQL 엔진으로 연결합니다.")
        return create_engine(
            url,
            echo=False,
            pool_pre_ping=True,
            pool_size=10,
            max_overflow=20,
            pool_recycle=1800,
            pool_timeout=10,
            connect_args={
                "connect_timeout": 5,
                "options": "-c statement_timeout=30000",
            },
        )
    else:
        _logger.info(f"SQLite 엔진으로 연결합니다: {url}")
        return create_engine(url, echo=False)


def get_engine_for_db(db_path: str = None):
    """스크립트용 엔진 헬퍼"""
    if db_path is None:
        return _create_engine_for_url(_resolve_database_url())
    if db_path.startswith(("postgresql://", "postgres://", "sqlite:///")):
        return _create_engine_for_url(db_path)
    return _create_engine_for_url(f"sqlite:///{db_path}")


# ─── 모듈 레벨 전역 엔진 ───

_database_url = _resolve_database_url()
engine = _create_engine_for_url(_database_url)

# 세션 팩토리
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# 베이스 클래스
Base = declarative_base()


def get_db():
    """데이터베이스 세션 의존성"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    """데이터베이스 초기화 (테이블 생성)"""
    Base.metadata.create_all(bind=engine)


# ─── CoupangDB 호환 클래스 (레거시 코드 지원) ───

class CoupangDB:
    """구 CoupangDB 인터페이스 호환 래퍼.

    마이그레이션 이전 코드(analysis/collector.py, operations/*.py 등)가
    CoupangDB 메서드를 그대로 사용할 수 있도록 SQLAlchemy ORM을 래핑.
    """

    def __init__(self, config=None):
        self._session = SessionLocal()
        self._config = config

    # ── 공통 ──────────────────────────────────────────
    @property
    def conn(self):
        """raw 연결 반환 (pandas.read_sql 호환)"""
        return self._session.bind

    def close(self):
        self._session.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    # ── 키워드 / 스냅샷 / 검색결과 ─────────────────────
    def get_or_create_keyword(self, keyword: str):
        from core.models.keyword import Keyword
        obj = self._session.query(Keyword).filter(Keyword.keyword == keyword).first()
        if not obj:
            obj = Keyword(keyword=keyword)
            self._session.add(obj)
            self._session.commit()
            self._session.refresh(obj)
        return obj

    def list_keywords(self):
        from core.models.keyword import Keyword
        return self._session.query(Keyword).order_by(Keyword.keyword).all()

    def create_snapshot(self, keyword_id: int, source: str, page_count: int = 1):
        from core.models.keyword import Snapshot
        snap = Snapshot(keyword_id=keyword_id, source=source, page_count=page_count)
        self._session.add(snap)
        self._session.commit()
        self._session.refresh(snap)
        return snap

    def update_snapshot_count(self, snapshot_id: int, total: int):
        from core.models.keyword import Snapshot
        snap = self._session.query(Snapshot).get(snapshot_id)
        if snap:
            snap.total_products = total
            self._session.commit()

    def get_snapshots(self, keyword_id: int):
        from core.models.keyword import Snapshot
        return (self._session.query(Snapshot)
                .filter(Snapshot.keyword_id == keyword_id)
                .order_by(Snapshot.collected_at.desc())
                .all())

    def get_snapshot_count(self, keyword: str) -> int:
        from core.models.keyword import Keyword, Snapshot
        kw = self._session.query(Keyword).filter(Keyword.keyword == keyword).first()
        if not kw:
            return 0
        return self._session.query(Snapshot).filter(Snapshot.keyword_id == kw.id).count()

    def insert_products(self, products: list):
        """Product(dataclass) 리스트 → SearchResult ORM 객체로 저장"""
        from core.models.keyword import SearchResult
        objs = []
        for p in products:
            kwargs = {}
            for field in (
                "snapshot_id", "keyword_id", "exposure_order", "vendor_item_id",
                "ad_type", "organic_rank", "product_name", "original_price",
                "discount_rate", "sale_price", "rating", "review_count",
                "url", "product_id_coupang", "item_id", "source_type",
                "delivery_type", "arrival_date", "free_shipping", "cashback",
                "keyword_in_name", "keyword_position", "category", "image_count", "sku",
            ):
                val = getattr(p, field, None)
                if val is not None:
                    kwargs[field] = val
            objs.append(SearchResult(**kwargs))
        self._session.bulk_save_objects(objs)
        self._session.commit()

    def update_product_enrichment(self, product_id: int, category: str, image_count: int, sku: str):
        from core.models.keyword import SearchResult
        sr = self._session.query(SearchResult).get(product_id)
        if sr:
            sr.category = category
            sr.image_count = image_count
            sr.sku = sku
            self._session.commit()

    def get_analysis_dataframe(self, keyword: str, snapshot_id=None):
        import pandas as pd
        from sqlalchemy import text
        if snapshot_id:
            sql = text("""
                SELECT sr.*, s.collected_at, s.source
                FROM search_results sr
                JOIN snapshots s ON sr.snapshot_id = s.id
                JOIN keywords k ON sr.keyword_id = k.id
                WHERE k.keyword = :kw AND sr.snapshot_id = :sid
                ORDER BY sr.exposure_order
            """)
            params = {"kw": keyword, "sid": snapshot_id}
        else:
            sql = text("""
                SELECT sr.*, s.collected_at, s.source
                FROM search_results sr
                JOIN snapshots s ON sr.snapshot_id = s.id
                JOIN keywords k ON sr.keyword_id = k.id
                WHERE k.keyword = :kw
                ORDER BY s.collected_at DESC, sr.exposure_order
            """)
            params = {"kw": keyword}
        with self._session.bind.connect() as conn:
            return pd.read_sql(sql, conn, params=params)

    def get_product_history(self, product_id_str: str):
        import pandas as pd
        from sqlalchemy import text
        sql = text("""
            SELECT sr.*, s.collected_at FROM search_results sr
            JOIN snapshots s ON sr.snapshot_id = s.id
            WHERE sr.vendor_item_id = :pid OR sr.product_id_coupang = :pid
            ORDER BY s.collected_at
        """)
        with self._session.bind.connect() as conn:
            return pd.read_sql(sql, conn, params={"pid": product_id_str})

    # ── 계정 ────────────────────────────────────────────
    def list_accounts(self):
        from core.models.account import Account
        return self._session.query(Account).order_by(Account.account_code).all()

    def get_account_by_code(self, code: str):
        from core.models.account import Account
        return self._session.query(Account).filter(Account.account_code == code).first()

    def add_account(self, account):
        self._session.add(account)
        self._session.commit()
        return account

    def update_account_status(self, account_id: int, status: str):
        from core.models.account import Account
        acc = self._session.query(Account).get(account_id)
        if acc:
            acc.status = status
            self._session.commit()

    # ── 재고 ────────────────────────────────────────────
    def upsert_inventory_product(self, product):
        from core.models.inventory import InventoryProduct
        existing = (self._session.query(InventoryProduct)
                    .filter(InventoryProduct.account_id == product.account_id,
                            InventoryProduct.seller_product_id == product.seller_product_id)
                    .first())
        if existing:
            for col in ("product_name", "sale_price", "original_price", "status",
                        "category", "brand", "barcode", "stock_qty", "wing_product_id", "memo"):
                v = getattr(product, col, None)
                if v is not None:
                    setattr(existing, col, v)
            self._session.commit()
            return existing.id, False
        else:
            self._session.add(product)
            self._session.commit()
            return product.id, True

    def list_inventory(self, account_id: int, status: str = None,
                       limit: int = 100, offset: int = 0):
        from core.models.inventory import InventoryProduct
        q = self._session.query(InventoryProduct).filter(
            InventoryProduct.account_id == account_id)
        if status:
            q = q.filter(InventoryProduct.status == status)
        return q.offset(offset).limit(limit).all()

    def search_inventory(self, query_str: str, account_id: int = None):
        from core.models.inventory import InventoryProduct
        q = self._session.query(InventoryProduct).filter(
            InventoryProduct.product_name.like(f"%{query_str}%"))
        if account_id:
            q = q.filter(InventoryProduct.account_id == account_id)
        return q.limit(200).all()

    def count_inventory_by_status(self, account_id: int) -> dict:
        from sqlalchemy import func
        from core.models.inventory import InventoryProduct
        rows = (self._session.query(InventoryProduct.status,
                                    func.count(InventoryProduct.id))
                .filter(InventoryProduct.account_id == account_id)
                .group_by(InventoryProduct.status)
                .all())
        return dict(rows)

    def get_inventory_total(self, account_id: int) -> int:
        from core.models.inventory import InventoryProduct
        return (self._session.query(InventoryProduct)
                .filter(InventoryProduct.account_id == account_id)
                .count())

    def create_inventory_snapshot(self, account_id: int, source_file: str,
                                  total: int, new: int, updated: int):
        """구 스키마 호환용 — 신규 스키마에 snapshot 테이블 없어 로그만 출력"""
        _logger.info(f"[재고 스냅샷] 계정={account_id}, 파일={source_file}, "
                     f"전체={total}, 신규={new}, 갱신={updated}")

    # ── 노출 로그 ────────────────────────────────────────
    def insert_exposure_log(self, log):
        self._session.add(log)
        self._session.commit()

    def get_exposure_logs_by_account(self, account_id: int, limit: int = 100):
        from core.models.exposure import ExposureLog
        return (self._session.query(ExposureLog)
                .filter(ExposureLog.account_id == account_id)
                .order_by(ExposureLog.checked_at.desc())
                .limit(limit).all())

    def get_exposure_summary(self, account_id: int) -> dict:
        from sqlalchemy import func
        from core.models.exposure import ExposureLog
        total = (self._session.query(func.count(ExposureLog.id))
                 .filter(ExposureLog.account_id == account_id).scalar() or 0)
        found = (self._session.query(func.count(ExposureLog.id))
                 .filter(ExposureLog.account_id == account_id,
                         ExposureLog.found.is_(True)).scalar() or 0)
        return {"total": total, "total_checks": total, "found": found,
                "not_found": total - found,
                "exposure_rate": round(found / total * 100, 1) if total else 0}

    # ── 카탈로그 매칭 ────────────────────────────────────
    def insert_catalog_match(self, match) -> int:
        self._session.add(match)
        self._session.commit()
        return match.id

    def get_matched_inventory_ids(self, account_id: int) -> set:
        from core.models.catalog import CatalogMatch
        rows = (self._session.query(CatalogMatch.listing_id)
                .filter(CatalogMatch.account_id == account_id,
                        CatalogMatch.status == "승인")
                .all())
        return {r[0] for r in rows}

    def get_best_matches(self, account_id: int, min_score: float = 0) -> list:
        from core.models.catalog import CatalogMatch
        rows = (self._session.query(CatalogMatch)
                .filter(CatalogMatch.account_id == account_id,
                        CatalogMatch.total_score >= min_score)
                .order_by(CatalogMatch.total_score.desc())
                .all())
        return [{c.name: getattr(r, c.name) for c in CatalogMatch.__table__.columns}
                for r in rows]

    def get_approved_matches(self, account_id: int) -> list:
        from core.models.catalog import CatalogMatch
        rows = (self._session.query(CatalogMatch)
                .filter(CatalogMatch.account_id == account_id,
                        CatalogMatch.status == "승인")
                .all())
        return [{c.name: getattr(r, c.name) for c in CatalogMatch.__table__.columns}
                for r in rows]

    def update_catalog_match_status(self, match_id: int = None, status: str = "승인",
                                    inventory_product_id: int = None, account_id: int = None):
        from core.models.catalog import CatalogMatch
        if match_id:
            m = self._session.query(CatalogMatch).get(match_id)
            if m:
                m.status = status
        elif inventory_product_id and account_id:
            (self._session.query(CatalogMatch)
             .filter(CatalogMatch.listing_id == inventory_product_id,
                     CatalogMatch.account_id == account_id)
             .update({"status": status}))
        self._session.commit()

    def get_catalog_match_summary(self, account_id: int) -> dict:
        from sqlalchemy import func
        from core.models.catalog import CatalogMatch
        total = (self._session.query(func.count(CatalogMatch.id))
                 .filter(CatalogMatch.account_id == account_id).scalar() or 0)
        approved = (self._session.query(func.count(CatalogMatch.id))
                    .filter(CatalogMatch.account_id == account_id,
                            CatalogMatch.status == "승인").scalar() or 0)
        return {"total": total, "approved": approved, "pending": total - approved}

    # ── 상품 변경 이력 ────────────────────────────────────
    def log_product_change(self, account_code: str, seller_product_id: str,
                           action: str, field: str = "", before: str = "",
                           after: str = "", result_code: str = ""):
        from datetime import datetime
        from core.models.account import Account
        from core.models.product_change import ProductChange
        acc = self._session.query(Account).filter(Account.account_code == account_code).first()
        if not acc:
            return
        chg = ProductChange(
            account_id=acc.id,
            seller_product_id=seller_product_id,
            action=action,
            field=field,
            before_value=before,
            after_value=after,
            result_code=result_code,
            changed_at=datetime.utcnow(),
        )
        self._session.add(chg)
        self._session.commit()

    def get_product_changes(self, account_code: str = "", seller_product_id: str = "",
                            action: str = "", limit: int = 100) -> list:
        from core.models.account import Account
        from core.models.product_change import ProductChange
        q = self._session.query(ProductChange)
        if account_code:
            acc = self._session.query(Account).filter(Account.account_code == account_code).first()
            if acc:
                q = q.filter(ProductChange.account_id == acc.id)
        if seller_product_id:
            q = q.filter(ProductChange.seller_product_id == seller_product_id)
        if action:
            q = q.filter(ProductChange.action == action)
        return q.order_by(ProductChange.changed_at.desc()).limit(limit).all()
