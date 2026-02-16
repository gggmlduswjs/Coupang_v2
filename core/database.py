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
