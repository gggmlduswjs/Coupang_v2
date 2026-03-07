"""통합 설정 — Coupong Settings + 쿠팡데이터분석 CoupangConfig 병합"""
import os
from pathlib import Path
from typing import Optional
from dataclasses import dataclass, field

from pydantic_settings import BaseSettings

BASE_DIR = Path(__file__).resolve().parent.parent


class Settings(BaseSettings):
    """환경변수 기반 설정 (Pydantic)

    DB 연결만 사용. WING API/알라딘 등은 core/accounts.py, os.getenv, Streamlit secrets 참조.
    """

    database_url: str = f"sqlite:///{BASE_DIR / 'data' / 'coupang.db'}"
    supabase_database_url: Optional[str] = None

    class Config:
        env_file = ".env"
        case_sensitive = False
        extra = "ignore"


# 전역 설정 인스턴스
settings = Settings()


@dataclass
class AnalysisConfig:
    """분석 전용 설정 (Playwright 수집/HTML 파싱용)"""
    base_dir: str = str(BASE_DIR)
    db_path: str = str(BASE_DIR / "data" / "coupang.db")
    html_cache_dir: str = str(BASE_DIR / "data" / "html_cache")
    reports_dir: str = str(BASE_DIR / "reports")
    backup_dir: str = str(BASE_DIR / "data" / "backups")
    delay_min: float = 3.0
    delay_max: float = 7.0
    headless: bool = False
    max_pages: int = 3
    chrome_debug_port: int = 9222

    user_agents: list[str] = field(default_factory=lambda: [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
    ])

    top_n: int = 10
    enrich_top_n: int = 5
    price_bins: int = 10
    review_rate: float = 0.03

    catalog_name_weight: float = 60.0
    catalog_price_weight: float = 25.0
    catalog_category_weight: float = 10.0
    catalog_review_weight: float = 5.0

    def ensure_dirs(self):
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        os.makedirs(self.html_cache_dir, exist_ok=True)
        os.makedirs(self.reports_dir, exist_ok=True)
        os.makedirs(self.backup_dir, exist_ok=True)
