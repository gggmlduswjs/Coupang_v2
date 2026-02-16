"""통합 설정 — Coupong Settings + 쿠팡데이터분석 CoupangConfig 병합"""
import os
from pathlib import Path
from typing import Optional
from dataclasses import dataclass, field

from pydantic_settings import BaseSettings

BASE_DIR = Path(__file__).resolve().parent.parent


class Settings(BaseSettings):
    """환경변수 기반 설정 (Pydantic)"""

    # Database
    database_url: str = f"sqlite:///{BASE_DIR / 'data' / 'coupang.db'}"
    supabase_database_url: Optional[str] = None
    supabase_url: Optional[str] = None
    supabase_service_key: Optional[str] = None
    supabase_anon_key: Optional[str] = None

    # Redis
    redis_url: str = "redis://localhost:6379/0"

    # Security
    encryption_key: str = ""

    # Crawler (알라딘)
    crawl_delay_min: float = 1.0
    crawl_delay_max: float = 3.0
    crawl_max_items_per_session: int = 100
    crawl_timeout: int = 30

    # Playwright (쿠팡 검색 수집)
    headless: bool = False
    max_pages: int = 3
    chrome_debug_port: int = 9222
    collect_delay_min: float = 3.0
    collect_delay_max: float = 7.0

    # Upload
    upload_delay_min: float = 5.0
    upload_delay_max: float = 10.0
    upload_max_daily_per_account: int = 20
    upload_enable_playwright: bool = False

    # Analysis (쿠팡데이터분석 파라미터)
    analysis_period_days: int = 7
    analysis_exposure_low_threshold: int = 10
    analysis_conversion_low_threshold: int = 50
    top_n: int = 10
    enrich_top_n: int = 5
    price_bins: int = 10
    review_rate: float = 0.03

    # Catalog matching weights
    catalog_name_weight: float = 60.0
    catalog_price_weight: float = 25.0
    catalog_category_weight: float = 10.0
    catalog_review_weight: float = 5.0

    # Notification
    enable_kakao_notification: bool = False
    kakao_api_key: Optional[str] = None
    enable_email_notification: bool = False
    email_smtp_server: Optional[str] = None
    email_smtp_port: Optional[int] = None
    email_smtp_user: Optional[str] = None
    email_smtp_password: Optional[str] = None
    email_from: Optional[str] = None
    email_to: Optional[str] = None

    # Logging
    log_level: str = "INFO"
    log_file_max_bytes: int = 10485760
    log_backup_count: int = 5

    # Obsidian
    obsidian_vault_path: Optional[str] = None

    # Aladin API
    aladin_ttb_key: Optional[str] = None

    # Coupang Accounts (로그인)
    coupang_id_1: Optional[str] = None
    coupang_pw_1: Optional[str] = None
    coupang_id_2: Optional[str] = None
    coupang_pw_2: Optional[str] = None
    coupang_id_3: Optional[str] = None
    coupang_pw_3: Optional[str] = None
    coupang_id_4: Optional[str] = None
    coupang_pw_4: Optional[str] = None
    coupang_id_5: Optional[str] = None
    coupang_pw_5: Optional[str] = None

    # Coupang WING API (5개 계정)
    coupang_007book_vendor_id: Optional[str] = None
    coupang_007book_access_key: Optional[str] = None
    coupang_007book_secret_key: Optional[str] = None
    coupang_007bm_vendor_id: Optional[str] = None
    coupang_007bm_access_key: Optional[str] = None
    coupang_007bm_secret_key: Optional[str] = None
    coupang_007ez_vendor_id: Optional[str] = None
    coupang_007ez_access_key: Optional[str] = None
    coupang_007ez_secret_key: Optional[str] = None
    coupang_002bm_vendor_id: Optional[str] = None
    coupang_002bm_access_key: Optional[str] = None
    coupang_002bm_secret_key: Optional[str] = None
    coupang_big6ceo_vendor_id: Optional[str] = None
    coupang_big6ceo_access_key: Optional[str] = None
    coupang_big6ceo_secret_key: Optional[str] = None

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
    upload_excel_dir: str = str(BASE_DIR / "엑셀")

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
