"""
광고 성과 보고서 동기화 스크립트
================================
쿠팡 광고센터 보고서 Excel → ad_performances 테이블

지원 보고서 유형 (쿠팡 광고센터 기준):
  - 상품광고 보고서 (광고진행 옵션ID, 광고진행 상품명, 전환 지표)
  - 브랜드광고 보고서 (광고명, 소재ID, 키워드, 카테고리)
  - 디스플레이광고 보고서 (노출영역, 플랫폼)
  - 키워드 보고서 (키워드, 매치유형)
  - 캠페인 보고서 (캠페인 단위 집계)

기여 기간 처리:
  - (14일) 컬럼 우선 사용, 없으면 (1일) 컬럼 사용
  - 접미사 없는 컬럼은 그대로 매핑

사용법:
    python scripts/sync_ad_performance.py path/to/report.xlsx
    python scripts/sync_ad_performance.py --dir path/to/folder
"""
import os
import sys
import re
import argparse
import logging
from datetime import datetime, date
from pathlib import Path
from typing import List, Optional, Tuple, Dict

import pandas as pd
from sqlalchemy import text

# 프로젝트 루트
ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT))

from core.database import get_engine_for_db

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# ── 한글 컬럼 → 영문 매핑 (접미사 없는 일반 컬럼) ──
COLUMN_MAP = {
    # 날짜
    "날짜": "ad_date",
    "일자": "ad_date",
    "보고서 날짜": "ad_date",

    # 캠페인/광고그룹
    "캠페인": "campaign_name",
    "캠페인명": "campaign_name",
    "캠페인 이름": "campaign_name",
    "캠페인ID": "campaign_id",
    "캠페인 ID": "campaign_id",
    "광고그룹": "ad_group_name",
    "광고그룹명": "ad_group_name",
    "광고 그룹": "ad_group_name",
    "광고 그룹명": "ad_group_name",

    # 상품 — 상품광고 보고서
    "광고진행 옵션ID": "coupang_product_id",
    "광고진행 상품명": "product_name",
    "광고전환매출발생 옵션ID": "conversion_option_id",
    "광고전환매출발생 상품명": "conversion_product_name",
    # 상품 — 일반/호환
    "상품ID": "coupang_product_id",
    "상품 ID": "coupang_product_id",
    "상품id": "coupang_product_id",
    "상품명": "product_name",
    "상품 이름": "product_name",
    "옵션ID": "coupang_product_id",
    "옵션 ID": "coupang_product_id",

    # 광고 구분 — 상품광고 보고서
    "입찰유형": "bid_type",
    "입찰 유형": "bid_type",
    "과금 방식": "bid_type",
    "과금방식": "bid_type",
    "판매방식": "sales_method",
    "판매 방식": "sales_method",
    "광고유형": "ad_type",
    "광고 유형": "ad_type",
    "광고 노출 지면": "placement",
    "광고노출지면": "placement",

    # 브랜드광고 보고서 전용
    "광고명": "ad_name",
    "광고 이름": "ad_name",
    "노출지면명": "placement",
    "노출지면": "placement",
    "소재ID": "creative_id",
    "소재 ID": "creative_id",
    "광고집행상품명": "product_name",
    "광고집행 상품명": "product_name",
    "광고집행 옵션ID": "coupang_product_id",
    "광고집행옵션ID": "coupang_product_id",
    "랜딩페이지유형": "landing_page_type",
    "랜딩페이지 유형": "landing_page_type",
    "랜딩페이지명": "landing_page_name",
    "랜딩페이지 이름": "landing_page_name",
    "카테고리": "category",

    # 디스플레이광고 보고서 전용
    "노출영역": "placement",
    "노출 영역": "placement",
    "플랫폼": "platform",

    # 키워드
    "키워드": "keyword",
    "매치유형": "match_type",
    "매치 유형": "match_type",
    "일치 유형": "match_type",

    # 성과 지표
    "노출수": "impressions",
    "노출": "impressions",
    "클릭수": "clicks",
    "클릭": "clicks",
    "클릭률": "ctr",
    "CTR": "ctr",
    "클릭률(%)": "ctr",
    "CTR(%)": "ctr",
    "평균CPC": "avg_cpc",
    "평균 CPC": "avg_cpc",
    "평균클릭비용": "avg_cpc",
    "평균 클릭비용": "avg_cpc",

    # 비용
    "총비용": "ad_spend",
    "광고비": "ad_spend",
    "비용": "ad_spend",
    "총 비용": "ad_spend",
    "소진비용": "ad_spend",

    # 직접전환 (접미사 없는 경우)
    "직접전환주문수": "direct_orders",
    "직접전환 주문수": "direct_orders",
    "직접전환매출": "direct_revenue",
    "직접전환 매출액": "direct_revenue",
    "직접전환 매출": "direct_revenue",
    "직접전환매출액": "direct_revenue",

    # 간접전환 (접미사 없는 경우)
    "간접전환주문수": "indirect_orders",
    "간접전환 주문수": "indirect_orders",
    "간접전환매출": "indirect_revenue",
    "간접전환 매출액": "indirect_revenue",
    "간접전환 매출": "indirect_revenue",
    "간접전환매출액": "indirect_revenue",

    # 총전환 (접미사 없는 경우)
    "총전환주문수": "total_orders",
    "주문수": "total_orders",
    "총 전환 주문수": "total_orders",
    "전환수": "total_orders",
    "총전환매출": "total_revenue",
    "매출액": "total_revenue",
    "총 전환 매출": "total_revenue",
    "전환매출": "total_revenue",
    "총전환매출액": "total_revenue",

    # 판매수량 (접미사 없는 경우)
    "총판매수량": "total_quantity",
    "직접판매수량": "direct_quantity",
    "간접판매수량": "indirect_quantity",

    # ROAS
    "ROAS": "roas",
    "ROAS(%)": "roas",
    "광고수익률": "roas",
    "광고 수익률": "roas",
}


# ── 기여 기간(1일/14일) 접미사 컬럼 매핑 ──
# 기본명 → 영문 필드명 (접미사 제거 후 매핑)
ATTRIBUTION_BASE = {
    "총주문수": "total_orders",
    "총 주문수": "total_orders",
    "직접주문수": "direct_orders",
    "직접 주문수": "direct_orders",
    "간접주문수": "indirect_orders",
    "간접 주문수": "indirect_orders",
    "총판매수량": "total_quantity",
    "총 판매수량": "total_quantity",
    "직접판매수량": "direct_quantity",
    "직접 판매수량": "direct_quantity",
    "간접판매수량": "indirect_quantity",
    "간접 판매수량": "indirect_quantity",
    "총전환매출액": "total_revenue",
    "총 전환매출액": "total_revenue",
    "직접전환매출액": "direct_revenue",
    "직접 전환매출액": "direct_revenue",
    "간접전환매출액": "indirect_revenue",
    "간접 전환매출액": "indirect_revenue",
    "총광고수익률": "roas",
    "직접광고수익률": "roas_direct",
    "간접광고수익률": "roas_indirect",
    "광고수익률": "roas",
}


class AdPerformanceSync:
    """광고 성과 보고서 Excel 파싱 + DB 저장"""

    CREATE_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS ad_performances (
        id SERIAL PRIMARY KEY,
        account_id INTEGER NOT NULL REFERENCES accounts(id),
        ad_date DATE NOT NULL,
        campaign_id VARCHAR(50) DEFAULT '',
        campaign_name VARCHAR(200) DEFAULT '',
        ad_group_name VARCHAR(200) DEFAULT '',
        coupang_product_id VARCHAR(50) DEFAULT '',
        product_name VARCHAR(500) DEFAULT '',
        listing_id INTEGER REFERENCES listings(id),
        keyword VARCHAR(200) DEFAULT '',
        match_type VARCHAR(20) DEFAULT '',
        impressions INTEGER DEFAULT 0,
        clicks INTEGER DEFAULT 0,
        ctr REAL DEFAULT 0.0,
        avg_cpc INTEGER DEFAULT 0,
        ad_spend INTEGER DEFAULT 0,
        direct_orders INTEGER DEFAULT 0,
        direct_revenue INTEGER DEFAULT 0,
        indirect_orders INTEGER DEFAULT 0,
        indirect_revenue INTEGER DEFAULT 0,
        total_orders INTEGER DEFAULT 0,
        total_revenue INTEGER DEFAULT 0,
        roas REAL DEFAULT 0.0,
        total_quantity INTEGER DEFAULT 0,
        direct_quantity INTEGER DEFAULT 0,
        indirect_quantity INTEGER DEFAULT 0,
        bid_type VARCHAR(30) DEFAULT '',
        sales_method VARCHAR(20) DEFAULT '',
        ad_type VARCHAR(50) DEFAULT '',
        option_id VARCHAR(50) DEFAULT '',
        ad_name VARCHAR(200) DEFAULT '',
        placement VARCHAR(100) DEFAULT '',
        creative_id VARCHAR(50) DEFAULT '',
        category VARCHAR(200) DEFAULT '',
        report_type VARCHAR(20) DEFAULT 'campaign',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(account_id, ad_date, campaign_id, ad_group_name,
               coupang_product_id, keyword, report_type)
    )
    """

    CREATE_INDEXES_SQL = [
        "CREATE INDEX IF NOT EXISTS ix_adperf_account_date ON ad_performances(account_id, ad_date)",
        "CREATE INDEX IF NOT EXISTS ix_adperf_listing ON ad_performances(listing_id)",
        "CREATE INDEX IF NOT EXISTS ix_adperf_product ON ad_performances(coupang_product_id)",
    ]

    # 기존 테이블에 새 컬럼 추가 (ALTER TABLE 마이그레이션)
    MIGRATION_COLUMNS = [
        ("total_quantity", "INTEGER DEFAULT 0"),
        ("direct_quantity", "INTEGER DEFAULT 0"),
        ("indirect_quantity", "INTEGER DEFAULT 0"),
        ("bid_type", "VARCHAR(30) DEFAULT ''"),
        ("sales_method", "VARCHAR(20) DEFAULT ''"),
        ("ad_type", "VARCHAR(50) DEFAULT ''"),
        ("option_id", "VARCHAR(50) DEFAULT ''"),
        ("ad_name", "VARCHAR(200) DEFAULT ''"),
        ("placement", "VARCHAR(100) DEFAULT ''"),
        ("creative_id", "VARCHAR(50) DEFAULT ''"),
        ("category", "VARCHAR(200) DEFAULT ''"),
    ]

    def __init__(self, db_path: str = None):
        self.engine = get_engine_for_db(db_path)
        self._ensure_table()

    def _ensure_table(self):
        """테이블이 없으면 생성"""
        with self.engine.connect() as conn:
            exists = conn.execute(text(
                "SELECT 1 FROM information_schema.tables WHERE table_name = 'ad_performances'"
            )).fetchone()
            if not exists:
                conn.execute(text(self.CREATE_TABLE_SQL))
                for idx_sql in self.CREATE_INDEXES_SQL:
                    conn.execute(text(idx_sql))
            conn.commit()
        logger.info("ad_performances 테이블 확인 완료")

    def _safe_int(self, val) -> int:
        """안전한 정수 변환"""
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return 0
        try:
            # 쉼표 제거 후 변환
            s = str(val).replace(",", "").replace("원", "").strip()
            if not s or s == "-":
                return 0
            return int(float(s))
        except (ValueError, TypeError):
            return 0

    def _safe_float(self, val) -> float:
        """안전한 실수 변환"""
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return 0.0
        try:
            s = str(val).replace(",", "").replace("%", "").strip()
            if not s or s == "-":
                return 0.0
            return float(s)
        except (ValueError, TypeError):
            return 0.0

    def _safe_str(self, val) -> str:
        """안전한 문자열 변환 (nan 처리 포함)"""
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return ""
        s = str(val).strip()
        return "" if s.lower() == "nan" else s

    def _parse_date(self, val) -> Optional[date]:
        """다양한 날짜 형식 파싱"""
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        if isinstance(val, (datetime, date)):
            return val if isinstance(val, date) else val.date()
        if isinstance(val, pd.Timestamp):
            return val.date()

        s = str(val).strip()
        # YYYY-MM-DD
        m = re.match(r"(\d{4})-(\d{1,2})-(\d{1,2})", s)
        if m:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        # YYYY/MM/DD
        m = re.match(r"(\d{4})/(\d{1,2})/(\d{1,2})", s)
        if m:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        # YYYY년 MM월 DD일
        m = re.search(r"(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일", s)
        if m:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        # YYYYMMDD
        m = re.match(r"(\d{4})(\d{2})(\d{2})$", s)
        if m:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        return None

    def detect_report_type(self, columns: list) -> str:
        """컬럼명으로 보고서 유형 자동 감지"""
        col_set = set(columns)

        # 디스플레이광고: 노출영역/플랫폼 있고, 상품/키워드 없음
        has_placement = "placement" in col_set
        has_platform = "platform" in col_set
        has_product = "coupang_product_id" in col_set or "option_id" in col_set
        has_keyword = "keyword" in col_set
        has_creative = "creative_id" in col_set or "ad_name" in col_set

        if has_placement and has_platform and not has_product and not has_keyword:
            return "display"

        # 브랜드광고: 광고명/소재ID 있음
        if has_creative and has_keyword:
            return "brand"

        # 키워드 보고서
        if has_keyword and not has_product:
            return "keyword"

        # 상품광고 보고서
        if has_product:
            return "product"

        return "campaign"

    def _normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        한글 컬럼명을 영문으로 정규화.

        처리 순서:
          1. COLUMN_MAP 정확 매칭
          2. COLUMN_MAP 대소문자 무시 매칭
          3. (14일) 기여 기간 컬럼 → ATTRIBUTION_BASE로 매핑 (우선)
          4. (1일) 기여 기간 컬럼 → 14일이 없는 필드만 매핑
        """
        rename_map = {}
        mapped_targets = set()

        # Pass 1: COLUMN_MAP 정확 매칭
        for col in df.columns:
            stripped = str(col).strip()
            if stripped in COLUMN_MAP:
                target = COLUMN_MAP[stripped]
                rename_map[col] = target
                mapped_targets.add(target)

        # Pass 2: COLUMN_MAP 대소문자 무시 매칭
        for col in df.columns:
            if col in rename_map:
                continue
            stripped = str(col).strip()
            for k, v in COLUMN_MAP.items():
                if stripped.lower() == k.lower() and v not in mapped_targets:
                    rename_map[col] = v
                    mapped_targets.add(v)
                    break

        # Pass 3: (14일) 기여 기간 컬럼 (우선)
        for col in df.columns:
            if col in rename_map:
                continue
            stripped = str(col).strip()
            if "(14일)" in stripped:
                base = stripped.replace("(14일)", "").strip()
                if base in ATTRIBUTION_BASE:
                    target = ATTRIBUTION_BASE[base]
                    rename_map[col] = target
                    mapped_targets.add(target)

        # Pass 4: (1일) 기여 기간 컬럼 (14일 없을 때만)
        for col in df.columns:
            if col in rename_map:
                continue
            stripped = str(col).strip()
            if "(1일)" in stripped:
                base = stripped.replace("(1일)", "").strip()
                if base in ATTRIBUTION_BASE:
                    target = ATTRIBUTION_BASE[base]
                    if target not in mapped_targets:
                        rename_map[col] = target
                        mapped_targets.add(target)
                    else:
                        # 14일 이미 매핑됨 → _1d 접미사로 별도 저장 (필요시 참조)
                        rename_map[col] = target + "_1d"

        if rename_map:
            df = df.rename(columns=rename_map)

        # 매핑 안 된 컬럼 로그
        unmapped = [c for c in df.columns if c not in rename_map.values()
                    and c not in mapped_targets and not str(c).startswith("Unnamed")]
        if unmapped:
            logger.debug(f"매핑 안 된 컬럼: {unmapped}")

        return df

    def parse_excel(self, filepath: str, account_id: int = None) -> Tuple[Optional[int], List[dict]]:
        """
        Excel 파싱 → (account_id, rows) 반환

        account_id가 None이면 파일명에서 vendor_id를 추출하여 매칭 시도.
        """
        # account_id 결정
        if account_id is None:
            account_id = self._resolve_account_id(filepath)

        if account_id is None:
            logger.error(f"account_id를 결정할 수 없음: {filepath}")
            return None, []

        # Excel 읽기
        try:
            df = pd.read_excel(filepath, engine="openpyxl")
        except Exception as e:
            logger.error(f"Excel 읽기 오류: {filepath} → {e}")
            return account_id, []

        if df.empty:
            logger.warning(f"빈 Excel: {filepath}")
            return account_id, []

        # 컬럼 정규화
        df = self._normalize_columns(df)
        report_type = self.detect_report_type(df.columns.tolist())
        logger.info(f"보고서 유형: {report_type} (파일: {Path(filepath).name})")
        logger.info(f"정규화된 컬럼: {df.columns.tolist()}")

        # 날짜 컬럼 없는 전체 기간 보고서 처리
        # 파일명에서 기간 추출: *_YYYYMMDD_YYYYMMDD.xlsx
        fallback_date = None
        if "ad_date" not in df.columns:
            fname = Path(filepath).stem
            m = re.search(r"(\d{8})_(\d{8})$", fname)
            if m:
                try:
                    d_from = date(int(m.group(1)[:4]), int(m.group(1)[4:6]), int(m.group(1)[6:8]))
                    d_to = date(int(m.group(2)[:4]), int(m.group(2)[4:6]), int(m.group(2)[6:8]))
                    fallback_date = d_to  # 종료일 사용
                    logger.info(f"날짜 컬럼 없음 → 파일명에서 기간 추출: {d_from} ~ {d_to}")
                except ValueError:
                    pass
            if fallback_date is None:
                logger.warning(f"날짜 컬럼 없고 파일명에서도 기간 추출 실패: {filepath}")

        rows = []
        for _, row in df.iterrows():
            ad_date = self._parse_date(row.get("ad_date"))
            if ad_date is None:
                ad_date = fallback_date
            if ad_date is None:
                continue

            parsed = {
                "ad_date": ad_date,
                "campaign_id": self._safe_str(row.get("campaign_id")),
                "campaign_name": self._safe_str(row.get("campaign_name")),
                "ad_group_name": self._safe_str(row.get("ad_group_name")),
                "coupang_product_id": self._safe_str(row.get("coupang_product_id")),
                "product_name": self._safe_str(row.get("product_name")),
                "keyword": self._safe_str(row.get("keyword")),
                "match_type": self._safe_str(row.get("match_type")),
                # 성과 지표
                "impressions": self._safe_int(row.get("impressions")),
                "clicks": self._safe_int(row.get("clicks")),
                "ctr": self._safe_float(row.get("ctr")),
                "avg_cpc": self._safe_int(row.get("avg_cpc")),
                "ad_spend": self._safe_int(row.get("ad_spend")),
                # 전환 (14일 우선, 없으면 1일, 없으면 접미사 없는 값)
                "direct_orders": self._safe_int(
                    row.get("direct_orders") or row.get("direct_orders_1d")),
                "direct_revenue": self._safe_int(
                    row.get("direct_revenue") or row.get("direct_revenue_1d")),
                "indirect_orders": self._safe_int(
                    row.get("indirect_orders") or row.get("indirect_orders_1d")),
                "indirect_revenue": self._safe_int(
                    row.get("indirect_revenue") or row.get("indirect_revenue_1d")),
                "total_orders": self._safe_int(
                    row.get("total_orders") or row.get("total_orders_1d")),
                "total_revenue": self._safe_int(
                    row.get("total_revenue") or row.get("total_revenue_1d")),
                "roas": self._safe_float(
                    row.get("roas") or row.get("roas_1d")),
                # 판매수량 (14일 우선)
                "total_quantity": self._safe_int(
                    row.get("total_quantity") or row.get("total_quantity_1d")),
                "direct_quantity": self._safe_int(
                    row.get("direct_quantity") or row.get("direct_quantity_1d")),
                "indirect_quantity": self._safe_int(
                    row.get("indirect_quantity") or row.get("indirect_quantity_1d")),
                # 광고 구분
                "bid_type": self._safe_str(row.get("bid_type")),
                "sales_method": self._safe_str(row.get("sales_method")),
                "ad_type": self._safe_str(row.get("ad_type")),
                "option_id": self._safe_str(row.get("option_id")),
                # 브랜드/디스플레이 전용
                "ad_name": self._safe_str(row.get("ad_name")),
                "placement": self._safe_str(row.get("placement")),
                "creative_id": self._safe_str(row.get("creative_id")),
                "category": self._safe_str(row.get("category")),
                # 메타
                "report_type": report_type,
            }

            rows.append(parsed)

        logger.info(f"파싱 완료: {filepath} → {len(rows)}건 (report_type={report_type})")
        return account_id, rows

    def _resolve_account_id(self, filepath: str) -> Optional[int]:
        """파일명에서 vendor_id 추출 → account_id 매칭"""
        fname = Path(filepath).stem
        parts = fname.split("-")
        if parts:
            vendor_id = parts[0]
            with self.engine.connect() as conn:
                row = conn.execute(
                    text("SELECT id FROM accounts WHERE vendor_id = :vid LIMIT 1"),
                    {"vid": vendor_id},
                ).fetchone()
                if row:
                    return row[0]
        return None

    def match_listings(self, account_id: int, rows: List[dict]) -> List[dict]:
        """coupang_product_id 또는 product_name으로 listing_id 매칭"""
        # 매칭 캐시 빌드
        with self.engine.connect() as conn:
            listings = conn.execute(
                text("""
                    SELECT id, coupang_product_id, product_name
                    FROM listings
                    WHERE account_id = :aid AND coupang_status = 'active'
                """),
                {"aid": account_id},
            ).mappings().all()

        pid_map = {}  # coupang_product_id → listing_id
        name_map = {}  # product_name → listing_id
        for l in listings:
            if l["coupang_product_id"]:
                pid_map[str(l["coupang_product_id"])] = l["id"]
            if l["product_name"]:
                name_map[l["product_name"]] = l["id"]

        matched = 0
        for row in rows:
            listing_id = None
            cpid = row.get("coupang_product_id", "")
            if cpid and cpid in pid_map:
                listing_id = pid_map[cpid]
            elif row.get("product_name") and row["product_name"] in name_map:
                listing_id = name_map[row["product_name"]]
            row["listing_id"] = listing_id
            if listing_id:
                matched += 1

        logger.info(f"리스팅 매칭: {matched}/{len(rows)}건")
        return rows

    def save_to_db(self, account_id: int, rows: List[dict]) -> int:
        """UPSERT — PostgreSQL execute_values 벌크 INSERT"""
        if not rows:
            return 0

        col_names = [
            "account_id", "ad_date", "campaign_id", "campaign_name", "ad_group_name",
            "coupang_product_id", "product_name", "listing_id",
            "keyword", "match_type",
            "impressions", "clicks", "ctr", "avg_cpc", "ad_spend",
            "direct_orders", "direct_revenue", "indirect_orders", "indirect_revenue",
            "total_orders", "total_revenue", "roas",
            "total_quantity", "direct_quantity", "indirect_quantity",
            "bid_type", "sales_method", "ad_type", "option_id",
            "ad_name", "placement", "creative_id", "category",
            "report_type",
        ]

        def _to_tuple(row):
            return (
                account_id, row["ad_date"].isoformat(), row["campaign_id"],
                row["campaign_name"], row["ad_group_name"],
                row["coupang_product_id"], row["product_name"], row.get("listing_id"),
                row["keyword"], row["match_type"],
                row["impressions"], row["clicks"], row["ctr"], row["avg_cpc"], row["ad_spend"],
                row["direct_orders"], row["direct_revenue"], row["indirect_orders"], row["indirect_revenue"],
                row["total_orders"], row["total_revenue"], row["roas"],
                row["total_quantity"], row["direct_quantity"], row["indirect_quantity"],
                row["bid_type"], row["sales_method"], row["ad_type"], row["option_id"],
                row["ad_name"], row["placement"], row["creative_id"], row["category"],
                row["report_type"],
            )

        # 중복 키 제거 (마지막 값 유지)
        unique_key = lambda r: (
            account_id, r["ad_date"].isoformat(), r["campaign_id"],
            r["ad_group_name"], r["coupang_product_id"], r["keyword"], r["report_type"],
        )
        deduped = {}
        for r in rows:
            deduped[unique_key(r)] = r
        rows = list(deduped.values())
        logger.info(f"중복 제거 후: {len(rows):,}건")

        tuples = [_to_tuple(r) for r in rows]

        # psycopg2 execute_values — 1회 SQL로 수천 건 벌크 INSERT
        update_cols = [
            "campaign_name", "ad_group_name", "product_name", "listing_id",
            "match_type", "impressions", "clicks", "ctr", "avg_cpc", "ad_spend",
            "direct_orders", "direct_revenue", "indirect_orders", "indirect_revenue",
            "total_orders", "total_revenue", "roas",
            "total_quantity", "direct_quantity", "indirect_quantity",
            "bid_type", "sales_method", "ad_type", "option_id",
            "ad_name", "placement", "creative_id", "category",
        ]
        _update_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
        _cols_str = ", ".join(col_names)

        sql = f"""INSERT INTO ad_performances ({_cols_str}) VALUES %s
            ON CONFLICT (account_id, ad_date, campaign_id, ad_group_name,
                         coupang_product_id, keyword, report_type)
            DO UPDATE SET {_update_set}"""

        from psycopg2.extras import execute_values
        raw_conn = self.engine.raw_connection()
        try:
            cur = raw_conn.cursor()
            BATCH = 2000
            upserted = 0
            for i in range(0, len(tuples), BATCH):
                batch = tuples[i:i + BATCH]
                execute_values(cur, sql, batch, page_size=BATCH)
                upserted += len(batch)
                logger.info(f"벌크 진행: {min(i + BATCH, len(tuples)):,}/{len(tuples):,}건")
            raw_conn.commit()
            cur.close()
        except Exception as e:
            raw_conn.rollback()
            logger.error(f"벌크 INSERT 실패: {e}")
            raise
        finally:
            raw_conn.close()

        logger.info(f"저장 완료: {upserted}/{len(rows)}건")
        return upserted

    def sync_file(self, filepath: str, account_id: int = None) -> dict:
        """단일 파일 동기화 (대시보드에서도 호출)"""
        aid, rows = self.parse_excel(filepath, account_id)
        if aid is None:
            return {"file": Path(filepath).name, "error": "계정 매칭 실패", "parsed": 0, "saved": 0}

        # 리스팅 매칭
        rows = self.match_listings(aid, rows)

        saved = self.save_to_db(aid, rows)

        # 계정명 조회
        with self.engine.connect() as conn:
            name_row = conn.execute(
                text("SELECT account_name FROM accounts WHERE id = :aid"),
                {"aid": aid},
            ).fetchone()
            account_name = name_row[0] if name_row else str(aid)

        # 기간 / 유형 정보
        dates = [r["ad_date"] for r in rows]
        date_range = f"{min(dates)} ~ {max(dates)}" if dates else "-"
        report_types = set(r["report_type"] for r in rows)

        return {
            "file": Path(filepath).name,
            "account": account_name,
            "account_id": aid,
            "period": date_range,
            "report_types": list(report_types),
            "parsed": len(rows),
            "saved": saved,
        }

    def sync_dir(self, dirpath: str, account_id: int = None) -> List[dict]:
        """폴더 내 모든 xlsx 파일 동기화"""
        p = Path(dirpath)
        files = sorted(p.glob("*.xlsx"))
        results = []
        for f in files:
            result = self.sync_file(str(f), account_id)
            results.append(result)
        return results


def main():
    parser = argparse.ArgumentParser(description="광고 성과 보고서 Excel → DB 동기화")
    parser.add_argument("path", nargs="?", help="Excel 파일 또는 폴더 경로")
    parser.add_argument("--dir", type=str, help="폴더 경로 (내부 xlsx 전체)")
    parser.add_argument("--account-id", type=int, help="계정 ID (자동 감지 불가 시)")
    args = parser.parse_args()

    syncer = AdPerformanceSync()

    if args.dir:
        results = syncer.sync_dir(args.dir, args.account_id)
    elif args.path:
        results = [syncer.sync_file(args.path, args.account_id)]
    else:
        print("Excel 파일 경로를 지정해주세요.")
        print("  python scripts/sync_ad_performance.py report.xlsx")
        print("  python scripts/sync_ad_performance.py --dir ./reports/")
        return

    # 리포트
    print("\n" + "=" * 70)
    print("광고 성과 보고서 동기화 결과")
    print("=" * 70)
    for r in results:
        if r.get("error"):
            print(f"  {r['file']:40s} | 오류: {r['error']}")
        else:
            types = ", ".join(r.get("report_types", []))
            print(
                f"  {r['file']:40s} | {r['account']:10s} | {r['period']} "
                f"| {types} | 파싱 {r['parsed']:4d} | 저장 {r['saved']:4d}"
            )
    print("=" * 70)

    # DB 확인
    eng = get_engine_for_db()
    with eng.connect() as conn:
        cnt = conn.execute(text("SELECT COUNT(*) FROM ad_performances")).scalar()
        print(f"\nad_performances 총 레코드: {cnt:,}건")


if __name__ == "__main__":
    main()
