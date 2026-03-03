"""
대시보드 공통 유틸리티
=====================
query_df, run_sql, create_wing_client, 포맷터 등 모든 페이지에서 공유하는 함수.
"""
import os
import streamlit as st
import pandas as pd
from sqlalchemy import text
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode

from core.api.wing_client import CoupangWingClient, CoupangWingError
from core.constants import WING_ACCOUNT_ENV_MAP
from core.database import engine


# ─── 데이터 접근 ───

@st.cache_data(ttl=10)
def query_df(sql: str, params: dict = None) -> pd.DataFrame:
    """SQL → DataFrame (10초 캐시, 실시간 데이터용)"""
    try:
        return pd.read_sql(text(sql), engine, params=params)
    except Exception as e:
        st.error(f"DB 오류: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def query_df_cached(sql: str, params: dict = None) -> pd.DataFrame:
    """SQL → DataFrame (5분 캐시, 정적/준정적 데이터용: 리스팅 메타, 출판사, 상품 등)"""
    try:
        return pd.read_sql(text(sql), engine, params=params)
    except Exception as e:
        st.error(f"DB 오류: {e}")
        return pd.DataFrame()


def run_sql(sql: str, params: dict = None):
    """INSERT/UPDATE/DELETE 실행"""
    with engine.connect() as conn:
        conn.execute(text(sql), params or {})
        conn.commit()


# ─── WING API ───

def create_wing_client(account_row):
    """계정 정보로 WING API 클라이언트 생성"""
    account_name = account_row["account_name"]
    env_prefix = WING_ACCOUNT_ENV_MAP.get(account_name, "")
    vendor_id = account_row.get("vendor_id") or ""
    access_key = account_row.get("wing_access_key") or ""
    secret_key = account_row.get("wing_secret_key") or ""
    if not access_key and env_prefix:
        vendor_id = os.getenv(f"{env_prefix}_VENDOR_ID", vendor_id)
        access_key = os.getenv(f"{env_prefix}_ACCESS_KEY", "")
        secret_key = os.getenv(f"{env_prefix}_SECRET_KEY", "")
    if not all([vendor_id, access_key, secret_key]):
        return None
    return CoupangWingClient(vendor_id, access_key, secret_key)


# ─── 포맷터 ───

_MONEY_KEYWORDS = ["판매", "마진", "정산", "수수료", "지급", "차감", "유보", "환불금액"]


def fmt_money_df(df: pd.DataFrame) -> pd.DataFrame:
    """금액 컬럼에 천단위 쉼표 포맷"""
    d = df.copy()
    for col in d.columns:
        if any(kw in col for kw in _MONEY_KEYWORDS) and pd.api.types.is_numeric_dtype(d[col]):
            d[col] = d[col].apply(lambda x: f"{int(x):,}" if pd.notna(x) else "0")
    return d


def fmt_krw(val):
    """한국식 금액 표시 (₩520만, ₩1.2억)"""
    val = int(val)
    if abs(val) >= 100_000_000:
        return f"₩{val / 100_000_000:.1f}억"
    elif abs(val) >= 10_000:
        return f"₩{val / 10_000:.0f}만"
    return f"₩{val:,}"


# ─── 상품 등록 헬퍼 ───

def product_to_upload_data(row):
    """상품 DB 행 → 등록용 딕셔너리"""
    sr = float(row.get("supply_rate", 0.65) or 0.65)
    return {
        "product_name": row.get("title", ""),
        "publisher": row.get("publisher_name", ""),
        "author": row.get("author", ""),
        "isbn": row.get("isbn", ""),
        "original_price": int(row.get("list_price", 0)),
        "sale_price": int(row.get("sale_price", 0)),
        "main_image_url": "",
        "description": "",
        "shipping_policy": row.get("shipping_policy", "free"),
        "margin_rate": int(round(sr * 100)),
    }


# ─── AgGrid 래퍼 ───

def render_grid(df: pd.DataFrame, key: str, height: int = 450,
                page_size: int = 20, wide_cols: dict = None):
    """AgGrid 표준 래퍼 (일관된 설정)"""
    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_pagination(paginationAutoPageSize=False, paginationPageSize=page_size)
    gb.configure_default_column(resizable=True, sorteable=True, filterable=True)
    if wide_cols:
        for col, width in wide_cols.items():
            gb.configure_column(col, width=width)
    grid_opts = gb.build()
    return AgGrid(df, gridOptions=grid_opts, height=height, theme="streamlit", key=key)


# ─── KPI 카드 ───

def render_kpi_row(metrics: list):
    """
    KPI 카드 행 렌더링.
    metrics: [(label, value, delta?, delta_color?), ...]
    """
    cols = st.columns(len(metrics))
    for col, item in zip(cols, metrics):
        label, value = item[0], item[1]
        delta = item[2] if len(item) > 2 else None
        delta_color = item[3] if len(item) > 3 else "normal"
        col.metric(label, value, delta=delta, delta_color=delta_color)


# ─── 동기화 버튼 래퍼 ───

def run_sync_with_progress(sync_class, label: str, accounts_df_param=None, **kwargs):
    """동기화 실행 + progress bar + 결과 표시"""
    try:
        syncer = sync_class()
        sync_accounts = syncer._get_accounts()
        bar = st.progress(0, text=f"{label} 시작...")
        results = []
        for i, sa in enumerate(sync_accounts):
            bar.progress(i / len(sync_accounts), text=f"[{sa['account_name']}] 동기화 중...")
            r = syncer.sync_account(sa, **kwargs)
            results.append(r)
        bar.progress(1.0, text=f"{label} 완료!")
        total_upserted = sum(r.get("upserted", 0) for r in results)
        total_fetched = sum(r.get("fetched", 0) for r in results)
        st.success(f"{label} 완료: {len(sync_accounts)}개 계정, {total_fetched}건 조회, {total_upserted}건 저장")
        return results
    except Exception as e:
        st.error(f"{label} 오류: {e}")
        return []
