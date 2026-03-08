"""
쿠팡 도서 자동화 대시보드
=========================
계정별 상품 관리 + API 등록 기능
실행: streamlit run dashboard.py
"""
import sys
import os
import streamlit as st
from pathlib import Path
import logging

# 프로젝트 루트를 path에 추가
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

# ── Streamlit Cloud: st.secrets → os.environ 주입 (DB 엔진 초기화 전 필수) ──
# 로컬에서는 .env가 이미 로드됐으므로 setdefault 로 덮어쓰지 않음
try:
    for _k, _v in st.secrets.items():
        if isinstance(_v, str):
            os.environ.setdefault(_k, _v)
except Exception:
    pass

logging.basicConfig(level=logging.INFO)

# ─── 페이지 설정 ───
st.set_page_config(page_title="쿠팡 도서 자동화", page_icon="📚", layout="wide")

# ─── DB 연결 확인 ───
from core.database import _database_url
if _database_url.startswith("sqlite"):
    st.error(
        "⚠️ **데이터베이스 미연결** — Streamlit Cloud Secrets에 `DATABASE_URL`이 설정되지 않았습니다.\n\n"
        "**설정 방법**: Streamlit Cloud 앱 대시보드 → ⋮ → Settings → Secrets 탭에 아래 내용 붙여넣기:\n"
        "```\nDATABASE_URL = \"postgresql://postgres.glivxzmrgypqhtryoglg:0864gmldus!@aws-1-ap-south-1.pooler.supabase.com:6543/postgres\"\n```"
    )
    st.stop()

# ─── 공통 유틸 ───
from dashboard.utils import query_df

# Streamlit 자동 pages 메뉴 숨김
st.markdown("<style>[data-testid='stSidebarNav']{display:none}</style>", unsafe_allow_html=True)

# ─── 사이드바 ───
st.sidebar.title("📚 쿠팡 도서 자동화")

accounts_df = query_df("""
    SELECT id, account_name, vendor_id, wing_api_enabled,
           wing_access_key, wing_secret_key,
           outbound_shipping_code, return_center_code
    FROM accounts WHERE is_active = true ORDER BY account_name
""")
account_names = accounts_df["account_name"].tolist() if not accounts_df.empty else []

st.sidebar.divider()
page = st.sidebar.radio("메뉴", ["Wing 바로가기", "주문/배송", "반품/교환", "CS", "상품조회", "상품등록"], key="sidebar_menu")
st.sidebar.divider()

if os.environ.get("RAILWAY_ENVIRONMENT"):
    try:
        import urllib.request
        _server_ip = urllib.request.urlopen("https://api.ipify.org", timeout=5).read().decode()
        st.sidebar.caption(f"서버 IP: {_server_ip}")
    except Exception:
        pass


# ─── 페이지 라우팅 ───
# 상품 페이지는 내부에서 계정 선택 처리 (selected_account 불필요)
# 주문/배송, 반품은 전 계정 실시간 조회 (selected_account는 처리 탭에서만 사용)

if page == "Wing 바로가기":
    from dashboard.pages.wing_login import render
    render(None, accounts_df, account_names)

elif page == "주문/배송":
    from dashboard.pages.orders import render
    render(None, accounts_df, account_names)

elif page == "반품/교환":
    from dashboard.pages.returns import render
    render(None, accounts_df, account_names)

elif page == "CS":
    from dashboard.pages.cs import render
    render(None, accounts_df, account_names)

elif page == "상품조회":
    from dashboard.pages.products_browse import render
    render(None, accounts_df, account_names)

elif page == "상품등록":
    from dashboard.pages.products_register_page import render
    render(None, accounts_df, account_names)

