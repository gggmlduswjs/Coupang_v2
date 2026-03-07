"""
Wing 셀러센터 바로가기
======================
wing:// 커스텀 프로토콜로 Chrome 자동 열기 + 계정별 실시간 현황
"""
import os
import sys
import json
import subprocess
from pathlib import Path
from datetime import datetime, timezone, timedelta
import streamlit as st
import streamlit.components.v1 as components

_IS_LOCAL = sys.platform == "win32"
_CAN_CALL_API = True
_WING_CREDS_PATH = Path(__file__).resolve().parents[2] / "wing_creds.json"


# ─────────────────────────────────────────
# 자격증명
# ─────────────────────────────────────────

def _load_creds() -> dict:
    if "wing_creds_override" in st.session_state:
        return st.session_state["wing_creds_override"]
    if _WING_CREDS_PATH.exists():
        try:
            raw = json.loads(_WING_CREDS_PATH.read_text(encoding="utf-8"))
            return {k.lower(): {"id": v.get("id", ""), "pw": v.get("pw", "")} for k, v in raw.items()}
        except Exception:
            pass
    try:
        raw = dict(st.secrets.get("wing_creds", {}))
        result = {}
        for name, val in raw.items():
            if hasattr(val, "items"):
                result[name.lower()] = {"id": val.get("id", ""), "pw": val.get("pw", "")}
            else:
                result[name.lower()] = {"id": str(val), "pw": ""}
        return result
    except Exception:
        return {}


def _match_cred(creds: dict, account_name: str) -> dict:
    return (
        creds.get(account_name)
        or creds.get(account_name.lower())
        or creds.get(account_name.upper())
        or {}
    )


# ─────────────────────────────────────────
# wing:// HTML 생성
# ─────────────────────────────────────────

def _launcher_html(names: list) -> str:
    names_js = json.dumps(names)
    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8">
<style>
*{{box-sizing:border-box;margin:0;padding:0;font-family:'Segoe UI',sans-serif;font-size:14px;}}
body{{padding:4px 0;}}
.btn-all{{width:100%;height:48px;background:#E4002B;color:#fff;border:none;
  border-radius:10px;font-size:16px;font-weight:700;cursor:pointer;margin-bottom:10px;
  transition:opacity .15s;}}
.btn-all:hover{{opacity:.85;}}
.grid{{display:flex;gap:6px;}}
.btn-one{{flex:1;height:38px;background:#fff;color:#333;
  border:1.5px solid #ddd;border-radius:8px;cursor:pointer;font-size:13px;font-weight:600;
  transition:all .15s;}}
.btn-one:hover{{background:#f0f0f0;border-color:#E4002B;color:#E4002B;}}
</style></head>
<body>
<button class="btn-all" onclick="openAll()">5개 계정 전부 열기</button>
<div class="grid" id="grid"></div>
<script>
const NAMES = {names_js};
const WING_URL = 'https://wing.coupang.com';
function openAll() {{
  NAMES.forEach(n => window.open(WING_URL, '_blank'));
}}
const grid = document.getElementById('grid');
NAMES.forEach(n => {{
  const b = document.createElement('button');
  b.className = 'btn-one';
  b.textContent = n;
  b.onclick = () => window.open(WING_URL, '_blank');
  grid.appendChild(b);
}});
</script>
</body></html>"""


# ─────────────────────────────────────────
# 주문 현황 데이터 로드
# ─────────────────────────────────────────

_STATUS_MAP = {
    "ACCEPT": ("결제완료", "#dc3545"),
    "INSTRUCT": ("상품준비중", "#fd7e14"),
    "DEPARTURE": ("배송지시", "#0d6efd"),
    "DELIVERING": ("배송중", "#6f42c1"),
    "FINAL_DELIVERY": ("배송완료", "#198754"),
}


@st.cache_data(ttl=30)
def _load_order_stats() -> dict:
    """계정별 주문 상태 집계. {account_name: {status: count, ...}, ...}
    활성 상태(ACCEPT~DELIVERING)는 날짜 필터 없이 현재 전체,
    배송완료(FINAL_DELIVERY)만 30일 필터 적용."""
    from dashboard.utils import query_df
    df = query_df("""
        SELECT a.account_name, o.status,
               COUNT(DISTINCT o.shipment_box_id) AS cnt
        FROM orders o
        JOIN accounts a ON o.account_id = a.id
        WHERE a.is_active = true
          AND o.canceled = false
          AND o.status IN ('ACCEPT','INSTRUCT','DEPARTURE','DELIVERING')
        GROUP BY a.account_name, o.status
        UNION ALL
        SELECT a.account_name, o.status,
               COUNT(DISTINCT o.shipment_box_id) AS cnt
        FROM orders o
        JOIN accounts a ON o.account_id = a.id
        WHERE a.is_active = true
          AND o.canceled = false
          AND o.status = 'FINAL_DELIVERY'
          AND o.ordered_at >= NOW() - INTERVAL '30 days'
        GROUP BY a.account_name, o.status
    """)
    if df.empty:
        return {}
    result = {}
    for _, row in df.iterrows():
        name = row["account_name"]
        if name not in result:
            result[name] = {}
        result[name][row["status"]] = int(row["cnt"])
    return result


@st.cache_data(ttl=60)
def _load_product_counts() -> dict:
    """계정별 활성 상품 수. {account_name: count}"""
    from dashboard.utils import query_df
    df = query_df("""
        SELECT a.account_name, COUNT(*) AS cnt
        FROM listings l
        JOIN accounts a ON l.account_id = a.id
        WHERE a.is_active = true
          AND l.coupang_status = 'active'
        GROUP BY a.account_name
    """)
    if df.empty:
        return {}
    return dict(zip(df["account_name"], df["cnt"]))


# ─────────────────────────────────────────
# 계정 카드 렌더링
# ─────────────────────────────────────────

def _render_account_card(name: str, stats: dict, product_count: int):
    """단일 계정의 KPI 카드"""
    st.markdown(f"### {name}")

    cols = st.columns(6)
    # 주문 상태 5개
    for i, (status_key, (label, color)) in enumerate(_STATUS_MAP.items()):
        count = stats.get(status_key, 0)
        with cols[i]:
            st.markdown(
                f"<div style='text-align:center;padding:8px 0;'>"
                f"<div style='font-size:12px;color:#666;'>{label}</div>"
                f"<div style='font-size:28px;font-weight:700;color:{color};'>{count:,}건</div>"
                f"</div>",
                unsafe_allow_html=True,
            )
    # 활성 상품 수
    with cols[5]:
        st.markdown(
            f"<div style='text-align:center;padding:8px 0;'>"
            f"<div style='font-size:12px;color:#666;'>활성상품</div>"
            f"<div style='font-size:28px;font-weight:700;color:#333;'>{product_count:,}건</div>"
            f"</div>",
            unsafe_allow_html=True,
        )


# ─────────────────────────────────────────
# 주문 동기화 (새로고침)
# ─────────────────────────────────────────

_KST = timezone(timedelta(hours=9))


def _run_order_sync():
    """WING API에서 주문 데이터 실시간 동기화"""
    with st.spinner("WING API에서 주문 동기화 중..."):
        try:
            from scripts.sync.sync_orders import OrderSync
            syncer = OrderSync()
            results = syncer.sync_all(days=1)

            total_fetched = sum(r["fetched"] for r in results)
            total_upserted = sum(r["upserted"] for r in results)
            errors = [r["account"] for r in results if r["fetched"] == 0]

            # 캐시 초기화
            _load_order_stats.clear()
            _load_product_counts.clear()

            if errors and len(errors) == len(results):
                st.error(f"API 동기화 실패 (IP 차단 가능성). 셀러센터에서 IP 등록을 확인하세요.")
            elif errors:
                st.warning(f"일부 계정 실패: {', '.join(errors)} | 성공: {total_upserted}건 저장")
            else:
                st.success(f"동기화 완료! {total_fetched}건 조회, {total_upserted}건 저장")
        except Exception as e:
            st.error(f"동기화 오류: {e}")


def _show_last_sync_time():
    """DB에서 마지막 동기화 시각 조회"""
    from dashboard.utils import query_df
    df = query_df("SELECT MAX(updated_at) AS last_sync FROM orders")
    if not df.empty and df.iloc[0]["last_sync"] is not None:
        last = df.iloc[0]["last_sync"]
        try:
            import pandas as pd
            if isinstance(last, pd.Timestamp) and last.tzinfo is not None:
                last = last.tz_convert("Asia/Seoul")
        except Exception:
            pass
        st.caption(f"마지막 동기화: {last:%Y-%m-%d %H:%M:%S}")


# ─────────────────────────────────────────
# 메인 render
# ─────────────────────────────────────────

def render(selected_account, accounts_df, account_names):
    st.title("Wing 자동 열기")

    if accounts_df.empty:
        st.warning("활성 계정이 없습니다.")
        return

    creds = _load_creds()
    names = accounts_df["account_name"].tolist()

    # ── Wing 셀러센터 바로가기 ────────────────────
    st.link_button("5개 계정 전부 열기", "wing://open-all", use_container_width=True, type="primary")
    cols = st.columns(len(names))
    for i, name in enumerate(names):
        with cols[i]:
            st.link_button(name, f"wing://open/{name}", use_container_width=True)

    # ── 자격증명 설정 (접힘) ─────────────────────
    all_set = all(_match_cred(creds, n).get("id") and _match_cred(creds, n).get("pw") for n in names)
    with st.expander(f"Wing 로그인 정보 설정{'  ✅' if all_set else '  ⚠️ 미설정'}", expanded=False):
        edited = {}
        for name in names:
            ex = _match_cred(creds, name)
            c1, c2 = st.columns(2)
            with c1:
                wid = st.text_input(f"{name} ID", value=ex.get("id", ""), key=f"_wid_{name}")
            with c2:
                wpw = st.text_input(f"{name} PW", value=ex.get("pw", ""), type="password", key=f"_wpw_{name}")
            edited[name] = {"id": wid, "pw": wpw}
        c1, c2 = st.columns(2)
        with c1:
            if st.button("저장 (이 세션)", type="primary", key="_wcred_save"):
                st.session_state["wing_creds_override"] = edited
                st.rerun()
        with c2:
            if _IS_LOCAL:
                if st.button("로컬에 저장 (wing:// 자동로그인용)", key="_wcred_local_save"):
                    try:
                        out = {n: {"id": v["id"], "pw": v["pw"]} for n, v in edited.items() if v.get("id") and v.get("pw")}
                        _WING_CREDS_PATH.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
                        st.success("wing_creds.json 저장됨")
                    except Exception as e:
                        st.error(f"저장 실패: {e}")
            else:
                out = {n: {"id": v["id"], "pw": v["pw"]} for n, v in edited.items() if v.get("id") and v.get("pw")}
                if out:
                    st.download_button("wing_creds.json 다운로드", json.dumps(out, ensure_ascii=False, indent=2),
                                       file_name="wing_creds.json", mime="application/json", key="_wcred_dl")

    st.divider()

    # ── 계정별 실시간 현황 ───────────────────────
    c_title, c_btn = st.columns([3, 1])
    with c_title:
        st.subheader("계정별 현황")
    with c_btn:
        if _CAN_CALL_API:
            if st.button("주문 새로고침", type="primary", key="_sync_orders"):
                _run_order_sync()

    # 마지막 동기화 시각 표시
    _show_last_sync_time()

    order_stats = _load_order_stats()
    product_counts = _load_product_counts()

    # 전체 합산 KPI
    total_stats = {}
    for acct_stats in order_stats.values():
        for status_key, cnt in acct_stats.items():
            total_stats[status_key] = total_stats.get(status_key, 0) + cnt
    total_products = sum(product_counts.values())

    cols = st.columns(6)
    for i, (status_key, (label, color)) in enumerate(_STATUS_MAP.items()):
        count = total_stats.get(status_key, 0)
        cols[i].metric(label, f"{count:,}건")
    cols[5].metric("활성상품", f"{total_products:,}건")

    st.divider()

    # 계정별 카드
    for name in names:
        stats = order_stats.get(name, {})
        pc = product_counts.get(name, 0)
        _render_account_card(name, stats, pc)
        st.divider()
