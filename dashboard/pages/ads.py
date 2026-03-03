"""
광고 보고서 업로드 + 분석 페이지
===============================
계정별 광고 성과 / 광고비 정산 Excel 업로드 → DB 저장 → 전체 분석.
"""

import logging
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import streamlit as st
from sqlalchemy import text

from dashboard.utils import (
    query_df,
    fmt_krw,
    fmt_money_df,
    engine,
)

logger = logging.getLogger(__name__)
ROOT = Path(__file__).resolve().parent.parent.parent


def render(selected_account, accounts_df, account_names):
    """광고 페이지"""

    st.title("광고")

    tab_upload, tab_analysis = st.tabs(["업로드", "분석"])

    with tab_upload:
        _render_upload(accounts_df, account_names)

    with tab_analysis:
        _render_analysis(accounts_df, account_names)


# ═════════════════════════════════════════
# 업로드 탭
# ═════════════════════════════════════════

def _render_upload(accounts_df, account_names):
    """계정별 광고 보고서 업로드"""

    st.caption(
        "WING > 광고 관리 > 광고 보고서에서 다운로드한 Excel을 계정별로 업로드하세요."
    )

    # ── 계정 선택 ──
    upload_account = st.selectbox(
        "계정 선택",
        account_names,
        key="ad_upload_account",
    )

    if not upload_account:
        st.info("계정을 선택하세요.")
        return

    acct_row = accounts_df[accounts_df["account_name"] == upload_account].iloc[0]
    acct_id = int(acct_row["id"])
    vendor_id = acct_row.get("vendor_id", "")

    st.caption(f"Vendor ID: **{vendor_id}**")

    # ── 2개 섹션 나란히 ──
    col_perf, col_spend = st.columns(2)

    # ── 좌: 매출 성장 광고 보고서 (= 상품광고) ──
    with col_perf:
        st.subheader("광고 성과 보고서")
        st.caption(
            "매출 성장 광고 보고서 다운로드\n\n"
            "설정: 기간 구분 **일별**, 보고서 구조 **캠페인>광고그룹>상품>키워드**"
        )

        perf_files = st.file_uploader(
            "광고 성과 Excel",
            type=["xlsx", "xls"],
            key=f"ad_perf_{upload_account}",
            accept_multiple_files=True,
        )

        if perf_files and st.button(
            f"성과 보고서 동기화 ({len(perf_files)}개)",
            key="btn_perf_sync",
            type="primary",
            use_container_width=True,
        ):
            _sync_performance(perf_files, acct_id)

    # ── 우: 일별 광고비 정산 ──
    with col_spend:
        st.subheader("광고비 정산 보고서")
        st.caption(
            "광고비 정산 보고서 > 일별 광고비 정산내역 다운로드\n\n"
            "파일명에 vendor_id가 자동 포함됩니다."
        )

        spend_files = st.file_uploader(
            "광고비 정산 Excel",
            type=["xlsx", "xls"],
            key=f"ad_spend_{upload_account}",
            accept_multiple_files=True,
        )

        if spend_files and st.button(
            f"정산 보고서 동기화 ({len(spend_files)}개)",
            key="btn_spend_sync",
            type="primary",
            use_container_width=True,
        ):
            _sync_spend(spend_files)

    st.divider()

    # ── 현재 계정 업로드 현황 ──
    st.subheader(f"{upload_account} 업로드 현황")
    _render_account_status(acct_id, upload_account)


def _sync_performance(files, account_id):
    """광고 성과 보고서 동기화"""
    from scripts.sync.sync_ad_performance import AdPerformanceSync

    syncer = AdPerformanceSync()
    results = []
    progress = st.progress(0, text="동기화 준비 중...")

    for i, f in enumerate(files):
        progress.progress(i / len(files), text=f"처리 중: {f.name}")
        tmp_path = ROOT / "data" / "reports" / f.name
        tmp_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path.write_bytes(f.getvalue())

        try:
            result = syncer.sync_file(str(tmp_path), account_id=account_id)
            results.append(result)
        except Exception as e:
            results.append({"file": f.name, "error": str(e), "parsed": 0, "saved": 0})

    progress.progress(1.0, text="완료!")
    _show_sync_results("광고 성과", results)
    query_df.clear()


def _sync_spend(files):
    """광고비 정산 보고서 동기화"""
    from scripts.sync.sync_ad_spend import AdSpendSync

    syncer = AdSpendSync()
    results = []
    progress = st.progress(0, text="동기화 준비 중...")

    for i, f in enumerate(files):
        progress.progress(i / len(files), text=f"처리 중: {f.name}")
        tmp_path = ROOT / "data" / "reports" / f.name
        tmp_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path.write_bytes(f.getvalue())

        try:
            result = syncer.sync_file(str(tmp_path))
            results.append(result)
        except Exception as e:
            results.append({"file": f.name, "error": str(e), "parsed": 0, "saved": 0})

    progress.progress(1.0, text="완료!")
    _show_sync_results("광고비 정산", results)
    query_df.clear()


def _show_sync_results(label: str, results: list):
    """동기화 결과 표시"""
    total_parsed = sum(r.get("parsed", 0) for r in results)
    total_saved = sum(r.get("saved", 0) for r in results)
    errors = [r for r in results if r.get("error")]

    if total_saved > 0:
        st.success(f"{label}: {len(results)}개 파일, 파싱 {total_parsed:,}건, 저장 {total_saved:,}건")
    elif not errors:
        st.warning(f"{label}: 새로 저장할 데이터가 없습니다 (이미 동기화됨)")

    for r in errors:
        st.error(f"{r['file']}: {r['error']}")

    if results:
        rows = []
        for r in results:
            rows.append({
                "파일": r.get("file", ""),
                "계정": r.get("account", "-"),
                "기간": r.get("period", "-"),
                "파싱": r.get("parsed", 0),
                "저장": r.get("saved", 0),
                "오류": r.get("error", ""),
            })
        st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)


def _render_account_status(acct_id: int, acct_name: str):
    """단일 계정의 업로드 현황"""
    try:
        perf = query_df(f"""
            SELECT MIN(ad_date) as 시작일, MAX(ad_date) as 종료일,
                COUNT(*) as 레코드수,
                SUM(ad_spend) as 광고비, SUM(total_revenue) as 전환매출
            FROM ad_performances WHERE account_id = {acct_id}
        """)
        spend = query_df(f"""
            SELECT MIN(ad_date) as 시작일, MAX(ad_date) as 종료일,
                COUNT(*) as 레코드수,
                SUM(total_charge) as 총비용
            FROM ad_spends WHERE account_id = {acct_id}
        """)
    except Exception:
        st.info("아직 업로드된 데이터가 없습니다.")
        return

    c1, c2 = st.columns(2)
    with c1:
        p = perf.iloc[0] if not perf.empty else None
        p_cnt = int(p["레코드수"]) if p is not None and pd.notna(p["레코드수"]) else 0
        if p_cnt > 0:
            st.metric("광고 성과", f"{p_cnt:,}건")
            st.caption(f"{p['시작일']} ~ {p['종료일']}")
        else:
            st.metric("광고 성과", "0건")
            st.caption("업로드 필요")

    with c2:
        s = spend.iloc[0] if not spend.empty else None
        s_cnt = int(s["레코드수"]) if s is not None and pd.notna(s["레코드수"]) else 0
        if s_cnt > 0:
            st.metric("광고비 정산", f"{s_cnt:,}건")
            st.caption(f"{s['시작일']} ~ {s['종료일']}")
        else:
            st.metric("광고비 정산", "0건")
            st.caption("업로드 필요")


# ═════════════════════════════════════════
# 분석 탭
# ═════════════════════════════════════════

def _render_analysis(accounts_df, account_names):
    """전체 광고 분석"""

    # 데이터 존재 확인
    try:
        p_cnt = int(query_df("SELECT COUNT(*) as c FROM ad_performances").iloc[0]["c"])
        s_cnt = int(query_df("SELECT COUNT(*) as c FROM ad_spends").iloc[0]["c"])
    except Exception:
        p_cnt, s_cnt = 0, 0

    if p_cnt == 0 and s_cnt == 0:
        st.info("광고 데이터가 없습니다. '업로드' 탭에서 보고서를 업로드하세요.")
        return

    # ── 상단 컨트롤 ──
    ctrl1, ctrl2 = st.columns([3, 3])
    with ctrl1:
        period_opt = st.selectbox("기간", ["1주", "2주", "1개월", "3개월"], index=2, key="ad_period")
    with ctrl2:
        acct_filter = st.selectbox("계정", ["전체"] + account_names, key="ad_acct_filter")

    period_map = {"1주": 7, "2주": 14, "1개월": 30, "3개월": 90}
    days_back = period_map[period_opt]
    d_to = date.today()
    d_from = d_to - timedelta(days=days_back)
    d_from_s = d_from.isoformat()
    d_to_s = d_to.isoformat()

    # 계정 필터
    acct_where_perf = ""
    acct_where_spend = ""
    if acct_filter != "전체":
        _aid_row = accounts_df[accounts_df["account_name"] == acct_filter]
        if _aid_row.empty:
            st.error(f"계정 '{acct_filter}'을 찾을 수 없습니다.")
            return
        _aid = int(_aid_row.iloc[0]["id"])
        acct_where_perf = f"AND ap.account_id = {_aid}"
        acct_where_spend = f"AND ad.account_id = {_aid}"

    st.divider()

    # ── KPI ──
    _render_kpi(d_from_s, d_to_s, acct_where_perf, acct_where_spend, p_cnt, s_cnt)

    st.divider()

    # ── 일별 추이 ──
    if p_cnt > 0:
        _render_daily_chart(d_from_s, d_to_s, acct_where_perf)

    # ── 하단 분석 탭 ──
    if acct_filter == "전체":
        sub1, sub2, sub3, sub4 = st.tabs(["계정별 비교", "캠페인별", "상품별 TOP", "효율 리포트"])
    else:
        sub1, sub2, sub3, sub4 = st.tabs(["캠페인별", "상품별 TOP", "키워드별", "효율 리포트"])

    if acct_filter == "전체":
        with sub1:
            _render_account_compare(d_from_s, d_to_s, p_cnt, s_cnt)
        with sub2:
            _render_campaign_table(d_from_s, d_to_s, acct_where_perf, p_cnt)
        with sub3:
            _render_product_top(d_from_s, d_to_s, acct_where_perf, p_cnt)
    else:
        with sub1:
            _render_campaign_table(d_from_s, d_to_s, acct_where_perf, p_cnt)
        with sub2:
            _render_product_top(d_from_s, d_to_s, acct_where_perf, p_cnt)
        with sub3:
            _render_keyword_table(d_from_s, d_to_s, acct_where_perf, p_cnt)

    with sub4:
        _render_efficiency_report(d_from_s, d_to_s, acct_where_perf, p_cnt)


def _render_kpi(d_from, d_to, aw_perf, aw_spend, p_cnt, s_cnt):
    """KPI 카드"""
    spend_total = 0
    revenue_total = 0
    clicks = 0
    impressions = 0
    orders = 0

    if p_cnt > 0:
        kpi = query_df(f"""
            SELECT
                COALESCE(SUM(ap.ad_spend), 0) as spend,
                COALESCE(SUM(ap.total_revenue), 0) as revenue,
                COALESCE(SUM(ap.clicks), 0) as clicks,
                COALESCE(SUM(ap.impressions), 0) as impressions,
                COALESCE(SUM(ap.total_orders), 0) as orders
            FROM ad_performances ap
            WHERE ap.ad_date BETWEEN '{d_from}' AND '{d_to}' {aw_perf}
        """)
        if not kpi.empty:
            k = kpi.iloc[0]
            spend_total = int(k["spend"])
            revenue_total = int(k["revenue"])
            clicks = int(k["clicks"])
            impressions = int(k["impressions"])
            orders = int(k["orders"])

    # 정산 데이터가 있으면 광고비는 정산 기준 사용 (더 정확)
    settle_total = 0
    if s_cnt > 0:
        settle = query_df(f"""
            SELECT COALESCE(SUM(ad.total_charge), 0) as total
            FROM ad_spends ad
            WHERE ad.ad_date BETWEEN '{d_from}' AND '{d_to}' {aw_spend}
        """)
        if not settle.empty:
            settle_total = int(settle.iloc[0]["total"])

    ad_cost = settle_total if settle_total > 0 else spend_total
    roas = round(revenue_total / ad_cost * 100) if ad_cost > 0 else 0
    ctr = round(clicks / impressions * 100, 2) if impressions > 0 else 0
    cpc = round(ad_cost / clicks) if clicks > 0 else 0

    k1, k2, k3, k4, k5, k6 = st.columns(6)
    k1.metric("광고비", fmt_krw(ad_cost))
    k2.metric("전환매출", fmt_krw(revenue_total))
    k3.metric("ROAS", f"{roas}%")
    k4.metric("전환주문", f"{orders:,}건")
    k5.metric("클릭수", f"{clicks:,}")
    k6.metric("CTR", f"{ctr}%")

    st.caption(f"{d_from} ~ {d_to}  |  CPC 평균: {cpc:,}원")


def _render_daily_chart(d_from, d_to, aw_perf):
    """일별 광고비 vs 전환매출 차트"""
    daily = query_df(f"""
        SELECT ap.ad_date as 날짜,
            SUM(ap.ad_spend) as 광고비,
            SUM(ap.total_revenue) as 전환매출,
            SUM(ap.clicks) as 클릭수,
            SUM(ap.total_orders) as 주문수
        FROM ad_performances ap
        WHERE ap.ad_date BETWEEN '{d_from}' AND '{d_to}' {aw_perf}
        GROUP BY ap.ad_date ORDER BY ap.ad_date
    """)
    if daily.empty:
        return

    daily["날짜"] = pd.to_datetime(daily["날짜"])

    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(
        go.Bar(x=daily["날짜"], y=daily["광고비"], name="광고비", marker_color="#ff6b6b", opacity=0.7),
        secondary_y=False,
    )
    fig.add_trace(
        go.Bar(x=daily["날짜"], y=daily["전환매출"], name="전환매출", marker_color="#4dabf7", opacity=0.7),
        secondary_y=False,
    )
    roas_daily = (daily["전환매출"] / daily["광고비"].replace(0, pd.NA) * 100).fillna(0)
    fig.add_trace(
        go.Scatter(x=daily["날짜"], y=roas_daily, name="ROAS(%)", line=dict(color="#51cf66", width=2)),
        secondary_y=True,
    )
    fig.update_layout(
        title="일별 광고비 vs 전환매출",
        barmode="group",
        height=350,
        margin=dict(t=40, b=10, l=10, r=10),
        legend=dict(orientation="h", y=1.12),
    )
    fig.update_yaxes(title_text="금액", secondary_y=False)
    fig.update_yaxes(title_text="ROAS(%)", secondary_y=True)

    st.plotly_chart(fig, use_container_width=True)


def _render_account_compare(d_from, d_to, p_cnt, s_cnt):
    """계정별 비교"""
    if p_cnt > 0:
        acct_perf = query_df(f"""
            SELECT a.account_name as 계정,
                SUM(ap.impressions) as 노출수,
                SUM(ap.clicks) as 클릭수,
                SUM(ap.ad_spend) as 광고비,
                SUM(ap.total_orders) as 전환주문,
                SUM(ap.total_revenue) as 전환매출,
                ROUND(SUM(ap.total_revenue) * 100.0 / NULLIF(SUM(ap.ad_spend), 0), 0) as "ROAS(%)",
                ROUND(SUM(ap.clicks) * 100.0 / NULLIF(SUM(ap.impressions), 0), 2) as "CTR(%)"
            FROM ad_performances ap
            JOIN accounts a ON ap.account_id = a.id
            WHERE ap.ad_date BETWEEN '{d_from}' AND '{d_to}'
            GROUP BY ap.account_id, a.account_name ORDER BY 광고비 DESC
        """)
        if not acct_perf.empty:
            st.subheader("계정별 광고 성과")

            # 차트 + 테이블
            chart_col, pie_col = st.columns([3, 2])
            with chart_col:
                st.bar_chart(acct_perf.set_index("계정")[["광고비", "전환매출"]])
            with pie_col:
                import plotly.express as px
                _pie = acct_perf[acct_perf["광고비"] > 0]
                if not _pie.empty:
                    fig = px.pie(_pie, values="광고비", names="계정", title="광고비 비중",
                                 hole=0.4, color_discrete_sequence=px.colors.qualitative.Set2)
                    fig.update_layout(margin=dict(t=40, b=10, l=10, r=10), height=300, showlegend=True)
                    st.plotly_chart(fig, use_container_width=True)

            st.dataframe(fmt_money_df(acct_perf), hide_index=True, use_container_width=True)

    if s_cnt > 0:
        acct_spend = query_df(f"""
            SELECT a.account_name as 계정,
                COUNT(DISTINCT ad.campaign_id) as 캠페인수,
                SUM(ad.spent_amount) as 소진액,
                SUM(ad.total_charge) as 총비용
            FROM ad_spends ad
            JOIN accounts a ON ad.account_id = a.id
            WHERE ad.ad_date BETWEEN '{d_from}' AND '{d_to}'
            GROUP BY ad.account_id, a.account_name ORDER BY 총비용 DESC
        """)
        if not acct_spend.empty:
            st.subheader("계정별 광고비 정산")
            st.dataframe(fmt_money_df(acct_spend), hide_index=True, use_container_width=True)


def _render_campaign_table(d_from, d_to, aw_perf, p_cnt):
    """캠페인별 성과"""
    if p_cnt == 0:
        st.info("광고 성과 데이터가 없습니다.")
        return

    campaigns = query_df(f"""
        SELECT ap.campaign_name, ap.campaign_id,
            SUM(ap.impressions) as 노출수,
            SUM(ap.clicks) as 클릭수,
            ROUND(SUM(ap.clicks) * 100.0 / NULLIF(SUM(ap.impressions), 0), 2) as "CTR(%)",
            SUM(ap.ad_spend) as 광고비,
            SUM(ap.total_orders) as 전환주문,
            SUM(ap.total_revenue) as 전환매출,
            ROUND(SUM(ap.total_revenue) * 100.0 / NULLIF(SUM(ap.ad_spend), 0), 0) as "ROAS(%)"
        FROM ad_performances ap
        WHERE ap.ad_date BETWEEN '{d_from}' AND '{d_to}' {aw_perf}
            AND ap.campaign_name != ''
        GROUP BY ap.campaign_name, ap.campaign_id ORDER BY 광고비 DESC
        LIMIT 30
    """)
    if not campaigns.empty:
        campaigns["캠페인"] = campaigns.apply(
            lambda r: f"{r['campaign_name']} ({r['campaign_id']})" if r["campaign_id"] else r["campaign_name"], axis=1)
        campaigns = campaigns.drop(columns=["campaign_name", "campaign_id"])
        cols = ["캠페인"] + [c for c in campaigns.columns if c != "캠페인"]
        campaigns = campaigns[cols]
    if not campaigns.empty:
        st.dataframe(fmt_money_df(campaigns), hide_index=True, use_container_width=True)
        xl = _df_to_excel_bytes(campaigns, "캠페인별")
        st.download_button("Excel 다운로드", xl, "campaigns.xlsx",
                           "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                           key="dl_campaigns")
    else:
        st.info("캠페인 데이터가 없습니다.")


def _render_product_top(d_from, d_to, aw_perf, p_cnt):
    """상품별 TOP 성과"""
    if p_cnt == 0:
        st.info("광고 성과 데이터가 없습니다.")
        return

    products = query_df(f"""
        SELECT ap.campaign_name, ap.campaign_id, ap.product_name as 상품명,
            SUM(ap.impressions) as 노출수,
            SUM(ap.clicks) as 클릭수,
            SUM(ap.ad_spend) as 광고비,
            SUM(ap.total_orders) as 전환주문,
            SUM(ap.total_revenue) as 전환매출,
            ROUND(SUM(ap.total_revenue) * 100.0 / NULLIF(SUM(ap.ad_spend), 0), 0) as "ROAS(%)"
        FROM ad_performances ap
        WHERE ap.ad_date BETWEEN '{d_from}' AND '{d_to}' {aw_perf}
            AND ap.product_name != ''
        GROUP BY ap.campaign_name, ap.campaign_id, ap.coupang_product_id, ap.product_name ORDER BY 전환매출 DESC
        LIMIT 20
    """)
    if not products.empty:
        products["캠페인"] = products.apply(
            lambda r: f"{r['campaign_name']} ({r['campaign_id']})" if r["campaign_id"] else r["campaign_name"], axis=1)
        products = products.drop(columns=["campaign_name", "campaign_id"])
        cols = ["캠페인"] + [c for c in products.columns if c != "캠페인"]
        products = products[cols]
    if not products.empty:
        products.insert(0, "#", range(1, len(products) + 1))
        st.dataframe(fmt_money_df(products), hide_index=True, use_container_width=True)
        xl = _df_to_excel_bytes(products, "상품별TOP")
        st.download_button("Excel 다운로드", xl, "products_ad.xlsx",
                           "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                           key="dl_products_ad")
    else:
        st.info("상품 데이터가 없습니다.")


def _render_keyword_table(d_from, d_to, aw_perf, p_cnt):
    """키워드별 성과 (개별 계정 선택 시)"""
    if p_cnt == 0:
        st.info("광고 성과 데이터가 없습니다.")
        return

    keywords = query_df(f"""
        SELECT ap.campaign_name, ap.campaign_id,
            ap.keyword as 키워드,
            ap.match_type as 매치유형,
            SUM(ap.impressions) as 노출수,
            SUM(ap.clicks) as 클릭수,
            ROUND(SUM(ap.clicks) * 100.0 / NULLIF(SUM(ap.impressions), 0), 2) as "CTR(%)",
            SUM(ap.ad_spend) as 광고비,
            SUM(ap.total_orders) as 전환주문,
            SUM(ap.total_revenue) as 전환매출,
            ROUND(SUM(ap.total_revenue) * 100.0 / NULLIF(SUM(ap.ad_spend), 0), 0) as "ROAS(%)"
        FROM ad_performances ap
        WHERE ap.ad_date BETWEEN '{d_from}' AND '{d_to}' {aw_perf}
            AND ap.keyword != ''
        GROUP BY ap.campaign_name, ap.campaign_id, ap.keyword, ap.match_type ORDER BY 광고비 DESC
        LIMIT 30
    """)
    if not keywords.empty:
        keywords["캠페인"] = keywords.apply(
            lambda r: f"{r['campaign_name']} ({r['campaign_id']})" if r["campaign_id"] else r["campaign_name"], axis=1)
        keywords = keywords.drop(columns=["campaign_name", "campaign_id"])
        cols = ["캠페인"] + [c for c in keywords.columns if c != "캠페인"]
        keywords = keywords[cols]
    if not keywords.empty:
        st.dataframe(fmt_money_df(keywords), hide_index=True, use_container_width=True)
        xl = _df_to_excel_bytes(keywords, "키워드별")
        st.download_button("Excel 다운로드", xl, "keywords.xlsx",
                           "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                           key="dl_keywords")
    else:
        st.info("키워드 데이터가 없습니다.")


# ═════════════════════════════════════════
# Excel 유틸리티
# ═════════════════════════════════════════

def _df_to_excel_bytes(df: pd.DataFrame, sheet_name: str = "Sheet1") -> bytes:
    """DataFrame -> Excel bytes (단일 시트)"""
    from io import BytesIO
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side

    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False, startrow=1)
        ws = writer.sheets[sheet_name]
        _style_excel_header(ws, len(df.columns), len(df), sheet_name)
    return buf.getvalue()


def _style_excel_header(ws, num_cols: int, num_rows: int, title: str = ""):
    """Excel 시트 헤더 스타일링 (export_order_sheets.py 패턴)"""
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side

    header_fill = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
    header_font = Font(bold=True, color="FFFFFF", size=10)
    thin_border = Border(
        left=Side(style="thin"), right=Side(style="thin"),
        top=Side(style="thin"), bottom=Side(style="thin"),
    )

    # 타이틀 행
    if title:
        ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=max(num_cols, 1))
        cell = ws.cell(row=1, column=1)
        cell.value = title
        cell.font = Font(bold=True, size=13)
        cell.alignment = Alignment(horizontal="center")

    # 헤더 행 (row 2)
    for ci in range(1, num_cols + 1):
        c = ws.cell(row=2, column=ci)
        c.fill = header_fill
        c.font = header_font
        c.alignment = Alignment(horizontal="center")
        c.border = thin_border

    # 데이터 행 테두리
    for ri in range(3, 3 + num_rows):
        for ci in range(1, num_cols + 1):
            ws.cell(row=ri, column=ci).border = thin_border

    # 열 너비 자동 조정 (최소 12, 최대 40)
    for ci in range(1, num_cols + 1):
        header_val = str(ws.cell(row=2, column=ci).value or "")
        max_len = max(len(header_val) * 2, 12)  # 한글 2배
        for ri in range(3, min(3 + num_rows, 53)):  # 최대 50행 샘플
            val = str(ws.cell(row=ri, column=ci).value or "")
            max_len = max(max_len, min(len(val) + 2, 40))
        from openpyxl.utils import get_column_letter
        col_letter = get_column_letter(ci)
        ws.column_dimensions[col_letter].width = min(max_len, 40)


# ═════════════════════════════════════════
# 효율 리포트
# ═════════════════════════════════════════

def _render_efficiency_report(d_from, d_to, aw_perf, p_cnt):
    """광고 효율 리포트 -- 비효율 키워드/상품/캠페인 분석 + Excel 다운로드"""
    if p_cnt == 0:
        st.info("광고 성과 데이터가 없습니다.")
        return

    # ── 총 광고비 ──
    total_kpi = query_df(f"""
        SELECT COALESCE(SUM(ap.ad_spend), 0) as total_spend
        FROM ad_performances ap
        WHERE ap.ad_date BETWEEN '{d_from}' AND '{d_to}' {aw_perf}
    """)
    total_spend = int(total_kpi.iloc[0]["total_spend"]) if not total_kpi.empty else 0

    if total_spend == 0:
        st.info("선택한 기간에 광고비 데이터가 없습니다.")
        return

    # ── 키워드별 집계 ──
    kw_df = query_df(f"""
        SELECT ap.campaign_name, ap.campaign_id,
            ap.keyword as 키워드, ap.match_type as 매치유형,
            SUM(ap.impressions) as 노출수,
            SUM(ap.clicks) as 클릭수,
            SUM(ap.ad_spend) as 광고비,
            SUM(ap.total_orders) as 전환주문,
            SUM(ap.total_revenue) as 전환매출,
            ROUND(SUM(ap.ad_spend) * 1.0 / NULLIF(SUM(ap.clicks), 0), 0) as "CPC",
            ROUND(SUM(ap.total_revenue) * 100.0 / NULLIF(SUM(ap.ad_spend), 0), 0) as "ROAS(%)"
        FROM ad_performances ap
        WHERE ap.ad_date BETWEEN '{d_from}' AND '{d_to}' {aw_perf}
            AND ap.keyword != '' AND ap.keyword != '-'
        GROUP BY ap.campaign_name, ap.campaign_id, ap.keyword, ap.match_type
        HAVING SUM(ap.ad_spend) > 0
        ORDER BY 광고비 DESC
    """)
    if not kw_df.empty:
        kw_df["캠페인"] = kw_df.apply(
            lambda r: f"{r['campaign_name']} ({r['campaign_id']})" if r["campaign_id"] else r["campaign_name"], axis=1)
        kw_df = kw_df.drop(columns=["campaign_name", "campaign_id"])
        cols = ["캠페인"] + [c for c in kw_df.columns if c != "캠페인"]
        kw_df = kw_df[cols]

    # ── 상품별 집계 ──
    prod_df = query_df(f"""
        SELECT ap.campaign_name, ap.campaign_id, ap.product_name as 상품명,
            SUM(ap.impressions) as 노출수,
            SUM(ap.clicks) as 클릭수,
            SUM(ap.ad_spend) as 광고비,
            SUM(ap.total_orders) as 전환주문,
            SUM(ap.total_revenue) as 전환매출,
            ROUND(SUM(ap.total_revenue) * 100.0 / NULLIF(SUM(ap.ad_spend), 0), 0) as "ROAS(%)"
        FROM ad_performances ap
        WHERE ap.ad_date BETWEEN '{d_from}' AND '{d_to}' {aw_perf}
            AND ap.product_name != ''
        GROUP BY ap.campaign_name, ap.campaign_id, ap.coupang_product_id, ap.product_name
        HAVING SUM(ap.ad_spend) > 0
        ORDER BY 광고비 DESC
    """)
    if not prod_df.empty:
        prod_df["캠페인"] = prod_df.apply(
            lambda r: f"{r['campaign_name']} ({r['campaign_id']})" if r["campaign_id"] else r["campaign_name"], axis=1)
        prod_df = prod_df.drop(columns=["campaign_name", "campaign_id"])
        cols = ["캠페인"] + [c for c in prod_df.columns if c != "캠페인"]
        prod_df = prod_df[cols]

    # ── 캠페인별 집계 ──
    camp_df = query_df(f"""
        SELECT ap.campaign_name, ap.campaign_id,
            SUM(ap.impressions) as 노출수,
            SUM(ap.clicks) as 클릭수,
            SUM(ap.ad_spend) as 광고비,
            SUM(ap.total_orders) as 전환주문,
            SUM(ap.total_revenue) as 전환매출,
            ROUND(SUM(ap.total_revenue) * 100.0 / NULLIF(SUM(ap.ad_spend), 0), 0) as "ROAS(%)"
        FROM ad_performances ap
        WHERE ap.ad_date BETWEEN '{d_from}' AND '{d_to}' {aw_perf}
            AND ap.campaign_name != ''
        GROUP BY ap.campaign_name, ap.campaign_id
        HAVING SUM(ap.ad_spend) > 0
        ORDER BY 광고비 DESC
    """)
    if not camp_df.empty:
        camp_df["캠페인"] = camp_df.apply(
            lambda r: f"{r['campaign_name']} ({r['campaign_id']})" if r["campaign_id"] else r["campaign_name"], axis=1)
        camp_df = camp_df.drop(columns=["campaign_name", "campaign_id"])
        cols = ["캠페인"] + [c for c in camp_df.columns if c != "캠페인"]
        camp_df = camp_df[cols]

    # ── 캠페인 필터 ──
    all_campaigns = sorted(set(
        (kw_df["캠페인"].unique().tolist() if not kw_df.empty else [])
        + (prod_df["캠페인"].unique().tolist() if not prod_df.empty else [])
        + (camp_df["캠페인"].unique().tolist() if not camp_df.empty else [])
    ))
    camp_filter = st.selectbox(
        "캠페인 필터", ["전체"] + all_campaigns, key="eff_camp_filter")

    if camp_filter != "전체":
        kw_df = kw_df[kw_df["캠페인"] == camp_filter] if not kw_df.empty else kw_df
        prod_df = prod_df[prod_df["캠페인"] == camp_filter] if not prod_df.empty else prod_df
        camp_df = camp_df[camp_df["캠페인"] == camp_filter] if not camp_df.empty else camp_df
        # 선택 캠페인 기준 총 광고비 재계산
        total_spend = int(kw_df["광고비"].sum()) + int(prod_df[~prod_df["상품명"].isin([""])]["광고비"].sum()) if not kw_df.empty else 0
        if not camp_df.empty:
            total_spend = int(camp_df["광고비"].sum())

    st.divider()

    # ── 분류 ──
    zero_conv_kw = kw_df[kw_df["전환주문"] == 0] if not kw_df.empty else pd.DataFrame()
    low_roas_kw = kw_df[(kw_df["전환주문"] > 0) & (kw_df["ROAS(%)"].fillna(0) < 200)] if not kw_df.empty else pd.DataFrame()
    good_kw = kw_df[(kw_df["전환주문"] > 0) & (kw_df["ROAS(%)"].fillna(0) >= 200)].sort_values("전환매출", ascending=False) if not kw_df.empty else pd.DataFrame()

    zero_conv_prod = prod_df[prod_df["전환주문"] == 0] if not prod_df.empty else pd.DataFrame()
    good_prod = prod_df[(prod_df["전환주문"] > 0) & (prod_df["ROAS(%)"].fillna(0) >= 200)].sort_values("전환매출", ascending=False) if not prod_df.empty else pd.DataFrame()

    # ── (A) 상단 KPI ──
    wasted_spend = int(zero_conv_kw["광고비"].sum()) if not zero_conv_kw.empty else 0
    waste_pct = round(wasted_spend / total_spend * 100, 1) if total_spend > 0 else 0
    efficient_kw_count = len(good_kw)

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("총 광고비", fmt_krw(total_spend))
    k2.metric("낭비 광고비 (전환 0건)", fmt_krw(wasted_spend))
    k3.metric("낭비 비율", f"{waste_pct}%")
    k4.metric("효율 키워드 수", f"{efficient_kw_count}개")

    st.divider()

    # ── (B) 키워드 비효율 분석 ──
    st.subheader("키워드 분석")

    if not kw_df.empty:
        if not zero_conv_kw.empty:
            st.markdown(f"**전환 0건 키워드** ({len(zero_conv_kw)}개, 낭비 {fmt_krw(wasted_spend)})")
            st.dataframe(fmt_money_df(zero_conv_kw.head(30)), hide_index=True, use_container_width=True)

        if not low_roas_kw.empty:
            low_roas_spend = int(low_roas_kw["광고비"].sum())
            st.markdown(f"**ROAS 200% 미만 -- 적자 키워드** ({len(low_roas_kw)}개, 광고비 {fmt_krw(low_roas_spend)})")
            st.dataframe(fmt_money_df(low_roas_kw), hide_index=True, use_container_width=True)

        if not good_kw.empty:
            st.markdown(f"**효율 좋은 키워드 (ROAS >= 200%)** ({len(good_kw)}개)")
            st.dataframe(fmt_money_df(good_kw), hide_index=True, use_container_width=True)
    else:
        st.info("키워드 데이터가 없습니다.")

    st.divider()

    # ── (C) 상품 비효율 분석 ──
    st.subheader("상품 분석")

    if not prod_df.empty:
        if not zero_conv_prod.empty:
            wasted_prod = int(zero_conv_prod["광고비"].sum())
            st.markdown(f"**전환 0건 상품** ({len(zero_conv_prod)}개, 낭비 {fmt_krw(wasted_prod)})")
            st.dataframe(fmt_money_df(zero_conv_prod), hide_index=True, use_container_width=True)

        if not good_prod.empty:
            st.markdown(f"**효율 좋은 상품 (ROAS >= 200%)** ({len(good_prod)}개)")
            st.dataframe(fmt_money_df(good_prod), hide_index=True, use_container_width=True)
    else:
        st.info("상품 데이터가 없습니다.")

    st.divider()

    # ── (D) 캠페인 비효율 분석 ──
    st.subheader("캠페인 ROAS 비교")

    if not camp_df.empty:
        st.dataframe(fmt_money_df(camp_df), hide_index=True, use_container_width=True)
    else:
        st.info("캠페인 데이터가 없습니다.")

    # ── (E) 개선 제안 ──
    st.divider()
    st.subheader("개선 제안")

    recs_df = _generate_recommendations(kw_df, prod_df, camp_df)

    if not recs_df.empty:
        # 요약 카드
        n_stop = len(recs_df[recs_df["조치"].str.contains("중지")])
        n_adjust = len(recs_df[recs_df["조치"].str.contains("하향|점검|정리", regex=True)])
        n_expand = len(recs_df[recs_df["조치"].str.contains("확대|상향", regex=True)])

        c1, c2, c3 = st.columns(3)
        c1.metric("즉시 중지 권장", f"{n_stop}건")
        c2.metric("입찰가/키워드 조정", f"{n_adjust}건")
        c3.metric("예산 확대 검토", f"{n_expand}건")

        # 우선순위 아이콘 매핑
        display_df = recs_df.copy()
        icon_map = {"높음": "[!] 높음", "중간": "[~] 중간", "낮음": "[o] 낮음"}
        display_df["우선순위"] = display_df["우선순위"].map(icon_map)

        st.dataframe(
            fmt_money_df(display_df[["대상유형", "캠페인", "이름", "ROAS(%)", "광고비", "조치", "우선순위", "사유"]]),
            hide_index=True,
            use_container_width=True,
        )
    else:
        st.info("조치가 필요한 항목이 없습니다.")

    st.divider()

    # ── (F) Excel 다운로드 ──
    xl_bytes = _create_efficiency_excel(
        total_spend, wasted_spend, waste_pct, efficient_kw_count,
        zero_conv_kw, good_kw, zero_conv_prod, good_prod, camp_df,
        recs_df, d_from, d_to,
    )
    st.download_button(
        "효율 리포트 Excel 다운로드",
        xl_bytes,
        f"광고_효율_리포트_{d_from}_{d_to}.xlsx",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="dl_efficiency_excel",
        type="primary",
        use_container_width=True,
    )


def _create_efficiency_excel(total_spend, wasted_spend, waste_pct, efficient_kw_count,
                              zero_conv_kw, good_kw, zero_conv_prod, good_prod,
                              camp_df, recs_df, d_from, d_to):
    """효율 리포트 멀티시트 Excel 생성"""
    from io import BytesIO

    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        period_label = f"{d_from} ~ {d_to}"

        # Sheet 1: 요약
        summary = pd.DataFrame([
            {"항목": "총 광고비", "값": f"{total_spend:,}원"},
            {"항목": "낭비 광고비 (전환 0건 키워드)", "값": f"{wasted_spend:,}원"},
            {"항목": "낭비 비율", "값": f"{waste_pct}%"},
            {"항목": "효율 키워드 수 (ROAS>=200%)", "값": f"{efficient_kw_count}개"},
            {"항목": "분석 기간", "값": period_label},
        ])
        summary.to_excel(writer, sheet_name="요약", index=False, startrow=1)
        ws = writer.sheets["요약"]
        _style_excel_header(ws, 2, len(summary), f"광고 효율 요약 ({period_label})")
        ws.column_dimensions["A"].width = 30
        ws.column_dimensions["B"].width = 20

        # Sheet 2: 비효율 키워드
        if not zero_conv_kw.empty:
            zero_conv_kw.to_excel(writer, sheet_name="비효율 키워드", index=False, startrow=1)
            ws = writer.sheets["비효율 키워드"]
            _style_excel_header(ws, len(zero_conv_kw.columns), len(zero_conv_kw),
                                f"전환 0건 키워드 ({period_label})")
        else:
            pd.DataFrame({"메시지": ["전환 0건 키워드 없음"]}).to_excel(
                writer, sheet_name="비효율 키워드", index=False)

        # Sheet 3: 효율 키워드
        if not good_kw.empty:
            good_kw.to_excel(writer, sheet_name="효율 키워드", index=False, startrow=1)
            ws = writer.sheets["효율 키워드"]
            _style_excel_header(ws, len(good_kw.columns), len(good_kw),
                                f"효율 키워드 ROAS>=200% ({period_label})")
        else:
            pd.DataFrame({"메시지": ["ROAS>=200% 키워드 없음"]}).to_excel(
                writer, sheet_name="효율 키워드", index=False)

        # Sheet 4: 비효율 상품
        if not zero_conv_prod.empty:
            zero_conv_prod.to_excel(writer, sheet_name="비효율 상품", index=False, startrow=1)
            ws = writer.sheets["비효율 상품"]
            _style_excel_header(ws, len(zero_conv_prod.columns), len(zero_conv_prod),
                                f"전환 0건 상품 ({period_label})")
        else:
            pd.DataFrame({"메시지": ["전환 0건 상품 없음"]}).to_excel(
                writer, sheet_name="비효율 상품", index=False)

        # Sheet 5: 효율 상품
        if not good_prod.empty:
            good_prod.to_excel(writer, sheet_name="효율 상품", index=False, startrow=1)
            ws = writer.sheets["효율 상품"]
            _style_excel_header(ws, len(good_prod.columns), len(good_prod),
                                f"효율 상품 ROAS>=200% ({period_label})")
        else:
            pd.DataFrame({"메시지": ["ROAS>=200% 상품 없음"]}).to_excel(
                writer, sheet_name="효율 상품", index=False)

        # Sheet 6: 캠페인 비교
        if not camp_df.empty:
            camp_df.to_excel(writer, sheet_name="캠페인 비교", index=False, startrow=1)
            ws = writer.sheets["캠페인 비교"]
            _style_excel_header(ws, len(camp_df.columns), len(camp_df),
                                f"캠페인별 성과 ({period_label})")
        else:
            pd.DataFrame({"메시지": ["캠페인 데이터 없음"]}).to_excel(
                writer, sheet_name="캠페인 비교", index=False)

        # Sheet 7: 개선 제안
        if recs_df is not None and not recs_df.empty:
            recs_df.to_excel(writer, sheet_name="개선 제안", index=False, startrow=1)
            ws = writer.sheets["개선 제안"]
            _style_excel_header(ws, len(recs_df.columns), len(recs_df),
                                f"개선 제안 ({period_label})")
        else:
            pd.DataFrame({"메시지": ["조치가 필요한 항목 없음"]}).to_excel(
                writer, sheet_name="개선 제안", index=False)

    return buf.getvalue()


def _generate_recommendations(kw_df, prod_df, camp_df):
    """키워드/상품/캠페인별 조치 권장 목록 생성"""
    recs = []

    # ── 키워드 조치 ──
    if not kw_df.empty:
        for _, r in kw_df.iterrows():
            conv = int(r.get("전환주문", 0) or 0)
            spend = int(r.get("광고비", 0) or 0)
            roas = float(r.get("ROAS(%)", 0) or 0)
            clicks = int(r.get("클릭수", 0) or 0)
            impressions = int(r.get("노출수", 0) or 0)
            ctr = (clicks / impressions * 100) if impressions > 0 else 0
            name = r.get("키워드", "")
            campaign = r.get("캠페인", "")

            if conv == 0 and ctr >= 5:
                recs.append({"대상유형": "키워드", "캠페인": campaign, "이름": name,
                             "ROAS(%)": roas, "광고비": spend,
                             "조치": "상품페이지 점검", "우선순위": "높음",
                             "사유": f"CTR {ctr:.1f}%로 높으나 전환 0건"})
            elif conv == 0 and spend >= 5000:
                recs.append({"대상유형": "키워드", "캠페인": campaign, "이름": name,
                             "ROAS(%)": roas, "광고비": spend,
                             "조치": "키워드 중지", "우선순위": "높음",
                             "사유": f"전환 0건, 광고비 {spend:,}원 소진"})
            elif conv == 0 and spend < 5000:
                recs.append({"대상유형": "키워드", "캠페인": campaign, "이름": name,
                             "ROAS(%)": roas, "광고비": spend,
                             "조치": "모니터링", "우선순위": "낮음",
                             "사유": "전환 0건, 데이터 부족"})
            elif roas < 100:
                recs.append({"대상유형": "키워드", "캠페인": campaign, "이름": name,
                             "ROAS(%)": roas, "광고비": spend,
                             "조치": "입찰가 하향 또는 중지", "우선순위": "높음",
                             "사유": f"ROAS {roas:.0f}% 적자"})
            elif roas < 200:
                recs.append({"대상유형": "키워드", "캠페인": campaign, "이름": name,
                             "ROAS(%)": roas, "광고비": spend,
                             "조치": "입찰가 하향 검토", "우선순위": "중간",
                             "사유": f"ROAS {roas:.0f}% 저효율"})
            elif roas >= 500 and clicks >= 10:
                recs.append({"대상유형": "키워드", "캠페인": campaign, "이름": name,
                             "ROAS(%)": roas, "광고비": spend,
                             "조치": "입찰가 상향 검토", "우선순위": "중간",
                             "사유": f"ROAS {roas:.0f}%, 클릭 {clicks}회 -- 확대 여지"})

    # ── 상품 조치 ──
    if not prod_df.empty:
        for _, r in prod_df.iterrows():
            conv = int(r.get("전환주문", 0) or 0)
            spend = int(r.get("광고비", 0) or 0)
            roas = float(r.get("ROAS(%)", 0) or 0)
            name = r.get("상품명", "")
            campaign = r.get("캠페인", "")

            if conv == 0 and spend >= 10000:
                recs.append({"대상유형": "상품", "캠페인": campaign, "이름": name,
                             "ROAS(%)": roas, "광고비": spend,
                             "조치": "광고 중지", "우선순위": "높음",
                             "사유": f"전환 0건, 광고비 {spend:,}원 소진"})
            elif conv > 0 and roas < 100:
                recs.append({"대상유형": "상품", "캠페인": campaign, "이름": name,
                             "ROAS(%)": roas, "광고비": spend,
                             "조치": "예산 축소 또는 중지", "우선순위": "높음",
                             "사유": f"ROAS {roas:.0f}% 적자"})
            elif conv > 0 and roas >= 500:
                recs.append({"대상유형": "상품", "캠페인": campaign, "이름": name,
                             "ROAS(%)": roas, "광고비": spend,
                             "조치": "예산 확대 검토", "우선순위": "중간",
                             "사유": f"ROAS {roas:.0f}% 고효율"})

    # ── 캠페인 조치 ──
    if not camp_df.empty:
        for _, r in camp_df.iterrows():
            roas = float(r.get("ROAS(%)", 0) or 0)
            spend = int(r.get("광고비", 0) or 0)
            name = r.get("캠페인", "")

            if roas < 100:
                recs.append({"대상유형": "캠페인", "캠페인": name, "이름": name,
                             "ROAS(%)": roas, "광고비": spend,
                             "조치": "캠페인 예산 축소", "우선순위": "높음",
                             "사유": f"ROAS {roas:.0f}% 적자"})
            elif roas < 200:
                recs.append({"대상유형": "캠페인", "캠페인": name, "이름": name,
                             "ROAS(%)": roas, "광고비": spend,
                             "조치": "키워드 정리 필요", "우선순위": "중간",
                             "사유": f"ROAS {roas:.0f}% 저효율"})
            elif roas >= 300:
                recs.append({"대상유형": "캠페인", "캠페인": name, "이름": name,
                             "ROAS(%)": roas, "광고비": spend,
                             "조치": "예산 확대 검토", "우선순위": "중간",
                             "사유": f"ROAS {roas:.0f}% 고효율"})

    if not recs:
        return pd.DataFrame()

    recs_df = pd.DataFrame(recs)
    # 우선순위 정렬: 높음 -> 중간 -> 낮음, 같은 순위 내 광고비 내림차순
    priority_order = {"높음": 0, "중간": 1, "낮음": 2}
    recs_df["_sort"] = recs_df["우선순위"].map(priority_order)
    recs_df = (recs_df.sort_values(["_sort", "광고비"], ascending=[True, False])
               .drop(columns="_sort").reset_index(drop=True))
    return recs_df
