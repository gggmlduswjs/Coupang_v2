"""위닝 모니터링 — 계정별 바이박스 비위닝 현황"""
import glob
import json
import logging
import pandas as pd
import streamlit as st

from dashboard.utils import query_df

logger = logging.getLogger(__name__)

NON_WINNER_PATTERN = r'C:\Users\MSI\Desktop\*\data\non_winner_*.json'


def _load_non_winner_data() -> dict[str, list[str]]:
    """non_winner_*.json 파일 로드 → {account_name: [spid, ...]}"""
    files = glob.glob(NON_WINNER_PATTERN)
    result = {}
    for f in files:
        try:
            with open(f, encoding='utf-8') as fp:
                data = json.load(fp)
            acct = data.get('account', '')
            spids = [str(s) for s in data.get('spids', [])]
            if acct and spids:
                result[acct] = spids
        except Exception as e:
            logger.warning(f"파일 로드 실패 {f}: {e}")
    return result


def render(selected_account, accounts_df, account_names):
    st.title("위닝 모니터링")

    non_winner_data = _load_non_winner_data()
    if not non_winner_data:
        st.warning("non_winner_*.json 캐시 파일을 찾을 수 없습니다.")
        st.caption(f"경로: {NON_WINNER_PATTERN}")
        return

    # 파일 타임스탬프 표시
    files = glob.glob(NON_WINNER_PATTERN)
    if files:
        import os
        ts = max(os.path.getmtime(f) for f in files)
        from datetime import datetime
        st.caption(f"캐시 기준: {datetime.fromtimestamp(ts):%Y-%m-%d %H:%M}")

    st.divider()

    # ── 계정별 KPI ──
    kpi_cols = st.columns(len(non_winner_data))
    for i, (acct, spids) in enumerate(sorted(non_winner_data.items())):
        # 해당 계정 총 active 수
        acct_row = accounts_df[accounts_df["account_name"] == acct]
        if acct_row.empty:
            continue
        acct_id = int(acct_row.iloc[0]["id"])
        total_row = query_df(
            "SELECT COUNT(*) as cnt FROM listings WHERE account_id=:aid AND coupang_status='active'",
            {"aid": acct_id}
        )
        total = int(total_row.iloc[0]["cnt"]) if not total_row.empty else 1
        nw_cnt = len(spids)
        win_cnt = total - nw_cnt
        win_pct = win_cnt / total * 100 if total else 0
        kpi_cols[i].metric(
            acct,
            f"위닝 {win_pct:.0f}%",
            delta=f"비위닝 {nw_cnt}건",
            delta_color="inverse",
        )

    st.divider()

    # ── 계정 선택 → 비위닝 상품 목록 ──
    avail_accts = sorted(non_winner_data.keys())
    sel_acct = st.selectbox("계정 선택", avail_accts, key="win_acct")
    if not sel_acct:
        return

    spids = non_winner_data[sel_acct]
    acct_row = accounts_df[accounts_df["account_name"] == sel_acct]
    if acct_row.empty:
        st.error(f"{sel_acct} 계정 없음")
        return
    acct_id = int(acct_row.iloc[0]["id"])

    if not spids:
        st.success(f"{sel_acct}: 비위닝 상품 없음!")
        return

    # spids → listings 조회
    # PostgreSQL: coupang_product_id IN (...)
    spids_int = []
    for s in spids:
        try:
            spids_int.append(int(s))
        except Exception:
            pass

    if not spids_int:
        st.info("비위닝 상품 데이터 없음")
        return

    # 100개씩 나눠서 조회 (IN 절 길이 제한 방지)
    CHUNK = 200
    dfs = []
    for i in range(0, len(spids_int), CHUNK):
        chunk = tuple(spids_int[i:i+CHUNK])
        chunk_df = query_df(f"""
            SELECT l.product_name as 상품명,
                   l.sale_price as 판매가,
                   l.original_price as 정가,
                   l.brand as 출판사,
                   l.coupang_product_id as 쿠팡ID,
                   l.vendor_item_id as VID,
                   l.isbn as ISBN
            FROM listings l
            WHERE l.account_id = :aid
              AND l.coupang_product_id IN {chunk}
            ORDER BY l.sale_price DESC
        """, {"aid": acct_id})
        if not chunk_df.empty:
            dfs.append(chunk_df)

    if not dfs:
        st.info("비위닝 상품 DB 매칭 결과 없음")
        return

    nw_df = pd.concat(dfs, ignore_index=True)
    st.caption(f"**{sel_acct}** 비위닝 상품: {len(nw_df):,}건 / 캐시 기준 {len(spids)}건")

    # 검색
    search = st.text_input("상품명 검색", key="win_search")
    if search:
        nw_df = nw_df[nw_df["상품명"].str.contains(search, case=False, na=False)]

    st.dataframe(nw_df, use_container_width=True, hide_index=True, height=500)

    csv = nw_df.to_csv(index=False).encode("utf-8-sig")
    st.download_button(
        f"CSV ({len(nw_df)}건)",
        csv,
        f"non_winner_{sel_acct}.csv",
        "text/csv",
    )

    # ── 가격 인하 시뮬레이션 ──
    st.divider()
    st.subheader("가격 인하 시뮬레이션")
    st.caption("비위닝 상품 판매가를 N원 낮추면 순마진이 어떻게 변하는지 확인합니다.")
    sim_col1, sim_col2 = st.columns([1, 3])
    with sim_col1:
        price_cut = st.number_input("인하 금액(원)", value=500, step=100, min_value=100, key="win_cut")
    if not nw_df.empty and "판매가" in nw_df.columns:
        sim_df = nw_df[["상품명", "정가", "판매가"]].copy()
        sim_df["인하후"] = (sim_df["판매가"] - price_cut).clip(lower=0)
        sim_df["정가대비"] = ((sim_df["인하후"] / sim_df["정가"].replace(0, 1)) * 100).round(1).astype(str) + "%"
        st.dataframe(sim_df[["상품명", "정가", "판매가", "인하후", "정가대비"]],
                     use_container_width=True, hide_index=True)
