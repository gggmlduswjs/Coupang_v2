"""갭 분석 — 계정별 미등록 상품 현황"""
import logging
import pandas as pd
import streamlit as st

from dashboard.utils import query_df, query_df_cached

logger = logging.getLogger(__name__)


def render(selected_account, accounts_df, account_names):
    st.title("갭 분석")
    st.caption("전체 계정 기준 ISBN 교집합에서 각 계정이 누락한 상품을 파악합니다.")

    # ── 계정별 갭 KPI ──
    gap_summary = query_df("""
        WITH all_isbns AS (
            SELECT DISTINCT isbn
            FROM listings
            WHERE isbn IS NOT NULL AND coupang_status = 'active'
        ),
        per_account AS (
            SELECT l.isbn, a.id as account_id, a.account_name
            FROM listings l
            JOIN accounts a ON l.account_id = a.id
            WHERE l.isbn IS NOT NULL AND l.coupang_status = 'active' AND a.is_active = true
            GROUP BY l.isbn, a.id, a.account_name
        ),
        accts AS (
            SELECT id, account_name FROM accounts WHERE is_active = true
        )
        SELECT
            accts.account_name,
            COUNT(ai.isbn) as total_isbns,
            COUNT(pa.isbn) as has_count,
            COUNT(ai.isbn) - COUNT(pa.isbn) as gap_count
        FROM accts
        CROSS JOIN all_isbns ai
        LEFT JOIN per_account pa ON pa.isbn = ai.isbn AND pa.account_id = accts.id
        GROUP BY accts.id, accts.account_name
        ORDER BY gap_count DESC
    """)

    if gap_summary.empty:
        st.info("데이터 없음")
        return

    total_isbn = int(gap_summary["total_isbns"].iloc[0])
    st.metric("전체 고유 ISBN 수", f"{total_isbn:,}개")
    st.divider()

    cols = st.columns(len(gap_summary))
    for i, (_, row) in enumerate(gap_summary.iterrows()):
        pct = row["has_count"] / row["total_isbns"] * 100 if row["total_isbns"] else 0
        cols[i].metric(
            row["account_name"],
            f"갭 {int(row['gap_count']):,}건",
            delta=f"{pct:.0f}% 보유",
            delta_color="normal",
        )

    st.divider()

    # ── 드릴다운: 계정 선택 → 미등록 ISBN 목록 ──
    st.subheader("계정별 미등록 상품 상세")
    sel_col1, sel_col2 = st.columns([1, 3])
    with sel_col1:
        target_acct = st.selectbox("조회 계정", account_names, key="gap_target")
    with sel_col2:
        ref_acct = st.selectbox(
            "기준 계정 (상품명 참조용)",
            [a for a in account_names if a != target_acct],
            key="gap_ref",
        )

    if not target_acct or not ref_acct:
        return

    target_id_row = accounts_df[accounts_df["account_name"] == target_acct]
    ref_id_row = accounts_df[accounts_df["account_name"] == ref_acct]
    if target_id_row.empty or ref_id_row.empty:
        return

    target_id = int(target_id_row.iloc[0]["id"])
    ref_id = int(ref_id_row.iloc[0]["id"])

    gap_df = query_df("""
        SELECT
            ref.isbn,
            ref.product_name as 상품명,
            ref.original_price as 정가,
            ref.sale_price as 판매가,
            ref.brand as 출판사
        FROM listings ref
        WHERE ref.account_id = :ref_id
          AND ref.coupang_status = 'active'
          AND ref.isbn IS NOT NULL
          AND ref.isbn NOT IN (
              SELECT isbn FROM listings
              WHERE account_id = :target_id
                AND isbn IS NOT NULL
                AND coupang_status = 'active'
          )
        ORDER BY ref.original_price DESC
        LIMIT 500
    """, {"ref_id": ref_id, "target_id": target_id})

    if gap_df.empty:
        st.success(f"{target_acct}에 {ref_acct} 상품이 모두 등록되어 있습니다!")
        return

    st.caption(f"{ref_acct}에는 있지만 **{target_acct}에 없는 상품: {len(gap_df):,}건** (최대 500건 표시)")

    # 검색 필터
    search = st.text_input("상품명/ISBN 검색", key="gap_search")
    if search:
        mask = (
            gap_df["상품명"].str.contains(search, case=False, na=False) |
            gap_df["isbn"].str.contains(search, na=False)
        )
        gap_df = gap_df[mask]

    st.dataframe(
        gap_df[["상품명", "정가", "판매가", "출판사", "isbn"]],
        use_container_width=True,
        hide_index=True,
        height=500,
    )

    # CSV 다운로드
    csv = gap_df.to_csv(index=False).encode("utf-8-sig")
    st.download_button(
        f"CSV 다운로드 ({len(gap_df)}건)",
        csv,
        f"gap_{target_acct}_vs_{ref_acct}.csv",
        "text/csv",
    )
