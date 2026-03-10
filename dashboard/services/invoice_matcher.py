"""송장 매칭 서비스 — 한진 출력자료 ↔ 배송리스트 매칭 통합 모듈.

공개 함수 3개:
- load_latest_batch(): DB에서 최신 배치 로드
- match_invoices(): 한진 엑셀 ↔ 배치 자동 매칭
- check_registerable(): INSTRUCT 주문 대조 → 등록 가능/이미출고 분류
"""
import logging
from typing import Optional

import pandas as pd
from sqlalchemy import desc, text as sa_text

from core.database import SessionLocal
from core.models.delivery_log import DeliveryListLog
from dashboard.utils import engine

logger = logging.getLogger(__name__)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 1. load_latest_batch
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def load_latest_batch() -> Optional[pd.DataFrame]:
    """DB에서 가장 최근 batch_id의 배송리스트를 seq_no 순으로 로드.

    Returns:
        DataFrame with columns: 번호, 묶음배송번호, 주문번호, 수취인이름,
        구매자, _account_id, _vendor_item_id  /  None if empty.
    """
    try:
        db = SessionLocal()
        # 최신 batch_id 조회
        latest = db.query(DeliveryListLog.batch_id).filter(
            DeliveryListLog.batch_id.isnot(None),
        ).order_by(desc(DeliveryListLog.downloaded_at)).first()

        if not latest or not latest[0]:
            db.close()
            return None

        batch_id = latest[0]
        logs = db.query(DeliveryListLog).filter(
            DeliveryListLog.batch_id == batch_id,
        ).order_by(DeliveryListLog.seq_no).all()
        db.close()

        if not logs:
            return None

        rows = []
        for log in logs:
            rows.append({
                "번호": log.seq_no or 0,
                "묶음배송번호": log.shipment_box_id,
                "주문번호": log.order_id or 0,
                "수취인이름": log.receiver_name or "",
                "구매자": log.buyer_name or "",
                "_account_id": log.account_id,
                "_vendor_item_id": log.vendor_item_id or 0,
                "_batch_id": batch_id,
                "_downloaded_at": log.downloaded_at,
            })

        df = pd.DataFrame(rows)
        return df
    except Exception as e:
        logger.warning(f"최신 배치 로드 실패: {e}")
        return None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 2. match_invoices
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def _find_recv_col(df: pd.DataFrame) -> Optional[str]:
    """한진 엑셀에서 수취인 컬럼명 탐색."""
    for name in ["받으시는 분", "받으시는분", "수취인", "수취인이름"]:
        if name in df.columns:
            return name
    return None


def _match_by_sequence(hanjin_df: pd.DataFrame, batch_df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """전략1: 한진 출력자료등록 — 순번 컬럼 → batch의 seq_no 매칭."""
    seq_col = None
    for col in ["순번", "번호", "NO", "No", "no"]:
        if col in hanjin_df.columns:
            seq_col = col
            break
    if seq_col is None:
        return None

    invoice_col = None
    for col in ["운송장번호", "송장번호", "운송장", "waybill"]:
        if col in hanjin_df.columns:
            invoice_col = col
            break
    if invoice_col is None:
        return None

    hj = hanjin_df[hanjin_df[invoice_col].notna() & (hanjin_df[invoice_col] != "")].copy()
    if hj.empty:
        return None

    results = []
    for _, hr in hj.iterrows():
        seq = int(hr[seq_col])
        invoice = str(hr[invoice_col]).strip()
        dl_match = batch_df[batch_df["번호"] == seq]
        if dl_match.empty:
            continue
        dr = dl_match.iloc[0]
        results.append({
            "묶음배송번호": dr["묶음배송번호"],
            "주문번호": dr["주문번호"],
            "운송장번호": invoice,
            "_account_id": dr["_account_id"],
            "_vendor_item_id": dr["_vendor_item_id"],
        })

    return pd.DataFrame(results) if results else None


def _match_by_row_order(hanjin_df: pd.DataFrame, batch_df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """전략2: 한진 원본List — 행 수 일치 시 위치 기반 매칭."""
    hj = hanjin_df[hanjin_df["운송장번호"].notna() & (hanjin_df["운송장번호"] != "")].copy()
    if hj.empty or len(hj) != len(batch_df):
        return None

    results = []
    for i in range(len(hj)):
        hr = hj.iloc[i]
        dr = batch_df.iloc[i]
        results.append({
            "묶음배송번호": dr["묶음배송번호"],
            "주문번호": dr["주문번호"],
            "운송장번호": str(hr["운송장번호"]).strip(),
            "_account_id": dr["_account_id"],
            "_vendor_item_id": dr["_vendor_item_id"],
        })

    return pd.DataFrame(results) if results else None


def _match_by_name(hanjin_df: pd.DataFrame, recv_col: str) -> Optional[pd.DataFrame]:
    """전략3: DB orders 테이블에서 수취인/주문자 이름으로 매칭 (fallback)."""
    hj = hanjin_df[hanjin_df["운송장번호"].notna() & (hanjin_df["운송장번호"] != "")].copy()
    if hj.empty:
        return None

    names = set()
    for _, hr in hj.iterrows():
        n = str(hr[recv_col]).strip()
        if n and n != "nan":
            names.add(n)
    if not names:
        return None

    try:
        name_list = list(names)
        sql = """
            SELECT shipment_box_id, order_id, receiver_name, orderer_name,
                   account_id, vendor_item_id
            FROM orders
            WHERE (receiver_name = ANY(:names) OR orderer_name = ANY(:names))
              AND ordered_at >= NOW() - INTERVAL '14 days'
            ORDER BY ordered_at DESC
        """
        with engine.connect() as conn:
            result = conn.execute(sa_text(sql), {"names": name_list})
            db_rows = result.fetchall()
    except Exception as e:
        logger.warning(f"DB 주문 조회 실패: {e}")
        return None

    if not db_rows:
        return None

    db_df = pd.DataFrame(db_rows, columns=[
        "묶음배송번호", "주문번호", "수취인", "주문자", "_account_id", "_vendor_item_id",
    ])

    hj_unique = hj.drop_duplicates(subset=["운송장번호"], keep="first")
    results = []
    used_box_ids = set()

    for _, hr in hj_unique.iterrows():
        invoice = str(hr["운송장번호"]).strip()
        hj_name = str(hr[recv_col]).strip()
        if not hj_name or hj_name == "nan":
            continue

        avail = db_df[~db_df["묶음배송번호"].isin(used_box_ids)]
        candidates = avail[avail["수취인"].astype(str).str.strip() == hj_name]
        if candidates.empty:
            candidates = avail[avail["주문자"].astype(str).str.strip() == hj_name]
        if candidates.empty:
            continue

        first = candidates.iloc[0]
        box_id = first["묶음배송번호"]
        used_box_ids.add(box_id)
        results.append({
            "묶음배송번호": box_id,
            "주문번호": first["주문번호"],
            "운송장번호": invoice,
            "_account_id": first["_account_id"],
            "_vendor_item_id": first["_vendor_item_id"],
        })

    return pd.DataFrame(results) if results else None


def _enrich_from_db(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """묶음배송번호/주문번호는 있지만 _account_id/_vendor_item_id가 없을 때 DB에서 보충."""
    try:
        box_ids = [int(x) for x in df["묶음배송번호"].unique()]
        sql = """
            SELECT shipment_box_id, order_id, account_id, vendor_item_id
            FROM orders
            WHERE shipment_box_id = ANY(:box_ids)
              AND ordered_at >= NOW() - INTERVAL '14 days'
        """
        with engine.connect() as conn:
            result = conn.execute(sa_text(sql), {"box_ids": box_ids})
            db_rows = result.fetchall()
        if not db_rows:
            return None
        db_df = pd.DataFrame(db_rows, columns=["묶음배송번호", "주문번호", "_account_id", "_vendor_item_id"])
        db_df = db_df.drop_duplicates(subset=["묶음배송번호", "주문번호"])
        # str 변환 후 merge
        df["묶음배송번호"] = df["묶음배송번호"].astype(str)
        df["주문번호"] = df["주문번호"].astype(str)
        db_df["묶음배송번호"] = db_df["묶음배송번호"].astype(str)
        db_df["주문번호"] = db_df["주문번호"].astype(str)
        merged = df.merge(db_df, on=["묶음배송번호", "주문번호"], how="left")
        matched = merged[merged["_account_id"].notna()].copy()
        return matched if not matched.empty else None
    except Exception as e:
        logger.warning(f"DB enrichment 실패: {e}")
        return None


def match_invoices(hanjin_df: pd.DataFrame, batch_df: Optional[pd.DataFrame]) -> tuple[Optional[pd.DataFrame], str]:
    """한진 엑셀 ↔ 배치/DB 자동 매칭.

    Returns:
        (matched_df, method_name): 매칭 결과 DataFrame + 사용된 전략 이름.
        matched_df columns: 묶음배송번호, 주문번호, 운송장번호, _account_id, _vendor_item_id
    """
    # 직접 매칭 (묶음배송번호/주문번호 컬럼이 있는 경우)
    if all(c in hanjin_df.columns for c in ["묶음배송번호", "주문번호", "운송장번호"]):
        filled = hanjin_df[hanjin_df["운송장번호"].notna() & (hanjin_df["운송장번호"] != "")].copy()
        if not filled.empty:
            # _account_id/_vendor_item_id 없으면 DB에서 조회
            if "_account_id" not in filled.columns:
                filled = _enrich_from_db(filled)
            if filled is not None and not filled.empty:
                return filled[["묶음배송번호", "주문번호", "운송장번호", "_account_id", "_vendor_item_id"]].copy(), "직접매칭"

    # 전략1: 순번 매칭
    if batch_df is not None and "순번" in hanjin_df.columns:
        result = _match_by_sequence(hanjin_df, batch_df)
        if result is not None and not result.empty:
            return result, "순번매칭"

    # 전략2: 행순서 매칭
    if batch_df is not None and "운송장번호" in hanjin_df.columns:
        result = _match_by_row_order(hanjin_df, batch_df)
        if result is not None and not result.empty:
            return result, "행순서매칭"

    # 전략3: 이름 매칭 (DB fallback)
    recv_col = _find_recv_col(hanjin_df)
    if recv_col and "운송장번호" in hanjin_df.columns:
        result = _match_by_name(hanjin_df, recv_col)
        if result is not None and not result.empty:
            return result, "이름매칭(DB)"

    return None, ""


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 3. check_registerable
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def check_registerable(matched_df: pd.DataFrame, instruct_df: pd.DataFrame) -> dict:
    """매칭된 송장을 현재 INSTRUCT 주문과 대조 → 등록 가능/이미출고 분류.

    Args:
        matched_df: match_invoices() 결과
        instruct_df: 현재 INSTRUCT 상태 주문 (취소 제외)

    Returns:
        {
            "registerable": DataFrame (등록 가능),
            "already_shipped": DataFrame (이미 DEPARTURE/DELIVERING),
            "summary": {"등록가능": N, "이미출고": M},
        }
    """
    if matched_df is None or matched_df.empty:
        return {
            "registerable": pd.DataFrame(),
            "already_shipped": pd.DataFrame(),
            "summary": {"등록가능": 0, "이미출고": 0},
        }

    matched = matched_df.copy()
    matched["_box_str"] = matched["묶음배송번호"].astype(str)

    if instruct_df.empty:
        return {
            "registerable": pd.DataFrame(),
            "already_shipped": matched.drop(columns=["_box_str"]),
            "summary": {"등록가능": 0, "이미출고": len(matched)},
        }

    current_boxes = set(instruct_df["묶음배송번호"].astype(str))
    is_registerable = matched["_box_str"].isin(current_boxes)

    registerable = matched[is_registerable].drop(columns=["_box_str"]).copy()
    already_shipped = matched[~is_registerable].drop(columns=["_box_str"]).copy()

    return {
        "registerable": registerable,
        "already_shipped": already_shipped,
        "summary": {"등록가능": len(registerable), "이미출고": len(already_shipped)},
    }
