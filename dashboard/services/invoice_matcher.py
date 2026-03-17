"""송장 매칭 서비스 — 한진 출력자료 ↔ 배송리스트 매칭 통합 모듈.

공개 함수 3개:
- load_latest_batch(): DB에서 최신 배치 로드
- match_invoices(): 한진 엑셀 ↔ 배치 자동 매칭
- check_registerable(): INSTRUCT 주문 대조 → 등록 가능/이미출고 분류
"""
import logging
from typing import Optional

import pandas as pd
from sqlalchemy import text as sa_text

from dashboard.utils import engine

logger = logging.getLogger(__name__)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 1. load_latest_batch
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def list_batches(limit: int = 20) -> Optional[pd.DataFrame]:
    """DB에서 배치 목록 조회 (최신순).

    Returns:
        DataFrame with columns: batch_id, downloaded_at, count  /  None if empty.
    """
    try:
        with engine.connect() as conn:
            results = conn.execute(sa_text("""
                SELECT batch_id,
                       MIN(downloaded_at) AS downloaded_at,
                       COUNT(*) AS count
                FROM delivery_list_logs
                WHERE batch_id IS NOT NULL
                GROUP BY batch_id
                ORDER BY MIN(downloaded_at) DESC
                LIMIT :lim
            """), {"lim": limit}).fetchall()
        if not results:
            return None
        rows = [{"batch_id": r[0], "downloaded_at": r[1], "count": r[2]} for r in results]
        return pd.DataFrame(rows)
    except Exception as e:
        logger.warning(f"배치 목록 조회 실패: {e}")
        return None


def load_latest_batch(batch_id: Optional[str] = None) -> Optional[pd.DataFrame]:
    """DB에서 배송리스트 배치를 seq_no 순으로 로드.

    Args:
        batch_id: 특정 배치 ID. None이면 가장 최근 배치.

    Returns:
        DataFrame with columns: 번호, 묶음배송번호, 주문번호, 수취인이름,
        구매자, _account_id, _vendor_item_id  /  None if empty.
    """
    try:
        with engine.connect() as conn:
            if batch_id is None:
                row = conn.execute(sa_text("""
                    SELECT batch_id FROM delivery_list_logs
                    WHERE batch_id IS NOT NULL
                    ORDER BY downloaded_at DESC
                    LIMIT 1
                """)).fetchone()
                if not row or not row[0]:
                    return None
                batch_id = row[0]

            results = conn.execute(sa_text("""
                SELECT seq_no, shipment_box_id, order_id,
                       receiver_name, buyer_name,
                       account_id, vendor_item_id,
                       downloaded_at, COALESCE(registered, false) AS registered
                FROM delivery_list_logs
                WHERE batch_id = :bid
                ORDER BY seq_no
            """), {"bid": batch_id}).fetchall()

        if not results:
            return None

        rows = []
        for r in results:
            rows.append({
                "번호": r[0] or 0,
                "묶음배송번호": r[1],
                "주문번호": r[2] or 0,
                "수취인이름": r[3] or "",
                "구매자": r[4] or "",
                "_account_id": r[5],
                "_vendor_item_id": r[6] or 0,
                "_batch_id": batch_id,
                "_downloaded_at": r[7],
                "_registered": r[8] or False,
            })

        return pd.DataFrame(rows)
    except Exception as e:
        logger.warning(f"배치 로드 실패: {e}")
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


def _match_by_name_batch(hanjin_df: pd.DataFrame, batch_df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """전략2: 한진 출력자료 ↔ 배치 수취인 이름 매칭.

    한진 시스템이 배송지역별로 행을 재정렬하므로 행순서 대신 이름으로 매칭.
    동명이인은 등장 순서대로 1:1 큐 매칭.
    """
    from collections import defaultdict

    invoice_col = None
    for col in ["운송장번호", "송장번호", "운송장", "waybill"]:
        if col in hanjin_df.columns:
            invoice_col = col
            break
    if invoice_col is None:
        return None

    recv_col = _find_recv_col(hanjin_df)
    if recv_col is None or "수취인이름" not in batch_df.columns:
        return None

    hj = hanjin_df[hanjin_df[invoice_col].notna() & (hanjin_df[invoice_col] != "")].copy()
    if hj.empty:
        return None

    # 배치 수취인 → 주문 목록 (큐) 구성
    name_queue: dict[str, list] = defaultdict(list)
    for _, row in batch_df.iterrows():
        name = _strip_receiver_suffix(str(row["수취인이름"]).strip())
        if name and name != "nan":
            name_queue[name].append(row)

    # 구매자 이름으로도 보조 큐 구성 (수취인≠구매자인 경우 fallback)
    buyer_queue: dict[str, list] = defaultdict(list)
    if "구매자" in batch_df.columns:
        for _, row in batch_df.iterrows():
            buyer = _strip_receiver_suffix(str(row["구매자"]).strip())
            if buyer and buyer != "nan":
                buyer_queue[buyer].append(row)

    results = []
    used_box_ids: set = set()

    for _, hr in hj.iterrows():
        invoice = str(hr[invoice_col]).strip()
        hj_name = _strip_receiver_suffix(str(hr[recv_col]).strip())
        if not hj_name or hj_name == "nan":
            continue

        # 수취인 이름으로 먼저 탐색, 없으면 구매자 이름으로 fallback
        matched_row = None
        for queue_key, queue_dict in [("recv", name_queue), ("buyer", buyer_queue)]:
            queue = queue_dict.get(hj_name, [])
            for idx, candidate in enumerate(queue):
                box_id = candidate["묶음배송번호"]
                if box_id not in used_box_ids:
                    matched_row = candidate
                    queue.pop(idx)
                    break
            if matched_row is not None:
                break

        if matched_row is None:
            continue

        box_id = matched_row["묶음배송번호"]
        used_box_ids.add(box_id)
        results.append({
            "묶음배송번호": box_id,
            "주문번호": matched_row["주문번호"],
            "운송장번호": invoice,
            "_account_id": matched_row["_account_id"],
            "_vendor_item_id": matched_row["_vendor_item_id"],
        })

    if not results:
        return None

    matched_count = len(results)
    total_count = len(hj)
    if matched_count < total_count:
        logger.warning(
            f"이름매칭(배치) {matched_count}/{total_count}건 매칭 "
            f"({total_count - matched_count}건 미매칭)"
        )

    return pd.DataFrame(results)


def _strip_receiver_suffix(name: str) -> str:
    """수취인 이름에서 합배송 방지 구분자 제거. '이수희 (2)' → '이수희'"""
    import re
    return re.sub(r"\s*\(\d+\)$", "", name).strip()


def _match_by_name(hanjin_df: pd.DataFrame, recv_col: str) -> Optional[pd.DataFrame]:
    """전략3: DB orders 테이블에서 수취인/주문자 이름으로 매칭 (fallback)."""
    hj = hanjin_df[hanjin_df["운송장번호"].notna() & (hanjin_df["운송장번호"] != "")].copy()
    if hj.empty:
        return None

    names = set()
    for _, hr in hj.iterrows():
        n = _strip_receiver_suffix(str(hr[recv_col]).strip())
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
              AND status IN ('INSTRUCT', 'ACCEPT')
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
        hj_name = _strip_receiver_suffix(str(hr[recv_col]).strip())
        if not hj_name or hj_name == "nan":
            continue

        avail = db_df[~db_df["묶음배송번호"].isin(used_box_ids)]
        candidates = avail[avail["수취인"].astype(str).str.strip() == hj_name]
        if candidates.empty:
            candidates = avail[avail["주문자"].astype(str).str.strip() == hj_name]
        if candidates.empty:
            continue

        # 동명이인 방지: INSTRUCT 상태 우선, 같은 이름 여러 계정이면 주문 1개씩만 매칭
        candidates = candidates.drop_duplicates(subset=["묶음배송번호"], keep="first")
        # 계정별로 분산 — 같은 이름이 여러 계정에 있으면 아직 미사용 계정 우선
        _used_accts = {r.get("_account_id") for r in results} if results else set()
        _new_acct = candidates[~candidates["_account_id"].isin(_used_accts)]
        first = _new_acct.iloc[0] if not _new_acct.empty else candidates.iloc[0]
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

    # 전략2: 이름매칭(배치) — 수취인 이름으로 배치 매칭 (행순서 무관)
    if batch_df is not None:
        result = _match_by_name_batch(hanjin_df, batch_df)
        if result is not None and not result.empty:
            return result, "이름매칭(배치)"

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


def check_registerable(matched_df: pd.DataFrame, instruct_df: pd.DataFrame,
                       batch_df: Optional[pd.DataFrame] = None) -> dict:
    """매칭된 송장을 현재 INSTRUCT 주문과 대조 → 등록 가능/이미출고 분류.

    판별 우선순위:
    1) batch_df에 registered=True → 이미 등록됨 (확정)
    2) instruct_df에 있음 → 등록 가능
    3) 둘 다 아님 → 이미 출고 (DEPARTURE 이후)

    Args:
        matched_df: match_invoices() 결과
        instruct_df: 현재 INSTRUCT 상태 주문 (취소 제외)
        batch_df: load_latest_batch() 결과 (registered 플래그 포함)

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
    matched["_vid_str"] = matched["_vendor_item_id"].astype(str)

    # 1) 배치에서 이미 등록된 건 제외 — (box, vendor_item_id) 단위
    already_registered_keys = set()
    if batch_df is not None and "_registered" in batch_df.columns:
        reg_rows = batch_df[batch_df["_registered"] == True]
        if not reg_rows.empty:
            already_registered_keys = set(
                reg_rows["묶음배송번호"].astype(str) + "_" + reg_rows.get("_vendor_item_id", pd.Series(0)).astype(str)
            )

    # 2) INSTRUCT 주문 집합 — (box, vendor_item_id) 단위
    current_keys = set()
    if not instruct_df.empty:
        current_keys = set(
            instruct_df["묶음배송번호"].astype(str) + "_" + instruct_df["_vendor_item_id"].astype(str)
        )

    # 분류: (box, vendor_item_id) 단위로 판별 → multi-item 부분 등록 지원
    matched["_key"] = matched["_box_str"] + "_" + matched["_vid_str"]
    is_already_registered = matched["_key"].isin(already_registered_keys)
    is_in_instruct = matched["_key"].isin(current_keys)
    # box 단위 fallback: vendor_item_id가 0이면 box만으로 판별
    is_in_instruct_box = matched["_box_str"].isin(
        set(instruct_df["묶음배송번호"].astype(str)) if not instruct_df.empty else set()
    )
    is_in_instruct = is_in_instruct | (is_in_instruct_box & (matched["_vid_str"] == "0"))
    is_registerable = ~is_already_registered & is_in_instruct

    registerable = matched[is_registerable].drop(columns=["_box_str", "_vid_str", "_key"]).copy()
    already_shipped = matched[~is_registerable].drop(columns=["_box_str", "_vid_str", "_key"]).copy()

    return {
        "registerable": registerable,
        "already_shipped": already_shipped,
        "summary": {"등록가능": len(registerable), "이미출고": len(already_shipped)},
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 4. check_missing_invoices
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def check_missing_invoices(
    batch_df: pd.DataFrame,
    matched_df: Optional[pd.DataFrame],
) -> Optional[pd.DataFrame]:
    """배치(배송리스트) 대비 한진 매칭 누락 건 감지.

    배치에는 있지만 한진 엑셀에서 매칭되지 않은 주문 = 송장 미출력 가능성.

    Args:
        batch_df: load_latest_batch() 결과 (전체 배치)
        matched_df: match_invoices() 결과 (한진 매칭 성공 건)

    Returns:
        누락 건 DataFrame (번호, 묶음배송번호, 주문번호, 수취인이름)  /  None if 전부 매칭됨.
    """
    if batch_df is None or batch_df.empty:
        return None

    batch_boxes = set(batch_df["묶음배송번호"].astype(str))

    if matched_df is not None and not matched_df.empty:
        matched_boxes = set(matched_df["묶음배송번호"].astype(str))
    else:
        matched_boxes = set()

    missing_boxes = batch_boxes - matched_boxes
    if not missing_boxes:
        return None

    missing = batch_df[batch_df["묶음배송번호"].astype(str).isin(missing_boxes)].copy()
    # 이미 등록 완료된 건은 제외
    if "_registered" in missing.columns:
        missing = missing[missing["_registered"] != True]
    if missing.empty:
        return None

    return missing[["번호", "묶음배송번호", "주문번호", "수취인이름"]].copy()
