"""매핑 로직 — 3단계 매칭 (바코드 → 등록상품명 → 퍼지).

007-ez 계정의 상품을 타겟 계정 상품과 1:1 매칭.
"""

import os
import re
import difflib
from typing import Optional

import pandas as pd


# ─── 헬퍼 ────────────────────────────────────────────

def _safe_str(val) -> str:
    if val is None:
        return ""
    s = str(val).strip()
    return "" if s.lower() == "none" else s


def _safe_int(val) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None


def _find_column(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
    """DataFrame에서 후보 컬럼명 중 존재하는 것 반환."""
    for c in candidates:
        if c in df.columns:
            return c
        # 부분 매칭
        for col in df.columns:
            if c in col:
                return col
    return None


# ─── 데이터 로딩 ─────────────────────────────────────

def load_detailinfo(path: str) -> pd.DataFrame:
    """Wing detailinfo Excel 로드.

    Sheet='Template', header=Row4 (0-indexed=3).
    주요 컬럼: C1=등록상품ID, C2=등록상품명, C3=쿠팡 노출상품명,
              C4=카테고리, C5=제조사, C6=브랜드, C7=검색어, C231=바코드
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"파일을 찾을 수 없습니다: {path}")

    # Template 시트, 헤더가 4번째 행 (0-indexed=3)
    try:
        df = pd.read_excel(path, sheet_name="Template", header=3)
    except ValueError:
        df = pd.read_excel(path, header=3)

    # 컬럼명 정리 (공백 제거)
    df.columns = [_safe_str(c) for c in df.columns]

    # 빈 행 제거
    id_col = _find_column(df, ["등록상품ID", "업체상품ID", "업체상품 ID"])
    if id_col:
        df = df.dropna(subset=[id_col])
        df = df[df[id_col].apply(lambda x: _safe_str(x) != "")]

    return df


def load_price_inventory(path: str) -> pd.DataFrame:
    """Wing price_inventory Excel 로드.

    Sheet='data', header=Row3 (0-indexed=2).
    주요 컬럼: C1=업체상품 ID, C2=Product ID, C3=옵션 ID,
              C5=바코드, C7=쿠팡 노출 상품명, C8=업체 등록 상품명, C10=판매가격
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"파일을 찾을 수 없습니다: {path}")

    try:
        df = pd.read_excel(path, sheet_name="data", header=2)
    except ValueError:
        df = pd.read_excel(path, header=2)

    df.columns = [_safe_str(c) for c in df.columns]

    id_col = _find_column(df, ["업체상품 ID", "업체상품ID"])
    if id_col:
        df = df.dropna(subset=[id_col])
        df = df[df[id_col].apply(lambda x: _safe_str(x) != "")]

    return df


# ─── 매핑 ─────────────────────────────────────────────

def build_mapping(source_detail: pd.DataFrame,
                  source_price: pd.DataFrame,
                  target_detail: pd.DataFrame,
                  target_price: pd.DataFrame) -> dict:
    """3단계 매핑 구축.

    Returns: {
        "matched": [{
            "source_spid": str, "target_spid": str,
            "match_key": "barcode"|"registered_name"|"fuzzy",
            "source_name": str, "target_name": str,
            "source_price": int, "target_price": int,
            "name_changed": bool, "price_changed": bool,
            "fuzzy_score": float (퍼지 매칭 시),
        }],
        "unmatched_source": [...],  # 007-ez에만 있는 상품
        "unmatched_target": [...],  # 타겟에만 있는 상품
        "stats": {"barcode": N, "registered_name": N, "fuzzy": N,
                  "unmatched_source": N, "unmatched_target": N}
    }
    """
    # 소스/타겟 상품 데이터 통합
    source_products = _merge_detail_price(source_detail, source_price)
    target_products = _merge_detail_price(target_detail, target_price)

    print(f"  소스 상품: {len(source_products)}개")
    print(f"  타겟 상품: {len(target_products)}개")

    matched = []
    stats = {"barcode": 0, "registered_name": 0, "fuzzy": 0,
             "unmatched_source": 0, "unmatched_target": 0}

    # 이미 매핑된 타겟 SPID 추적
    matched_source_spids = set()
    matched_target_spids = set()

    # ── Pass 1: 바코드(ISBN) 매칭 ──
    target_by_barcode: dict[str, dict] = {}
    for tp in target_products:
        bc = tp.get("barcode", "")
        if bc and bc not in target_by_barcode:
            target_by_barcode[bc] = tp

    for sp in source_products:
        bc = sp.get("barcode", "")
        if bc and bc in target_by_barcode:
            tp = target_by_barcode[bc]
            if tp["spid"] not in matched_target_spids:
                matched.append(_build_match_entry(sp, tp, "barcode"))
                matched_source_spids.add(sp["spid"])
                matched_target_spids.add(tp["spid"])
                stats["barcode"] += 1

    print(f"  바코드 매칭: {stats['barcode']}개")

    # ── Pass 2: 등록상품명 완전일치 ──
    target_by_regname: dict[str, dict] = {}
    for tp in target_products:
        if tp["spid"] not in matched_target_spids:
            rn = tp.get("registered_name", "")
            if rn and rn not in target_by_regname:
                target_by_regname[rn] = tp

    for sp in source_products:
        if sp["spid"] not in matched_source_spids:
            rn = sp.get("registered_name", "")
            if rn and rn in target_by_regname:
                tp = target_by_regname[rn]
                if tp["spid"] not in matched_target_spids:
                    matched.append(_build_match_entry(sp, tp, "registered_name"))
                    matched_source_spids.add(sp["spid"])
                    matched_target_spids.add(tp["spid"])
                    stats["registered_name"] += 1

    print(f"  등록상품명 매칭: {stats['registered_name']}개")

    # ── Pass 3: 퍼지매칭 (핵심 키워드 유사도) ──
    remaining_source = [sp for sp in source_products
                        if sp["spid"] not in matched_source_spids]
    remaining_target = [tp for tp in target_products
                        if tp["spid"] not in matched_target_spids]

    if remaining_source and remaining_target:
        # 타겟 인덱스 구축
        target_cores = [(tp, _extract_core_name(tp.get("display_name", "")))
                        for tp in remaining_target]

        for sp in remaining_source:
            s_core = _extract_core_name(sp.get("display_name", ""))
            if not s_core:
                continue

            best_score = 0.0
            best_target = None
            for tp, t_core in target_cores:
                if tp["spid"] in matched_target_spids:
                    continue
                if not t_core:
                    continue
                score = _fuzzy_score(s_core, t_core)
                if score > best_score:
                    best_score = score
                    best_target = tp

            if best_score >= 0.75 and best_target:
                entry = _build_match_entry(sp, best_target, "fuzzy")
                entry["fuzzy_score"] = round(best_score, 3)
                matched.append(entry)
                matched_source_spids.add(sp["spid"])
                matched_target_spids.add(best_target["spid"])
                stats["fuzzy"] += 1

    print(f"  퍼지 매칭: {stats['fuzzy']}개")

    # 미매핑 상품
    unmatched_source = [sp for sp in source_products
                        if sp["spid"] not in matched_source_spids]
    unmatched_target = [tp for tp in target_products
                        if tp["spid"] not in matched_target_spids]

    stats["unmatched_source"] = len(unmatched_source)
    stats["unmatched_target"] = len(unmatched_target)

    print(f"\n  [매핑 결과]")
    print(f"    매핑 완료: {len(matched)}개")
    print(f"    미매핑 (소스=갭): {stats['unmatched_source']}개")
    print(f"    미매핑 (타겟): {stats['unmatched_target']}개")

    return {
        "matched": matched,
        "unmatched_source": unmatched_source,
        "unmatched_target": unmatched_target,
        "stats": stats,
    }


def _merge_detail_price(detail_df: pd.DataFrame,
                        price_df: pd.DataFrame) -> list[dict]:
    """detailinfo + price_inventory를 SPID 기준으로 통합."""
    products = []

    # detailinfo 컬럼 찾기 (Row4 헤더)
    d_spid_col = _find_column(detail_df, ["등록상품ID", "업체상품ID", "업체상품 ID"])
    d_display_col = _find_column(detail_df, ["쿠팡 노출상품명", "노출상품명"])
    d_reg_col = _find_column(detail_df, ["등록상품명"])
    d_barcode_col = _find_column(detail_df, ["바코드"])
    d_category_col = _find_column(detail_df, ["카테고리"])
    d_pid_col = _find_column(detail_df, ["노출상품ID", "Product ID"])
    d_option_col = _find_column(detail_df, ["옵션 ID", "옵션ID"])

    # price_inventory 컬럼 찾기 (Row3 헤더)
    p_spid_col = _find_column(price_df, ["업체상품 ID", "업체상품ID"])
    p_price_col = _find_column(price_df, ["판매가격"])
    p_barcode_col = _find_column(price_df, ["바코드"])
    p_name_col = _find_column(price_df, ["쿠팡 노출 상품명", "상품명"])
    p_reg_col = _find_column(price_df, ["업체 등록 상품명"])

    # price를 SPID → 가격/바코드/등록상품명 dict로 변환
    price_map: dict[str, int] = {}
    price_barcode_map: dict[str, str] = {}
    price_regname_map: dict[str, str] = {}
    if p_spid_col and p_price_col:
        for _, row in price_df.iterrows():
            spid = _safe_str(row.get(p_spid_col, ""))
            if spid:
                price = _safe_int(row.get(p_price_col))
                if price is not None:
                    price_map[spid] = price
                bc = _safe_str(row.get(p_barcode_col, "")) if p_barcode_col else ""
                if bc:
                    price_barcode_map[spid] = bc
                rn = _safe_str(row.get(p_reg_col, "")) if p_reg_col else ""
                if rn:
                    price_regname_map[spid] = rn

    # detailinfo 기준으로 상품 목록 구성
    if not d_spid_col:
        return products

    seen_spids = set()
    for _, row in detail_df.iterrows():
        spid = _safe_str(row.get(d_spid_col, ""))
        if not spid or spid in seen_spids:
            continue
        seen_spids.add(spid)

        display = _safe_str(row.get(d_display_col, "")) if d_display_col else ""
        registered = _safe_str(row.get(d_reg_col, "")) if d_reg_col else ""
        barcode = _safe_str(row.get(d_barcode_col, "")) if d_barcode_col else ""
        category = _safe_str(row.get(d_category_col, "")) if d_category_col else ""
        pid = _safe_str(row.get(d_pid_col, "")) if d_pid_col else ""
        option_id = _safe_str(row.get(d_option_col, "")) if d_option_col else ""

        # 바코드가 detailinfo에 없으면 price_inventory에서 가져오기
        if not barcode and spid in price_barcode_map:
            barcode = price_barcode_map[spid]

        # 등록상품명: detailinfo의 등록상품명이 노출상품명과 같으면
        # price_inventory의 업체 등록 상품명을 우선 사용 (원본일 가능성 높음)
        if registered == display and spid in price_regname_map:
            registered = price_regname_map[spid]
        elif not registered and spid in price_regname_map:
            registered = price_regname_map[spid]

        price = price_map.get(spid)

        products.append({
            "spid": spid,
            "display_name": display,
            "registered_name": registered,
            "barcode": barcode,
            "category": category,
            "pid": pid,
            "option_id": option_id,
            "price": price,
        })

    return products


def _build_match_entry(source: dict, target: dict, match_key: str) -> dict:
    """매칭 결과 엔트리 생성."""
    s_name = source.get("display_name", "")
    t_name = target.get("display_name", "")
    s_price = source.get("price")
    t_price = target.get("price")

    return {
        "source_spid": source["spid"],
        "target_spid": target["spid"],
        "match_key": match_key,
        "source_name": s_name,
        "target_name": t_name,
        "source_registered_name": source.get("registered_name", ""),
        "target_registered_name": target.get("registered_name", ""),
        "source_barcode": source.get("barcode", ""),
        "source_price": s_price,
        "target_price": t_price,
        "name_changed": s_name != t_name and bool(s_name),
        "price_changed": s_price != t_price and s_price is not None,
    }


def _extract_core_name(name: str) -> str:
    """상품명에서 핵심 키워드 추출 (세트/묶음/권수 등 제거).

    예: '2025 EBS 수능특강 국어 세트(2권)' → 'EBS 수능특강 국어'
    """
    if not name:
        return ""

    # 연도 제거
    s = re.sub(r'20\d{2}\s*', '', name)
    # 괄호 내용 제거
    s = re.sub(r'\([^)]*\)', '', s)
    s = re.sub(r'\[[^\]]*\]', '', s)
    # 세트/묶음 관련 키워드 제거
    s = re.sub(r'세트|묶음|패키지|전\s*\d+\s*권|총\s*\d+\s*권|\d+권|\d+\s*세트', '', s)
    # 특수문자/여분 공백 정리
    s = re.sub(r'[^\w\s가-힣a-zA-Z]', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()

    return s


def _fuzzy_score(name_a: str, name_b: str) -> float:
    """두 상품명의 유사도 점수 (0~1). difflib.SequenceMatcher 사용."""
    if not name_a or not name_b:
        return 0.0
    return difflib.SequenceMatcher(None, name_a, name_b).ratio()
