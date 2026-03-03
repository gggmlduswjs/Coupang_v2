"""Wing Excel 임포트 — 셀러 상품 재고 관리"""

import os

import pandas as pd

from core.config import AnalysisConfig
from core.database import SessionLocal
from core.models import Account, InventoryProduct

# Wing Excel 컬럼명 → 내부 필드명 매핑
# Wing "쿠팡상품정보 수정요청" 템플릿 형식 + 기타 변형 지원
WING_COLUMN_MAP = {
    # seller_product_id — Wing에서 등록상품ID가 셀러 측 상품 식별자
    "등록상품ID": "seller_product_id",
    "셀러상품ID": "seller_product_id",
    "셀러 상품ID": "seller_product_id",
    "Seller Product ID": "seller_product_id",
    # product_name — 쿠팡 노출상품명 우선, 없으면 등록상품명
    "쿠팡 노출상품명": "product_name",
    "노출상품명": "product_name",
    "등록상품명": "product_name",
    "상품명": "product_name",
    "Product Name": "product_name",
    # sale_price
    "판매가": "sale_price",
    "판매가격": "sale_price",
    "Sale Price": "sale_price",
    # original_price
    "정가": "original_price",
    "원가": "original_price",
    "Original Price": "original_price",
    # status — 판매상태 우선 (승인상태는 별도 필드)
    "판매상태": "status",
    "상태": "status",
    "Status": "status",
    # category
    "카테고리": "category",
    "Category": "category",
    # brand
    "브랜드": "brand",
    "Brand": "brand",
    # barcode
    "바코드": "barcode",
    "Barcode": "barcode",
    "모델번호": "barcode",
    # stock_qty
    "재고수량": "stock_qty",
    "재고": "stock_qty",
    "Stock": "stock_qty",
    # wing_product_id — 노출상품ID가 쿠팡 측 노출 식별자
    "노출상품ID": "wing_product_id",
    "쿠팡상품ID": "wing_product_id",
    "상품ID": "wing_product_id",
    "Product ID": "wing_product_id",
}

# InventoryProduct에 실제로 존재하는 필드
_INVENTORY_FIELDS = {
    "seller_product_id", "product_name", "sale_price", "original_price",
    "status", "category", "brand", "barcode", "stock_qty", "wing_product_id", "memo",
}

# 숫자로 변환해야 하는 필드
_NUMERIC_FIELDS = {"sale_price", "original_price", "stock_qty"}


def detect_columns(df: pd.DataFrame) -> dict[str, str]:
    """DataFrame 컬럼을 자동 인식하여 매핑 반환.
    {내부 필드명: 실제 컬럼명} 형태.
    같은 내부 필드에 여러 컬럼이 매칭되면 첫 번째 것 우선."""
    mapping = {}
    for col in df.columns:
        col_clean = str(col).strip()
        if col_clean in WING_COLUMN_MAP:
            internal = WING_COLUMN_MAP[col_clean]
            if internal not in mapping:  # 우선순위: 먼저 매칭된 것 유지
                mapping[internal] = col_clean
    return mapping


def _detect_wing_format(filepath: str) -> dict:
    """Wing Excel 형식 자동 감지. sheet명, header 행 위치 반환."""
    ext = os.path.splitext(filepath)[1].lower()
    if ext == ".csv":
        return {"sheet": None, "header": 0, "format": "csv"}

    # xlsx: Template 시트 확인
    try:
        xl = pd.ExcelFile(filepath)
        sheets = xl.sheet_names
        xl.close()
    except Exception:
        return {"sheet": 0, "header": 0, "format": "unknown"}

    if "Template" in sheets:
        # Wing 쿠팡상품정보 수정요청 템플릿
        # 구조: 행0=버전, 행1=안내, 행2=그룹명, 행3=컬럼명, 행4+=데이터
        return {"sheet": "Template", "header": 3, "format": "wing_template"}

    return {"sheet": 0, "header": 0, "format": "generic"}


def _coerce_value(field: str, val):
    """필드 타입에 맞게 값 변환. 실패 시 None 반환."""
    if field in _NUMERIC_FIELDS:
        try:
            return int(float(str(val).replace(",", "")))
        except (ValueError, TypeError):
            return None
    return val


def import_wing_excel(filepath: str, account_code: str, config: AnalysisConfig = None) -> dict:
    """Wing Excel 파일을 읽어 DB에 upsert.

    Returns:
        {"total": int, "new": int, "updated": int, "skipped": int}
    """
    db = SessionLocal()

    # 계정 확인
    account = db.query(Account).filter(Account.account_code == account_code).first()
    if not account:
        print(f"  계정 '{account_code}'을(를) 찾을 수 없습니다. 먼저 계정을 추가하세요.")
        db.close()
        return {"total": 0, "new": 0, "updated": 0, "skipped": 0}

    # Excel 형식 감지 및 읽기
    print(f"  파일 로딩: {filepath}")
    fmt = _detect_wing_format(filepath)
    print(f"  형식 감지: {fmt['format']}")

    if fmt["format"] == "csv":
        df = pd.read_csv(filepath, dtype=str)
    else:
        df = pd.read_excel(
            filepath, dtype=str,
            sheet_name=fmt["sheet"],
            header=fmt["header"],
        )

    # 빈 행 제거 (모든 값이 NaN인 행)
    df = df.dropna(how="all")
    print(f"  총 {len(df)}행 로드")

    if len(df) == 0:
        print("  데이터가 없습니다.")
        db.close()
        return {"total": 0, "new": 0, "updated": 0, "skipped": 0}

    # 컬럼 매핑
    col_map = detect_columns(df)
    print(f"  인식된 컬럼: {list(col_map.keys())}")

    if "seller_product_id" not in col_map:
        print("  셀러상품ID/등록상품ID 컬럼을 찾을 수 없습니다.")
        print(f"  사용 가능한 컬럼: {[c for c in df.columns if not str(c).startswith('검색옵션')]}")
        db.close()
        return {"total": 0, "new": 0, "updated": 0, "skipped": 0}

    # upsert
    new_count = 0
    update_count = 0
    skip_count = 0

    for _, row in df.iterrows():
        # 매핑된 컬럼으로 dict 생성
        mapped = {}
        for internal_name, excel_col in col_map.items():
            val = row.get(excel_col, "")
            if pd.isna(val):
                val = ""
            mapped[internal_name] = val

        # seller_product_id 필수
        if not mapped.get("seller_product_id"):
            skip_count += 1
            continue

        # 기존 레코드 조회
        existing = db.query(InventoryProduct).filter(
            InventoryProduct.account_id == account.id,
            InventoryProduct.seller_product_id == mapped["seller_product_id"],
        ).first()

        if existing:
            # 업데이트
            for k, v in mapped.items():
                if k in _INVENTORY_FIELDS and v:
                    v = _coerce_value(k, v)
                    if v is not None:
                        setattr(existing, k, v)
            is_new = False
        else:
            # 신규 생성
            kwargs = {"account_id": account.id}
            for k, v in mapped.items():
                if k in _INVENTORY_FIELDS and v:
                    v = _coerce_value(k, v)
                    if v is not None:
                        kwargs[k] = v
            db.add(InventoryProduct(**kwargs))
            is_new = True

        if is_new:
            new_count += 1
        else:
            update_count += 1

    db.commit()

    total = new_count + update_count
    result = {"total": total, "new": new_count, "updated": update_count, "skipped": skip_count}
    print(f"  임포트 완료: 전체 {total} (신규 {new_count}, 갱신 {update_count}, 건너뜀 {skip_count})")
    db.close()
    return result
