"""쿠팡 API 기반 상품 관리 모듈

core.api.wing_client의 저수준 메서드를 조합하여
상품 등록, 수정, 상태 조회, 삭제를 수행.

핵심 제약사항:
- partial update: searchTags를 silently 무시 (SUCCESS 반환하나 변경 안됨)
- full update: searchTags 변경되지만 상품 상태 → "임시저장" (재승인 필요)
- approve 엔드포인트: 404 (공개 API에 없음)
"""

from dotenv import load_dotenv

load_dotenv()

from core.accounts import ACCOUNTS, get_wing_client as _get_client
from core.constants import calc_original_price, ORIGINAL_PRICE_RATIO, STATUS_MAP


# ─── 개별 상품 등록 ────────────────────────────────────

def register_product(account: str, product_data: dict) -> dict:
    """단일 상품 API 등록"""
    client = _get_client(account)
    return client.create_product(product_data)


# ─── 상품 수정 (partial) ──────────────────────────────

def update_product_fields(account: str, seller_product_id: str,
                          fields: dict) -> dict:
    """partial update로 가격/재고 등 수정.

    ※ searchTags는 이 방법으로 변경 불가 (silently 무시됨).

    Args:
        fields: {"salePrice": N, "maximumBuyCount": N, ...}
    """
    client = _get_client(account)

    # 상품 상세 조회 → vendorItemId 확보
    detail = client.get_product_by_id(seller_product_id)
    items = detail.get("data", {}).get("items", [])
    if not items:
        return {"code": "ERROR", "message": "items 없음"}

    # [세트물 안전 주의] 모든 items에 동일 fields를 적용합니다.
    # 세트물의 경우 옵션별로 가격/재고가 다를 수 있으므로,
    # 이 함수를 세트물에 사용할 때는 옵션별 차이를 반드시 확인하세요.
    patch_items = []
    for item in items:
        patch_item = {"vendorItemId": item["vendorItemId"]}
        patch_item.update(fields)
        patch_items.append(patch_item)

    body = {
        "sellerProductId": int(seller_product_id),
        "items": patch_items,
    }
    return client.patch_product(seller_product_id, body)


# ─── 상품 삭제 ─────────────────────────────────────────

def delete_products(account: str, seller_product_ids: list[str],
                    *, dry_run: bool = False) -> dict:
    """다건 상품 삭제.

    Returns:
        {"deleted": N, "error": N, "errors": [...]}
    """
    client = _get_client(account)
    result = {"deleted": 0, "error": 0, "errors": []}

    for spid in seller_product_ids:
        if dry_run:
            print(f"  [미리보기] 삭제 대상: {spid}")
            result["deleted"] += 1
            continue

        try:
            resp = client.delete_product(spid)
            code = resp.get("code", "")
            if code == "ERROR":
                msg = resp.get("message", "")
                result["errors"].append(f"{spid}: {msg}")
                result["error"] += 1
            else:
                result["deleted"] += 1
                print(f"  삭제 완료: {spid}")
        except Exception as e:
            result["errors"].append(f"{spid}: {str(e)[:80]}")
            result["error"] += 1

    return result


# ─── 상태 조회 ─────────────────────────────────────────

def check_status(account: str,
                 seller_product_ids: list[str] | None = None) -> list[dict]:
    """상품 상태 조회.

    Args:
        seller_product_ids: None이면 전체 판매중 상품 목록 반환
    """
    client = _get_client(account)

    if seller_product_ids:
        results = []
        for spid in seller_product_ids:
            try:
                detail = client.get_product_by_id(spid)
                data = detail.get("data", {})
                status = data.get("status", "UNKNOWN")
                name = data.get("sellerProductName", "")
                results.append({
                    "sellerProductId": spid,
                    "name": name,
                    "status": status,
                    "status_kr": STATUS_MAP.get(status, status),
                })
            except Exception as e:
                results.append({
                    "sellerProductId": spid,
                    "name": "",
                    "status": "ERROR",
                    "status_kr": f"조회 실패: {str(e)[:60]}",
                })
        return results
    else:
        # 전체 상품 목록
        products = client.list_selling_products(status="")  # 모든 상태
        return [{
            "sellerProductId": p["sellerProductId"],
            "name": p["name"],
            "status": p["status"],
            "status_kr": STATUS_MAP.get(p["status"], p["status"]),
            "salePrice": p.get("salePrice", 0),
        } for p in products]
