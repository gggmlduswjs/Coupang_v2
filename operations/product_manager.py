"""개별 상품 관리 모듈

core.api.wing_client의 저수준 메서드를 조합하여 개별 상품 단위로
상품명/검색어/태그/가격 등을 수정하고, 단일 상품 등록/삭제를 수행.

Update 전략:
- Partial (재승인 불필요): salePrice, originalPrice, quantity, outboundShippingTimeDay
  → vendor-items/{id}/prices/{price} 등 item-level API
- Full (재승인 필요): sellerProductName, searchTags, brand, manufacturer, attributes, images
  → PUT /seller-products (전체 body) → 상태 "임시저장", Wing 재승인 필요
  ※ partial update는 searchTags를 silently 무시하므로 반드시 full update
"""

import copy
import json
import sys

from dotenv import load_dotenv

load_dotenv()

from core.accounts import ACCOUNTS, get_wing_client as _get_client
from core.api.wing_client import CoupangWingClient, CoupangWingError
from core.constants import STATUS_MAP
from core.database import CoupangDB
from core.config import AnalysisConfig

# ─── Safety Locks ────────────────────────────────────────

SAFETY_LOCKS = {
    "PRICE_LOCK": False,       # True면 가격 변경 차단
    "DELETE_LOCK": False,      # True면 삭제 차단
    "REGISTER_LOCK": False,    # True면 등록 차단
    "SALE_STOP_LOCK": True,    # True면 판매중지 차단
}

# Partial update 가능 필드 (재승인 불필요)
PARTIAL_FIELDS = {
    "salePrice", "originalPrice", "quantity",
    "outboundShippingTimeDay", "deliveryCharge", "maximumBuyCount",
}

# Full update 필요 필드 (재승인 필요)
FULL_FIELDS = {
    "sellerProductName", "searchTags", "brand", "manufacturer",
    "attributes", "images", "contents",
}


def _check_lock(lock_name: str):
    """안전 잠금 확인"""
    if SAFETY_LOCKS.get(lock_name, False):
        raise PermissionError(
            f"안전 잠금 활성화: {lock_name}=True\n"
            f"product_manager.py의 SAFETY_LOCKS에서 해제 후 재시도하세요."
        )


def _get_vendor_item_ids(client: CoupangWingClient, spid: str) -> list[dict]:
    """상품의 vendorItemId 목록 반환: [{vendorItemId, salePrice, ...}, ...]"""
    detail = client.get_product_by_id(spid)
    data = detail.get("data", {})
    items = data.get("items", [])
    if not items:
        raise ValueError(f"SPID {spid}: items 없음 (상품이 존재하지 않거나 삭제됨)")
    return items


def _log(db: CoupangDB, account: str, spid: str, action: str,
         field: str = "", before: str = "", after: str = "", result_code: str = ""):
    """변경 이력 DB 기록"""
    try:
        db.log_product_change(account, spid, action, field, before, after, result_code)
    except Exception:
        pass  # 로깅 실패는 무시


# ─── 조회 함수 ──────────────────────────────────────────


def list_products(account: str, status: str = "", search: str = "",
                  limit: int = 50) -> list[dict]:
    """상품 목록 조회 (필터+검색)

    Args:
        account: 계정 코드
        status: 상태 필터 (APPROVED, DRAFT, PENDING 등, 빈 문자열=전체)
        search: 상품명 검색어 (대소문자 무시)
        limit: 최대 반환 개수

    Returns:
        [{sellerProductId, name, status, status_kr, salePrice, searchTags, brand}, ...]
    """
    client = _get_client(account)
    products = client.list_products()

    # API 응답: statusName(한국어) 기준, status(영문) 기준 양쪽 지원
    STATUS_NAME_MAP = {"승인완료": "APPROVED", "임시저장": "DRAFT", "승인대기": "PENDING"}
    if status:
        products = [
            p for p in products
            if STATUS_NAME_MAP.get(p.get("statusName", ""), p.get("status", "")) == status
        ]

    if search:
        search_lower = search.lower()
        products = [
            p for p in products
            if search_lower in p.get("sellerProductName", "").lower()
        ]

    results = []
    for p in products[:limit]:
        status_name = p.get("statusName", "")
        status_code = STATUS_NAME_MAP.get(status_name, status_name)
        results.append({
            "sellerProductId": p["sellerProductId"],
            "name": p.get("sellerProductName", ""),
            "status": status_code,
            "status_kr": status_name or STATUS_MAP.get(status_code, status_code),
            "salePrice": p.get("salePrice", 0),
            "searchTags": p.get("searchTags", []),
            "brand": p.get("brand", ""),
        })

    return results


def get_product_detail(account: str, spid: str) -> dict:
    """상품 전체 상세 조회

    Returns:
        API 원본 data 객체 전체 (sellerProductName, items, status 등)
    """
    client = _get_client(account)
    result = client.get_product_by_id(spid)
    data = result.get("data", {})
    data["status_kr"] = STATUS_MAP.get(data.get("status", ""), "")
    return data


def search_products(account: str, query: str, limit: int = 50) -> list[dict]:
    """상품명으로 검색

    Returns:
        list_products()와 동일한 형태
    """
    return list_products(account, search=query, limit=limit)


# ─── 수정 함수 ──────────────────────────────────────────


def update_product_name(account: str, spid: str, new_name: str,
                        *, dry_run: bool = False) -> dict:
    """상품명 수정 (Full Update - 재승인 필요)

    Returns:
        {"success": bool, "before": str, "after": str, "warning": str}
    """
    client = _get_client(account)
    detail = client.get_product_by_id(spid)
    data = detail.get("data", {})
    old_name = data.get("sellerProductName", "")

    result = {
        "success": False,
        "before": old_name,
        "after": new_name,
        "warning": "",
    }

    if old_name == new_name:
        result["warning"] = "변경 사항 없음 (동일한 상품명)"
        return result

    if dry_run:
        result["success"] = True
        result["warning"] = "[DRY-RUN] 실제 변경되지 않음"
        return result

    # Full update: 전체 body 수정
    body = copy.deepcopy(data)
    body["sellerProductName"] = new_name
    # [세트물 안전 주의] items의 itemName도 함께 변경
    # 세트물(items 2개 이상)의 경우, 각 옵션별 itemName이 다를 수 있으므로
    # 여기서는 old_name과 동일한 것만 교체 (일괄 덮어쓰기 금지)
    for item in body.get("items", []):
        if item.get("itemName") == old_name:
            item["itemName"] = new_name

    db = CoupangDB(AnalysisConfig())
    try:
        resp = client.update_product(spid, body)
        result["success"] = True
        result["warning"] = "상태 → 임시저장. Wing에서 재승인 필요"
        _log(db, account, spid, "update_name", "sellerProductName",
             old_name, new_name, "SUCCESS")
    except CoupangWingError as e:
        result["warning"] = f"API 오류: {e.message}"
        _log(db, account, spid, "update_name", "sellerProductName",
             old_name, new_name, f"ERROR:{e.code}")
    finally:
        db.close()

    return result


def update_product_tags(account: str, spid: str, tags: list[str],
                        *, merge: bool = False, dry_run: bool = False) -> dict:
    """검색어 태그 수정 (Full Update - 재승인 필요)

    Args:
        tags: 새 검색어 목록
        merge: True면 기존 태그와 병합, False면 교체

    Returns:
        {"success": bool, "before": list, "after": list, "warning": str}
    """
    client = _get_client(account)
    detail = client.get_product_by_id(spid)
    data = detail.get("data", {})
    items = data.get("items", [])

    old_tags = items[0].get("searchTags", []) if items else []

    if merge:
        # 새 태그 우선 병합 (중복 제거, 최대 20개)
        seen: set[str] = set()
        merged = []
        for t in tags + old_tags:
            t_clean = t.strip()
            if t_clean and t_clean.lower() not in seen:
                seen.add(t_clean.lower())
                merged.append(t_clean)
        new_tags = merged[:20]
    else:
        new_tags = [t.strip() for t in tags if t.strip()][:20]

    result = {
        "success": False,
        "before": old_tags,
        "after": new_tags,
        "warning": "",
    }

    if set(t.lower() for t in old_tags) == set(t.lower() for t in new_tags):
        result["warning"] = "변경 사항 없음 (동일한 태그)"
        return result

    if dry_run:
        result["success"] = True
        result["warning"] = "[DRY-RUN] 실제 변경되지 않음"
        return result

    # Full update: searchTags는 partial로 변경 불가
    body = copy.deepcopy(data)
    # [세트물 안전 주의] 모든 items에 동일한 searchTags를 적용하는 것은
    # 검색어 태그이므로 안전함 (검색어는 상품 레벨이지 옵션 레벨이 아님)
    for item in body.get("items", []):
        item["searchTags"] = new_tags

    db = CoupangDB(AnalysisConfig())
    try:
        resp = client.update_product(spid, body)
        result["success"] = True
        result["warning"] = "상태 → 임시저장. Wing에서 재승인 필요"
        _log(db, account, spid, "update_tags", "searchTags",
             "/".join(old_tags[:5]), "/".join(new_tags[:5]), "SUCCESS")
    except CoupangWingError as e:
        result["warning"] = f"API 오류: {e.message}"
        _log(db, account, spid, "update_tags", "searchTags",
             "/".join(old_tags[:5]), "/".join(new_tags[:5]), f"ERROR:{e.code}")
    finally:
        db.close()

    return result


def update_product_price(account: str, spid: str, price: int = 0,
                         original_price: int = 0,
                         *, dry_run: bool = False) -> dict:
    """가격 수정 (Partial - 재승인 불필요)

    Args:
        price: 판매가 (0이면 변경 안함)
        original_price: 정가/할인율기준가 (0이면 변경 안함)

    Returns:
        {"success": bool, "changes": list[dict], "warning": str}
    """
    _check_lock("PRICE_LOCK")

    client = _get_client(account)
    items = _get_vendor_item_ids(client, spid)

    changes = []
    # [세트물 안전 주의] 가격 변경 시 각 item(옵션)의 기존 가격을 개별 확인해야 함.
    # 세트물의 경우 각 옵션마다 가격이 다를 수 있으므로, 동일 가격 일괄 적용이 아닌
    # 각 item의 실제 변경 여부를 개별 판단합니다.
    for item in items:
        vid = item["vendorItemId"]
        old_price = item.get("salePrice", 0)
        old_original = item.get("originalPrice", 0)

        if price and price != old_price:
            changes.append({
                "vendorItemId": vid,
                "field": "salePrice",
                "before": old_price,
                "after": price,
            })
        if original_price and original_price != old_original:
            changes.append({
                "vendorItemId": vid,
                "field": "originalPrice",
                "before": old_original,
                "after": original_price,
            })

    result = {"success": False, "changes": changes, "warning": ""}

    if not changes:
        result["warning"] = "변경 사항 없음"
        return result

    if dry_run:
        result["success"] = True
        result["warning"] = "[DRY-RUN] 실제 변경되지 않음"
        return result

    db = CoupangDB(AnalysisConfig())
    try:
        for ch in changes:
            vid = ch["vendorItemId"]
            if ch["field"] == "salePrice":
                client.update_item_price(vid, ch["after"])
            elif ch["field"] == "originalPrice":
                client.update_original_price(vid, ch["after"])

            _log(db, account, spid, "update_price", ch["field"],
                 str(ch["before"]), str(ch["after"]), "SUCCESS")

        result["success"] = True
    except CoupangWingError as e:
        result["warning"] = f"API 오류: {e.message}"
        _log(db, account, spid, "update_price", "", "", "", f"ERROR:{e.code}")
    finally:
        db.close()

    return result


def update_product_field(account: str, spid: str, field: str, value,
                         *, dry_run: bool = False) -> dict:
    """범용 필드 수정 (전략 자동 선택)

    Partial 가능 필드: salePrice, originalPrice, quantity, outboundShippingTimeDay, maximumBuyCount
    Full 필요 필드: sellerProductName, searchTags, brand, manufacturer, ...

    Returns:
        {"success": bool, "strategy": str, "before": str, "after": str, "warning": str}
    """
    client = _get_client(account)
    detail = client.get_product_by_id(spid)
    data = detail.get("data", {})

    result = {"success": False, "strategy": "", "before": "", "after": str(value), "warning": ""}

    # 전략 자동 선택
    if field in PARTIAL_FIELDS:
        result["strategy"] = "partial"

        items = data.get("items", [])
        if not items:
            result["warning"] = "items 없음"
            return result

        old_value = items[0].get(field, "")
        result["before"] = str(old_value)

        if str(old_value) == str(value):
            result["warning"] = "변경 사항 없음"
            return result

        if dry_run:
            result["success"] = True
            result["warning"] = "[DRY-RUN] 실제 변경되지 않음 (partial update)"
            return result

        # item-level API 사용
        # [세트물 안전 주의] partial update로 모든 items에 동일 값을 적용합니다.
        # 세트물의 경우 옵션별 가격/재고가 다를 수 있으니 주의하세요.
        # 이 함수는 단품용입니다. 세트물은 개별 item별 수정이 필요합니다.
        db = CoupangDB(AnalysisConfig())
        try:
            for item in items:
                vid = item["vendorItemId"]
                if field == "salePrice":
                    client.update_item_price(vid, int(value))
                elif field == "originalPrice":
                    client.update_original_price(vid, int(value))
                elif field == "quantity":
                    client.update_item_quantity(vid, int(value))
                else:
                    # outboundShippingTimeDay 등은 patch로 처리
                    patch_body = {
                        "sellerProductId": int(spid),
                        "items": [{"vendorItemId": vid, field: value}],
                    }
                    client.patch_product(spid, patch_body)

            result["success"] = True
            _log(db, account, spid, "update_field", field,
                 str(old_value), str(value), "SUCCESS")
        except CoupangWingError as e:
            result["warning"] = f"API 오류: {e.message}"
            _log(db, account, spid, "update_field", field,
                 str(old_value), str(value), f"ERROR:{e.code}")
        finally:
            db.close()

    elif field in FULL_FIELDS:
        result["strategy"] = "full"

        # product-level 필드
        if field in ("sellerProductName", "brand", "manufacturer"):
            old_value = data.get(field, "")
        elif field in ("searchTags",):
            items = data.get("items", [])
            old_value = items[0].get("searchTags", []) if items else []
        else:
            old_value = ""

        result["before"] = str(old_value)

        if str(old_value) == str(value):
            result["warning"] = "변경 사항 없음"
            return result

        if dry_run:
            result["success"] = True
            result["warning"] = "[DRY-RUN] 실제 변경되지 않음 (full update → 재승인 필요)"
            return result

        body = copy.deepcopy(data)
        if field in ("sellerProductName", "brand", "manufacturer"):
            body[field] = value
        elif field == "searchTags":
            tags = value if isinstance(value, list) else [t.strip() for t in str(value).split("/") if t.strip()]
            for item in body.get("items", []):
                item["searchTags"] = tags[:20]

        db = CoupangDB(AnalysisConfig())
        try:
            client.update_product(spid, body)
            result["success"] = True
            result["warning"] = "상태 → 임시저장. Wing에서 재승인 필요"
            _log(db, account, spid, "update_field", field,
                 str(old_value)[:100], str(value)[:100], "SUCCESS")
        except CoupangWingError as e:
            result["warning"] = f"API 오류: {e.message}"
            _log(db, account, spid, "update_field", field,
                 str(old_value)[:100], str(value)[:100], f"ERROR:{e.code}")
        finally:
            db.close()
    else:
        result["warning"] = f"알 수 없는 필드: {field} (partial: {', '.join(sorted(PARTIAL_FIELDS))}, full: {', '.join(sorted(FULL_FIELDS))})"

    return result


# ─── 등록/삭제/판매중지 ─────────────────────────────────


def register_product(account: str, product_data: dict | str,
                     *, dry_run: bool = False) -> dict:
    """단일 상품 등록

    Args:
        product_data: API body dict 또는 JSON 파일 경로

    Returns:
        {"success": bool, "seller_product_id": str, "warning": str}
    """
    _check_lock("REGISTER_LOCK")

    if isinstance(product_data, str):
        # JSON 파일 경로
        with open(product_data, "r", encoding="utf-8") as f:
            product_data = json.load(f)

    result = {"success": False, "seller_product_id": "", "warning": ""}

    name = product_data.get("sellerProductName", "알 수 없음")

    if dry_run:
        items = product_data.get("items", [{}])
        price = items[0].get("salePrice", 0) if items else 0
        tags = items[0].get("searchTags", []) if items else []
        result["success"] = True
        result["warning"] = (
            f"[DRY-RUN] 등록 미리보기:\n"
            f"  상품명: {name}\n"
            f"  가격: {price:,}원\n"
            f"  태그: {len(tags)}개"
        )
        return result

    client = _get_client(account)
    db = CoupangDB(AnalysisConfig())
    try:
        resp = client.create_product(product_data)
        spid = str(resp.get("data", ""))
        result["success"] = True
        result["seller_product_id"] = spid
        _log(db, account, spid, "register", "", "", name, "SUCCESS")
    except CoupangWingError as e:
        result["warning"] = f"API 오류: {e.message}"
        _log(db, account, "", "register", "", "", name, f"ERROR:{e.code}")
    finally:
        db.close()

    return result


def delete_product(account: str, spid: str,
                   *, dry_run: bool = False) -> dict:
    """상품 삭제

    Returns:
        {"success": bool, "name": str, "warning": str}
    """
    _check_lock("DELETE_LOCK")

    client = _get_client(account)
    result = {"success": False, "name": "", "warning": ""}

    # 삭제 전 이름 조회
    try:
        detail = client.get_product_by_id(spid)
        name = detail.get("data", {}).get("sellerProductName", "")
        result["name"] = name
    except CoupangWingError:
        result["name"] = "(조회 실패)"

    if dry_run:
        result["success"] = True
        result["warning"] = f"[DRY-RUN] 삭제 대상: {result['name'][:60]}"
        return result

    db = CoupangDB(AnalysisConfig())
    try:
        client.delete_product(spid)
        result["success"] = True
        _log(db, account, spid, "delete", "", result["name"], "", "SUCCESS")
    except CoupangWingError as e:
        result["warning"] = f"API 오류: {e.message}"
        _log(db, account, spid, "delete", "", result["name"], "", f"ERROR:{e.code}")
    finally:
        db.close()

    return result


def stop_sale(account: str, spid: str,
              *, dry_run: bool = False) -> dict:
    """판매 중지

    Returns:
        {"success": bool, "items_stopped": int, "warning": str}
    """
    _check_lock("SALE_STOP_LOCK")

    client = _get_client(account)
    result = {"success": False, "items_stopped": 0, "warning": ""}

    items = _get_vendor_item_ids(client, spid)

    if dry_run:
        result["success"] = True
        result["items_stopped"] = len(items)
        result["warning"] = f"[DRY-RUN] {len(items)}개 아이템 판매중지 예정"
        return result

    db = CoupangDB(AnalysisConfig())
    try:
        for item in items:
            vid = item["vendorItemId"]
            client.stop_item_sale(vid)
            result["items_stopped"] += 1

        result["success"] = True
        _log(db, account, spid, "stop_sale", "", "", str(result["items_stopped"]), "SUCCESS")
    except CoupangWingError as e:
        result["warning"] = f"API 오류: {e.message} ({result['items_stopped']}개 처리 후 실패)"
        _log(db, account, spid, "stop_sale", "", "", "", f"ERROR:{e.code}")
    finally:
        db.close()

    return result


def resume_sale(account: str, spid: str,
                *, dry_run: bool = False) -> dict:
    """판매 재개

    Returns:
        {"success": bool, "items_resumed": int, "warning": str}
    """
    client = _get_client(account)
    result = {"success": False, "items_resumed": 0, "warning": ""}

    items = _get_vendor_item_ids(client, spid)

    if dry_run:
        result["success"] = True
        result["items_resumed"] = len(items)
        result["warning"] = f"[DRY-RUN] {len(items)}개 아이템 판매재개 예정"
        return result

    db = CoupangDB(AnalysisConfig())
    try:
        for item in items:
            vid = item["vendorItemId"]
            client.resume_item_sale(vid)
            result["items_resumed"] += 1

        result["success"] = True
        _log(db, account, spid, "resume_sale", "", "", str(result["items_resumed"]), "SUCCESS")
    except CoupangWingError as e:
        result["warning"] = f"API 오류: {e.message} ({result['items_resumed']}개 처리 후 실패)"
        _log(db, account, spid, "resume_sale", "", "", "", f"ERROR:{e.code}")
    finally:
        db.close()

    return result


# ─── 이력 조회 ──────────────────────────────────────────


def get_change_history(account: str, spid: str = "") -> dict:
    """변경 이력 조회 (API 이력 + 로컬 DB 이력)

    Returns:
        {"api_history": list, "local_history": list}
    """
    result = {"api_history": [], "local_history": []}

    # API 상태변경 이력
    if spid:
        try:
            client = _get_client(account)
            resp = client.get_product_history(spid)
            result["api_history"] = resp.get("data", [])
        except (CoupangWingError, Exception):
            pass

    # 로컬 DB 이력
    db = CoupangDB(AnalysisConfig())
    try:
        result["local_history"] = db.get_product_changes(
            account_code=account,
            seller_product_id=spid,
            limit=50,
        )
    finally:
        db.close()

    return result
