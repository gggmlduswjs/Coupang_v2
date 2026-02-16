"""쿠팡 API 기반 상품 관리 모듈

core.api.wing_client의 저수준 메서드를 조합하여
Excel → API 등록, 검색어 태그 업데이트, 상태 조회, 삭제, 갭 채우기를 수행.

핵심 제약사항:
- partial update: searchTags를 silently 무시 (SUCCESS 반환하나 변경 안됨)
- full update: searchTags 변경되지만 상품 상태 → "임시저장" (재승인 필요)
- approve 엔드포인트: 404 (공개 API에 없음)
"""

import os
import sys
import time

from dotenv import load_dotenv

load_dotenv(r"C:\Users\MSI\Desktop\Coupong\.env")

from core.api.wing_client import CoupangWingClient
from core.constants import calc_original_price, ORIGINAL_PRICE_RATIO
from operations.upload_excel import (
    read_products, clean_search_tag, generate_search_tags,
    compare_accounts, find_upload_files,
)


# ─── 계정 설정 ─────────────────────────────────────────

ACCOUNTS: dict[str, dict] = {
    "007-ez": {
        "vendor_id": os.getenv("COUPANG_007EZ_VENDOR_ID", ""),
        "access_key": os.getenv("COUPANG_007EZ_ACCESS_KEY", ""),
        "secret_key": os.getenv("COUPANG_007EZ_SECRET_KEY", ""),
    },
    "002-bm": {
        "vendor_id": os.getenv("COUPANG_002BM_VENDOR_ID", ""),
        "access_key": os.getenv("COUPANG_002BM_ACCESS_KEY", ""),
        "secret_key": os.getenv("COUPANG_002BM_SECRET_KEY", ""),
    },
    "007-bm": {
        "vendor_id": os.getenv("COUPANG_007BM_VENDOR_ID", ""),
        "access_key": os.getenv("COUPANG_007BM_ACCESS_KEY", ""),
        "secret_key": os.getenv("COUPANG_007BM_SECRET_KEY", ""),
    },
    "007-book": {
        "vendor_id": os.getenv("COUPANG_007BOOK_VENDOR_ID", ""),
        "access_key": os.getenv("COUPANG_007BOOK_ACCESS_KEY", ""),
        "secret_key": os.getenv("COUPANG_007BOOK_SECRET_KEY", ""),
    },
    "big6ceo": {
        "vendor_id": os.getenv("COUPANG_BIG6CEO_VENDOR_ID", ""),
        "access_key": os.getenv("COUPANG_BIG6CEO_ACCESS_KEY", ""),
        "secret_key": os.getenv("COUPANG_BIG6CEO_SECRET_KEY", ""),
    },
}


def _get_client(account: str) -> CoupangWingClient:
    """계정명으로 API 클라이언트 생성"""
    cfg = ACCOUNTS.get(account)
    if not cfg:
        raise ValueError(f"알 수 없는 계정: {account} (등록된 계정: {', '.join(ACCOUNTS.keys())})")
    if not cfg["access_key"]:
        raise ValueError(f"{account}: API 키 미설정 (.env 확인)")
    return CoupangWingClient(
        access_key=cfg["access_key"],
        secret_key=cfg["secret_key"],
        vendor_id=cfg["vendor_id"],
    )


# ─── 태그 병합 ─────────────────────────────────────────

def merge_tags(old_tags: list[str], new_tags: list[str], max_tags: int = 20) -> list[str]:
    """새 태그 우선 + 기존 태그 병합 (중복 제거, 최대 20개)"""
    merged = []
    seen: set[str] = set()
    for t in new_tags + old_tags:
        ct = clean_search_tag(t)
        if ct and ct.lower() not in seen:
            seen.add(ct.lower())
            merged.append(ct)
    return merged[:max_tags]


# ─── Excel → API 일괄등록 ──────────────────────────────

def register_from_excel(filepath: str, account: str, *,
                        dry_run: bool = False,
                        test_limit: int = 0) -> dict:
    """Excel 파일에서 상품 읽어 API로 일괄 등록.

    Args:
        filepath: Wing 업로드 Excel 경로
        account: 계정 코드
        dry_run: True면 등록하지 않고 미리보기만
        test_limit: 0이면 전체, N이면 N개만

    Returns:
        {"created": N, "skipped": N, "error": N, "errors": [...]}
    """
    client = _get_client(account)
    products = read_products(filepath)

    if test_limit > 0:
        products = products[:test_limit]

    result = {"created": 0, "skipped": 0, "error": 0, "errors": []}
    total = len(products)

    print(f"\n{'[미리보기]' if dry_run else '[등록]'} {total}개 상품 처리...")

    for i, prod in enumerate(products):
        name = prod["name"]
        if not name:
            result["skipped"] += 1
            continue

        # 필수 필드 체크
        if not prod["category"]:
            result["errors"].append(f"Row {prod['row']}: 카테고리 없음 - {name[:40]}")
            result["error"] += 1
            continue

        price = prod["price"]
        if not price or price <= 0:
            result["errors"].append(f"Row {prod['row']}: 가격 없음 - {name[:40]}")
            result["error"] += 1
            continue

        # 검색어 태그 생성
        search_str = prod.get("search", "")
        tags = [t.strip() for t in search_str.split("/") if t.strip()] if search_str else []
        if not tags:
            tags = generate_search_tags(name, prod.get("brand", ""), prod.get("category", ""))

        # API body 구성
        # BUG FIX: 1.11 하드코딩 → calc_original_price() 사용
        body = {
            "displayCategoryCode": _extract_category_code(prod["category"]),
            "sellerProductName": name,
            "vendorId": ACCOUNTS[account]["vendor_id"],
            "brand": prod.get("brand", ""),
            "manufacturer": prod.get("maker", ""),
            "items": [{
                "itemName": name,
                "originalPrice": prod.get("discount_ref") or calc_original_price(price),
                "salePrice": price,
                "maximumBuyCount": 999,
                "maximumBuyForPerson": 0,
                "outboundShippingTimeDay": prod.get("lead_time") or 1,
                "unitCount": 1,
                "searchTags": tags[:20],
                "images": [],
                "attributes": [],
                "contents": [],
            }],
            "deliveryInfo": {
                "deliveryType": "ROCKET",
                "deliveryAttributeType": "NORMAL",
            },
        }

        if prod.get("seller_code"):
            body["items"][0]["vendorItemCode"] = prod["seller_code"]
        if prod.get("barcode"):
            body["items"][0]["barcode"] = prod["barcode"]

        if dry_run:
            if result["created"] < 5:
                print(f"  [{i+1}/{total}] {name[:60]}")
                print(f"    가격: {price:,}원  태그: {len(tags)}개")
            result["created"] += 1
            continue

        try:
            resp = client.create_product(body)
            code = resp.get("code", "")
            if code == "ERROR":
                msg = resp.get("message", "알 수 없는 오류")
                result["errors"].append(f"Row {prod['row']}: {msg[:80]} - {name[:40]}")
                result["error"] += 1
            else:
                result["created"] += 1
                spid = resp.get("data", "")
                if (i + 1) <= 3 or (i + 1) % 50 == 0:
                    print(f"  [{i+1}/{total}] 등록 완료: {name[:50]} (SPID: {spid})")
        except Exception as e:
            result["errors"].append(f"Row {prod['row']}: {str(e)[:80]} - {name[:40]}")
            result["error"] += 1

        # 진행 상황 (50개마다)
        if (i + 1) % 50 == 0:
            done = result["created"] + result["skipped"] + result["error"]
            print(f"  진행: {done}/{total} (등록 {result['created']}, 건너뜀 {result['skipped']}, 오류 {result['error']})")

    return result


def _extract_category_code(category_str: str) -> int:
    """카테고리 문자열에서 코드 추출. 예: '[76218] 도서 > 참고서' → 76218"""
    import re
    m = re.search(r"\[(\d+)\]", category_str)
    if m:
        return int(m.group(1))
    # 숫자만 있으면 그대로
    stripped = category_str.strip()
    if stripped.isdigit():
        return int(stripped)
    return 0


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

STATUS_MAP = {
    "APPROVED": "승인완료(판매중)",
    "DRAFT": "임시저장",
    "PENDING": "승인대기",
    "DELETED": "삭제됨",
    "REJECTED": "반려됨",
    "UNKNOWN": "알 수 없음",
}


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


# ─── 검색어 태그 일괄 업데이트 ──────────────────────────

def update_search_tags(account: str, *,
                       dry_run: bool = False,
                       test_limit: int = 0) -> dict:
    """판매중 상품의 검색어 태그 일괄 업데이트.

    1. list_selling_products()로 판매중 상품 조회
    2. generate_search_tags()로 새 태그 생성
    3. merge_tags()로 병합
    4. patch_product()로 업데이트 시도
    5. ※ partial update는 searchTags를 무시하므로 경고 출력

    Returns:
        {"total": N, "changed": N, "skipped": N, "error": N,
         "warning": "partial update는 searchTags 무시됨"}
    """
    client = _get_client(account)

    print(f"\n[{account}] 판매중 상품 목록 조회...")
    products = client.list_selling_products(status="APPROVED")
    print(f"  {len(products)}개 상품 발견")

    if test_limit > 0:
        products = products[:test_limit]

    result = {
        "total": len(products),
        "changed": 0,
        "skipped": 0,
        "error": 0,
        "warning": "",
    }

    for i, prod in enumerate(products):
        spid = str(prod["sellerProductId"])
        name = prod["name"]
        brand = prod.get("brand", "")
        old_tags = prod.get("searchTags", [])

        # 새 태그 생성
        new_tags = generate_search_tags(name, brand)
        merged = merge_tags(old_tags, new_tags)

        # 변경 여부
        old_set = {t.lower() for t in old_tags}
        new_set = {t.lower() for t in merged}
        if old_set == new_set:
            result["skipped"] += 1
            continue

        if dry_run:
            if result["changed"] < 10:
                print(f"\n  [{i+1}] {name[:50]}")
                print(f"    기존({len(old_tags)}): {'/'.join(old_tags[:5])}...")
                print(f"    변경({len(merged)}): {'/'.join(merged[:5])}...")
                added = new_set - old_set
                if added:
                    print(f"    +추가: {', '.join(list(added)[:5])}")
            result["changed"] += 1
            continue

        # partial update 시도 (searchTags는 무시될 수 있음)
        try:
            detail = client.get_product_by_id(spid)
            items = detail.get("data", {}).get("items", [])
            if not items:
                result["error"] += 1
                continue

            patch_items = []
            for item in items:
                patch_items.append({
                    "vendorItemId": item["vendorItemId"],
                    "searchTags": merged,
                })

            body = {
                "sellerProductId": int(spid),
                "items": patch_items,
            }
            resp = client.patch_product(spid, body)
            code = resp.get("code", "")
            if code == "ERROR":
                result["error"] += 1
            else:
                result["changed"] += 1
        except Exception as e:
            result["error"] += 1
            if i == 0:
                print(f"  ※ 첫 상품 실패: {e}")

        if (i + 1) % 50 == 0:
            done = result["changed"] + result["skipped"] + result["error"]
            print(f"  진행: {done}/{result['total']}")

    # partial update searchTags 경고
    if not dry_run and result["changed"] > 0:
        result["warning"] = (
            "※ partial update는 searchTags를 silently 무시할 수 있음.\n"
            "  실제 변경 확인: python main.py upload status -a {account} --id <SPID>\n"
            "  확실한 변경: full update 필요 (상태 → 임시저장, Wing에서 재승인)"
        )

    return result


# ─── Excel ↔ API 동기화 ────────────────────────────────

def sync_status(account: str, base_dir: str, folder: str = "") -> dict:
    """Excel 상품과 API 등록 상태 비교.

    Returns:
        {"registered": N, "not_registered": N, "details": [...]}
    """
    client = _get_client(account)

    # API 판매중 상품 목록
    print(f"\n[{account}] API 상품 조회...")
    api_products = client.list_selling_products(status="")
    api_names = {p["name"].strip().lower() for p in api_products}
    print(f"  API: {len(api_products)}개")

    # Excel 상품 목록
    files = find_upload_files(base_dir, folder, count=False)
    excel_products = []
    for f in files:
        if f["account"] != account:
            continue
        try:
            products = read_products(f["path"])
            for p in products:
                excel_products.append({**p, "file": f["filename"]})
        except Exception:
            continue

    print(f"  Excel: {len(excel_products)}개")

    registered = 0
    not_registered = 0
    details = []

    for p in excel_products:
        name_lower = p["name"].strip().lower()
        is_registered = name_lower in api_names
        if is_registered:
            registered += 1
        else:
            not_registered += 1

        details.append({
            "name": p["name"],
            "file": p["file"],
            "registered": is_registered,
            "price": p.get("price"),
        })

    return {
        "registered": registered,
        "not_registered": not_registered,
        "api_total": len(api_products),
        "excel_total": len(excel_products),
        "details": details,
    }


# ─── 계정 간 갭 채우기 ─────────────────────────────────

def fill_gap(account_from: str, account_to: str, base_dir: str,
             folder: str, *, dry_run: bool = False,
             test_limit: int = 0) -> dict:
    """계정 A에만 있고 B에 없는 상품을 B의 API로 등록.

    1. compare_accounts()로 A에만 있는 상품 목록 추출
    2. A의 Excel에서 해당 상품 데이터 읽기
    3. B 계정으로 create_product() 호출

    Args:
        account_from: 기준 계정 (상품이 있는 쪽)
        account_to: 대상 계정 (빈 쪽, 여기에 등록)
        base_dir: 엑셀 디렉토리
        folder: 비교 폴더
        dry_run: True면 등록하지 않고 미리보기만
        test_limit: 0이면 전체, N이면 N개만

    Returns:
        {"gap": N, "created": N, "skipped": N, "error": N, "errors": [...]}
    """
    # 1. 갭 분석
    comp = compare_accounts(base_dir, folder, account_from, account_to)
    only_from = set(comp["only_a"])  # account_from에만 있는 상품

    result = {
        "gap": len(only_from),
        "created": 0,
        "skipped": 0,
        "error": 0,
        "errors": [],
    }

    if not only_from:
        print(f"\n  갭 없음: {account_from}과 {account_to}의 상품이 동일합니다.")
        return result

    print(f"\n  갭 발견: {account_from}에만 {len(only_from)}개 상품")

    # 2. account_from의 Excel에서 해당 상품 데이터 수집
    files = find_upload_files(base_dir, folder, count=False)
    gap_products = []
    for f in files:
        if f["account"] != account_from:
            continue
        try:
            products = read_products(f["path"])
            for p in products:
                if p["name"] in only_from:
                    gap_products.append(p)
        except Exception:
            continue

    if test_limit > 0:
        gap_products = gap_products[:test_limit]

    print(f"  Excel에서 {len(gap_products)}개 상품 데이터 확보")

    # 3. account_to로 등록
    client = _get_client(account_to)
    vendor_id = ACCOUNTS[account_to]["vendor_id"]

    for i, prod in enumerate(gap_products):
        name = prod["name"]
        price = prod["price"]
        if not price or price <= 0:
            result["skipped"] += 1
            continue

        # 검색어
        search_str = prod.get("search", "")
        tags = [t.strip() for t in search_str.split("/") if t.strip()] if search_str else []
        if not tags:
            tags = generate_search_tags(name, prod.get("brand", ""), prod.get("category", ""))

        # BUG FIX: 1.11 하드코딩 → calc_original_price() 사용
        body = {
            "displayCategoryCode": _extract_category_code(prod.get("category", "")),
            "sellerProductName": name,
            "vendorId": vendor_id,
            "brand": prod.get("brand", ""),
            "manufacturer": prod.get("maker", ""),
            "items": [{
                "itemName": name,
                "originalPrice": prod.get("discount_ref") or calc_original_price(price),
                "salePrice": price,
                "maximumBuyCount": 999,
                "maximumBuyForPerson": 0,
                "outboundShippingTimeDay": prod.get("lead_time") or 1,
                "unitCount": 1,
                "searchTags": tags[:20],
                "images": [],
                "attributes": [],
                "contents": [],
            }],
            "deliveryInfo": {
                "deliveryType": "ROCKET",
                "deliveryAttributeType": "NORMAL",
            },
        }

        if prod.get("seller_code"):
            body["items"][0]["vendorItemCode"] = prod["seller_code"]
        if prod.get("barcode"):
            body["items"][0]["barcode"] = prod["barcode"]

        if dry_run:
            if result["created"] < 10:
                print(f"  [{i+1}] {name[:60]}")
                print(f"    {account_from} → {account_to}  가격: {price:,}원")
            result["created"] += 1
            continue

        try:
            resp = client.create_product(body)
            code = resp.get("code", "")
            if code == "ERROR":
                msg = resp.get("message", "")
                result["errors"].append(f"{name[:40]}: {msg[:60]}")
                result["error"] += 1
            else:
                result["created"] += 1
                if (i + 1) <= 3 or (i + 1) % 20 == 0:
                    print(f"  [{i+1}] 등록: {name[:50]}")
        except Exception as e:
            result["errors"].append(f"{name[:40]}: {str(e)[:60]}")
            result["error"] += 1

    return result
