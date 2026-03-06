"""갭 등록 + 일괄 적용 — API 기반 상품 동기화."""

import copy
import time

from core.constants import calc_original_price


# 계정별 배송/반품 설정 (API 조회 결과 캐시)
ACCOUNT_SHIPPING_INFO: dict[str, dict] = {}


def register_gap_products(source_account: str, target_account: str,
                          mapping: dict, *,
                          dry_run: bool = True,
                          test_limit: int = 0) -> dict:
    """007-ez에만 있고 타겟에 없는 상품을 API로 신규 등록.

    1. mapping["unmatched_source"]에서 미등록 상품 목록 추출
    2. 007-ez API (get_product_by_id)로 수정된 상품 상세 조회
    3. 타겟 계정 API (create_product)로 등록

    Returns: {"gap": N, "created": N, "skipped": N, "error": N, "errors": [...]}
    """
    from core.accounts import get_wing_client as _get_client, ACCOUNTS

    gap_items = mapping.get("unmatched_source", [])
    result = {
        "gap": len(gap_items),
        "created": 0,
        "skipped": 0,
        "error": 0,
        "errors": [],
    }

    if not gap_items:
        print("  갭 상품 없음")
        return result

    if test_limit > 0:
        gap_items = gap_items[:test_limit]

    source_client = _get_client(source_account)
    target_client = _get_client(target_account)
    target_vendor_id = ACCOUNTS[target_account]["vendor_id"]

    mode = "[미리보기]" if dry_run else "[등록]"
    print(f"\n{mode} 갭 상품 {len(gap_items)}개 처리...")

    for i, item in enumerate(gap_items):
        spid = item["spid"]
        name = item.get("display_name", "")

        # 소스 API에서 상품 상세 조회
        try:
            detail = source_client.get_product_by_id(str(spid))
            if detail.get("code") == "ERROR":
                result["errors"].append(f"SPID {spid}: 조회 실패 - {detail.get('message', '')[:60]}")
                result["error"] += 1
                continue

            data = detail.get("data", detail)
            body = _clone_product_body(data, target_vendor_id, target_account)

        except Exception as e:
            result["errors"].append(f"SPID {spid}: {str(e)[:60]}")
            result["error"] += 1
            continue

        prod_name = body.get("sellerProductName", name)

        if dry_run:
            if result["created"] < 10:
                items_data = body.get("items", [{}])
                price = items_data[0].get("salePrice", 0) if items_data else 0
                print(f"  [{i+1}] {prod_name[:60]}")
                print(f"    {source_account} → {target_account}  가격: {price:,}원")
            result["created"] += 1
            continue

        try:
            resp = target_client.create_product(body)
            code = resp.get("code", "")
            if code == "ERROR":
                msg = resp.get("message", "")
                result["errors"].append(f"{prod_name[:40]}: {msg[:60]}")
                result["error"] += 1
            else:
                result["created"] += 1
                new_spid = resp.get("data", "")
                if (i + 1) <= 3 or (i + 1) % 20 == 0:
                    print(f"  [{i+1}] 등록: {prod_name[:50]} (SPID: {new_spid})")
        except Exception as e:
            result["errors"].append(f"{prod_name[:40]}: {str(e)[:60]}")
            result["error"] += 1

    return result


def _get_account_shipping_info(account: str) -> dict:
    """계정의 배송/반품 설정을 API에서 조회 (캐시)."""
    if account in ACCOUNT_SHIPPING_INFO:
        return ACCOUNT_SHIPPING_INFO[account]

    from core.accounts import get_wing_client as _get_client
    client = _get_client(account)
    resp = client.get_seller_products(max_per_page=1)
    products = resp.get("data", [])
    if not products:
        return {}

    spid = products[0].get("sellerProductId", "")
    detail = client.get_product_by_id(str(spid))
    d = detail.get("data", detail)

    info = {
        "vendorUserId": d.get("vendorUserId", ""),
        "returnCenterCode": d.get("returnCenterCode", ""),
        "outboundShippingPlaceCode": d.get("outboundShippingPlaceCode", ""),
        "returnChargeName": d.get("returnChargeName", ""),
        "returnZipCode": d.get("returnZipCode", ""),
        "returnAddress": d.get("returnAddress", ""),
        "returnAddressDetail": d.get("returnAddressDetail", ""),
        "companyContactNumber": d.get("companyContactNumber", ""),
    }
    ACCOUNT_SHIPPING_INFO[account] = info
    return info


def _clone_product_body(source_detail: dict, target_vendor_id: str,
                        target_account: str = "") -> dict:
    """007-ez API 응답에서 타겟용 create_product body 구성.

    소스 상품 데이터를 전체 복제하되:
    - vendorId → 타겟 계정으로 교체
    - vendorUserId, returnCenterCode, outboundShippingPlaceCode → 타겟 것으로 교체
    - 소스 고유 ID (sellerProductId, productId, vendorItemId 등) 제거
    - 읽기전용 필드 (status, statusName, mdId 등) 제거
    """
    body = copy.deepcopy(source_detail)

    # 타겟 vendorId로 교체
    body["vendorId"] = target_vendor_id

    # 타겟 계정의 배송/반품 설정 교체
    if target_account:
        shipping = _get_account_shipping_info(target_account)
        if shipping:
            for k, v in shipping.items():
                if v:
                    body[k] = v

    # 소스 고유 ID / 읽기전용 필드 제거
    for key in [
        "sellerProductId", "productId", "categoryId", "trackingId",
        "displayProductName", "generalProductName",
        "mdId", "mdName", "statusName", "status",
        "contributorType", "requested",
        "requiredDocuments", "extraInfoMessage",
        "roleCode", "multiShippingInfos", "multiReturnInfos",
    ]:
        body.pop(key, None)

    # items에서 소스 고유 ID 제거
    for item in body.get("items", []):
        for key in [
            "sellerProductItemId", "vendorItemId", "itemId",
            "supplyPrice", "saleAgentCommission",
            "isAutoGenerated", "freePriceType",
            "bestPriceGuaranteed3P",
        ]:
            item.pop(key, None)

    return body


def apply_corrections(source_account: str, target_account: str,
                      mapping: dict, *,
                      dry_run: bool = True,
                      test_limit: int = 0,
                      include_fuzzy: bool = False,
                      skip_images: bool = False) -> dict:
    """매핑 결과를 API로 일괄 적용.

    1단계: 기존 상품 가격 수정 (PATCH - 승인 유지)
    2단계: 기존 상품 이름+이미지 수정 (PUT - 임시저장 → 재승인 필요)
    3단계: 갭 상품 신규 등록 (POST - 007-ez에서 복제)

    Returns: {
        "price_updated": N, "name_updated": N, "image_updated": N,
        "gap_created": N, "skipped": N, "error": N, "errors": [...]
    }
    """
    from core.accounts import get_wing_client as _get_client, ACCOUNTS
    from .image_syncer import _extract_image_urls

    source_client = _get_client(source_account)
    target_client = _get_client(target_account)
    target_vendor_id = ACCOUNTS[target_account]["vendor_id"]

    result = {
        "price_updated": 0, "name_updated": 0, "image_updated": 0,
        "gap_created": 0, "skipped": 0, "error": 0, "errors": [],
    }

    safe_keys = {"barcode", "registered_name"}
    if include_fuzzy:
        safe_keys.add("fuzzy")

    matched = [m for m in mapping.get("matched", [])
               if m["match_key"] in safe_keys]
    gap_items = mapping.get("unmatched_source", [])

    # 변경 대상 분류
    price_targets = [m for m in matched if m["price_changed"]]
    name_targets = [m for m in matched if m["name_changed"]]

    total_work = len(price_targets) + len(name_targets) + len(gap_items)
    if test_limit > 0:
        price_targets = price_targets[:test_limit]
        name_targets = name_targets[:test_limit]
        gap_items = gap_items[:test_limit]
        total_work = len(price_targets) + len(name_targets) + len(gap_items)

    mode = "[미리보기]" if dry_run else "[적용]"
    print(f"\n{mode} {source_account} → {target_account}")
    print(f"  가격 수정: {len(price_targets)}개 (PATCH, 승인 유지)")
    print(f"  이름 수정: {len(name_targets)}개 (PUT, 재승인 필요)")
    print(f"  갭 등록:   {len(gap_items)}개 (POST, 신규)")
    if not include_fuzzy:
        fuzzy_skipped = sum(1 for m in mapping.get("matched", [])
                           if m["match_key"] == "fuzzy"
                           and (m["price_changed"] or m["name_changed"]))
        if fuzzy_skipped:
            print(f"  퍼지 제외: {fuzzy_skipped}개 (--include-fuzzy로 포함)")

    # ── 1단계: 가격 PATCH (승인 상태 유지) ──
    if price_targets:
        print(f"\n  ── 1단계: 가격 수정 ({len(price_targets)}개) ──")
        for i, m in enumerate(price_targets):
            t_spid = m["target_spid"]
            new_price = m["source_price"]
            name = m.get("target_name", "")[:45]

            if dry_run:
                if i < 10:
                    old_p = m.get("target_price", 0) or 0
                    print(f"    [{i+1}] {name}  {old_p:,} → {new_price:,}원")
                result["price_updated"] += 1
                continue

            try:
                detail = target_client.get_product_by_id(str(t_spid))
                items = detail.get("data", {}).get("items", [])
                if not items:
                    result["errors"].append(f"SPID {t_spid}: items 없음")
                    result["error"] += 1
                    continue

                # [세트물 안전 주의] 모든 items에 동일 가격을 적용합니다.
                # 세트물의 경우 옵션별 가격이 다를 수 있으므로,
                # 동기화 대상이 세트물인지 반드시 확인하세요.
                # BUG FIX: 1.11 하드코딩 → calc_original_price() 사용
                patch_items = []
                for item in items:
                    patch_items.append({
                        "vendorItemId": item["vendorItemId"],
                        "salePrice": new_price,
                        "originalPrice": calc_original_price(new_price),
                    })

                body = {"sellerProductId": int(t_spid), "items": patch_items}
                resp = target_client.patch_product(str(t_spid), body)

                if resp.get("code") == "ERROR":
                    result["errors"].append(f"SPID {t_spid}: {resp.get('message', '')[:60]}")
                    result["error"] += 1
                else:
                    result["price_updated"] += 1
                    if (i + 1) <= 3 or (i + 1) % 50 == 0:
                        print(f"    [{i+1}] 가격: {name}  → {new_price:,}원")

                time.sleep(0.3)  # API rate limit
            except Exception as e:
                result["errors"].append(f"SPID {t_spid}: {str(e)[:60]}")
                result["error"] += 1

    # ── 2단계: 이름(+이미지) PUT (재승인 필요) ──
    if name_targets:
        print(f"\n  ── 2단계: 이름 수정 ({len(name_targets)}개) ──")
        for i, m in enumerate(name_targets):
            s_spid = m["source_spid"]
            t_spid = m["target_spid"]
            new_name = m["source_name"]
            old_name = m.get("target_name", "")[:40]

            if dry_run:
                if i < 10:
                    print(f"    [{i+1}] {old_name}")
                    print(f"         → {new_name[:55]}")
                result["name_updated"] += 1
                continue

            try:
                # 타겟 상품 상세 조회
                t_resp = target_client.get_product_by_id(str(t_spid))
                t_data = t_resp.get("data", t_resp)

                # 이름 변경
                t_data["sellerProductName"] = new_name
                # [세트물 안전 주의] 모든 items의 itemName을 동일하게 변경합니다.
                # 세트물의 경우 옵션별 itemName이 다를 수 있으므로 주의하세요.
                for item in t_data.get("items", []):
                    item["itemName"] = new_name

                # 이미지도 동기화 (skip_images가 아니면)
                if not skip_images:
                    try:
                        s_resp = source_client.get_product_by_id(str(s_spid))
                        s_data = s_resp.get("data", s_resp)
                        s_items = s_data.get("items", [])
                        t_items = t_data.get("items", [])

                        s_urls = _extract_image_urls(
                            s_items[0].get("images", []) if s_items else [])
                        t_urls = _extract_image_urls(
                            t_items[0].get("images", []) if t_items else [])

                        if s_urls != t_urls:
                            for j, t_item in enumerate(t_items):
                                if j < len(s_items):
                                    t_item["images"] = s_items[j].get("images", [])
                            result["image_updated"] += 1
                    except Exception:
                        pass  # 이미지 실패해도 이름은 진행

                # PUT body에서 읽기전용 필드 제거 (sellerProductId는 유지!)
                for key in [
                    "productId", "categoryId", "trackingId",
                    "displayProductName", "generalProductName",
                    "mdId", "mdName", "statusName",
                    "contributorType", "requested",
                    "requiredDocuments", "extraInfoMessage",
                    "roleCode", "multiShippingInfos", "multiReturnInfos",
                ]:
                    t_data.pop(key, None)
                for item in t_data.get("items", []):
                    for key in [
                        "supplyPrice", "saleAgentCommission",
                        "isAutoGenerated", "freePriceType",
                        "bestPriceGuaranteed3P",
                    ]:
                        item.pop(key, None)

                resp = target_client.update_product(str(t_spid), t_data)
                if resp.get("code") == "ERROR":
                    result["errors"].append(f"SPID {t_spid}: {resp.get('message', '')[:60]}")
                    result["error"] += 1
                else:
                    result["name_updated"] += 1
                    if (i + 1) <= 3 or (i + 1) % 50 == 0:
                        print(f"    [{i+1}] 이름+이미지: {new_name[:50]}")

                time.sleep(0.5)  # PUT은 더 느리게
            except Exception as e:
                result["errors"].append(f"SPID {t_spid}: {str(e)[:60]}")
                result["error"] += 1

    # ── 3단계: 갭 상품 등록 ──
    if gap_items:
        print(f"\n  ── 3단계: 갭 상품 등록 ({len(gap_items)}개) ──")
        for i, item in enumerate(gap_items):
            spid = item["spid"]
            name = item.get("display_name", "")

            try:
                detail = source_client.get_product_by_id(str(spid))
                if detail.get("code") == "ERROR":
                    result["errors"].append(f"SPID {spid}: 조회실패")
                    result["error"] += 1
                    continue

                data = detail.get("data", detail)
                body = _clone_product_body(data, target_vendor_id, target_account)
                prod_name = body.get("sellerProductName", name)

                if dry_run:
                    if i < 10:
                        items_data = body.get("items", [{}])
                        price = items_data[0].get("salePrice", 0) if items_data else 0
                        print(f"    [{i+1}] {prod_name[:55]}  {price:,}원")
                    result["gap_created"] += 1
                    continue

                resp = target_client.create_product(body)
                if resp.get("code") == "ERROR":
                    msg = resp.get("message", "")
                    result["errors"].append(f"{prod_name[:35]}: {msg[:50]}")
                    result["error"] += 1
                else:
                    result["gap_created"] += 1
                    if (i + 1) <= 3 or (i + 1) % 50 == 0:
                        new_spid = resp.get("data", "")
                        print(f"    [{i+1}] 등록: {prod_name[:45]} (SPID:{new_spid})")

                time.sleep(0.5)
            except Exception as e:
                result["errors"].append(f"SPID {spid}: {str(e)[:60]}")
                result["error"] += 1

    return result
