"""이미지 동기화 — 소스→타겟 이미지 일괄 교체."""


def sync_images(source_account: str, target_account: str,
                mapping: dict, *, dry_run: bool = True,
                test_limit: int = 0) -> dict:
    """매핑된 기존 상품의 이미지를 API로 동기화.

    1. 007-ez API에서 상품 상세 → 이미지 URL 추출
    2. 타겟 API에서 상품 상세 조회
    3. 이미지가 다르면 PUT으로 교체
    ※ PUT 사용 → 상태가 '임시저장'으로 변경됨 → Wing에서 일괄 재승인 필요

    Returns: {"total": N, "changed": N, "same": N, "error": N, "errors": [...]}
    """
    from core.accounts import get_wing_client as _get_client

    result = {
        "total": 0,
        "changed": 0,
        "same": 0,
        "error": 0,
        "errors": [],
    }

    matched = mapping.get("matched", [])
    if not matched:
        print("  매핑된 상품 없음")
        return result

    # 매핑된 상품만 대상
    targets = matched
    if test_limit > 0:
        targets = targets[:test_limit]

    result["total"] = len(targets)

    source_client = _get_client(source_account)
    target_client = _get_client(target_account)

    mode = "[미리보기]" if dry_run else "[동기화]"
    print(f"\n{mode} 이미지 동기화 {len(targets)}개 상품...")

    for i, m in enumerate(targets):
        s_spid = m["source_spid"]
        t_spid = m["target_spid"]

        try:
            # 소스 상품 상세
            s_resp = source_client.get_product_by_id(str(s_spid))
            s_data = s_resp.get("data", s_resp)
            s_items = s_data.get("items", [])
            s_images = s_items[0].get("images", []) if s_items else []

            # 타겟 상품 상세
            t_resp = target_client.get_product_by_id(str(t_spid))
            t_data = t_resp.get("data", t_resp)
            t_items = t_data.get("items", [])
            t_images = t_items[0].get("images", []) if t_items else []

            # 이미지 URL 비교
            s_urls = _extract_image_urls(s_images)
            t_urls = _extract_image_urls(t_images)

            if s_urls == t_urls:
                result["same"] += 1
                continue

            name = m.get("source_name", "")[:50]

            if dry_run:
                if result["changed"] < 10:
                    print(f"  [{i+1}] {name}")
                    print(f"    소스 이미지: {len(s_images)}장 → 타겟 이미지: {len(t_images)}장")
                result["changed"] += 1
                continue

            # 타겟 상품의 이미지를 소스 이미지로 교체 (PUT = 전체 수정)
            for j, t_item in enumerate(t_items):
                if j < len(s_items):
                    t_item["images"] = s_items[j].get("images", [])

            resp = target_client.update_product(str(t_spid), t_data)
            code = resp.get("code", "")
            if code == "ERROR":
                msg = resp.get("message", "")
                result["errors"].append(f"SPID {t_spid}: {msg[:60]}")
                result["error"] += 1
            else:
                result["changed"] += 1
                if (i + 1) <= 3 or (i + 1) % 20 == 0:
                    print(f"  [{i+1}] 이미지 동기화: {name}")

        except Exception as e:
            result["errors"].append(f"SPID {t_spid}: {str(e)[:60]}")
            result["error"] += 1

    return result


def _extract_image_urls(images: list) -> list[str]:
    """이미지 리스트에서 URL만 추출하여 정렬."""
    urls = []
    for img in images:
        if isinstance(img, dict):
            url = img.get("imageUrl", img.get("cdnPath", ""))
        elif isinstance(img, str):
            url = img
        else:
            continue
        if url:
            urls.append(url)
    return sorted(urls)
