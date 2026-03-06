"""개별 상품 관리 CLI 명령: product"""

import os


def cmd_product(args, config):
    """개별 상품 관리"""
    from operations.product_manager import (
        list_products, get_product_detail, search_products,
        update_product_name, update_product_tags, update_product_price,
        update_product_field, register_product, delete_product,
        stop_sale, resume_sale, get_change_history,
    )
    from core.constants import STATUS_MAP

    action = args.product_action
    if not action:
        print("\nproduct 하위 명령을 지정하세요:")
        print("  조회: list, detail, search, history")
        print("  수정: update, update-name, update-tags, update-price")
        print("  관리: register, delete, stop, resume")
        return

    if action == "list":
        account = args.account
        try:
            products = list_products(
                account, status=args.status,
                search=args.search, limit=args.limit,
            )
        except Exception as e:
            print(f"\n  오류: {e}")
            return

        if not products:
            print(f"\n[{account}] 상품 없음")
            return

        print(f"\n[{account}] 상품 목록 ({len(products)}개)")
        print(f"\n{'SPID':<16} {'상태':<16} {'판매가':>10}  상품명")
        print("-" * 85)
        for p in products:
            price = f"{p['salePrice']:,}" if p.get("salePrice") else "-"
            name = p["name"][:45]
            print(f"{p['sellerProductId']:<16} {p['status_kr']:<16} {price:>10}  {name}")
        if len(products) >= args.limit:
            print(f"\n  ... 최대 {args.limit}개 표시 (--limit으로 조절)")

    elif action == "detail":
        try:
            data = get_product_detail(args.account, args.id)
        except Exception as e:
            print(f"\n  오류: {e}")
            return

        print(f"\n[상품 상세] SPID: {args.id}")
        print(f"  상품명: {data.get('sellerProductName', '')}")
        print(f"  상태: {data.get('status_kr', '')} ({data.get('status', '')})")
        print(f"  브랜드: {data.get('brand', '')}")
        print(f"  제조사: {data.get('manufacturer', '')}")
        print(f"  카테고리: {data.get('displayCategoryCode', '')}")

        items = data.get("items", [])
        for i, item in enumerate(items):
            print(f"\n  [아이템 {i+1}] vendorItemId: {item.get('vendorItemId', '')}")
            print(f"    판매가: {item.get('salePrice', 0):,}원")
            print(f"    정가: {item.get('originalPrice', 0):,}원")
            tags = item.get("searchTags", [])
            print(f"    검색어({len(tags)}): {'/'.join(tags[:10])}")
            if len(tags) > 10:
                print(f"           ... 외 {len(tags) - 10}개")
            print(f"    최대구매수량: {item.get('maximumBuyCount', 0)}")
            print(f"    출고리드타임: {item.get('outboundShippingTimeDay', 0)}일")

    elif action == "search":
        try:
            products = search_products(args.account, args.query, limit=args.limit)
        except Exception as e:
            print(f"\n  오류: {e}")
            return

        if not products:
            print(f"\n[{args.account}] '{args.query}' 검색 결과 없음")
            return

        print(f"\n[{args.account}] '{args.query}' 검색 결과: {len(products)}개")
        print(f"\n{'SPID':<16} {'판매가':>10}  상품명")
        print("-" * 75)
        for p in products:
            price = f"{p['salePrice']:,}" if p.get("salePrice") else "-"
            print(f"{p['sellerProductId']:<16} {price:>10}  {p['name'][:50]}")

    elif action == "update":
        dry = getattr(args, "dry_run", False)
        mode = "[미리보기]" if dry else "[수정]"
        print(f"\n{mode} {args.account} / SPID {args.id} / {args.field} = {args.value}")

        try:
            result = update_product_field(
                args.account, args.id, args.field, args.value, dry_run=dry,
            )
        except (PermissionError, Exception) as e:
            print(f"\n  오류: {e}")
            return

        if result["success"]:
            print(f"  전략: {result['strategy']}")
            print(f"  변경: {result['before']} → {result['after']}")
        if result.get("warning"):
            print(f"  ⚠ {result['warning']}")

    elif action == "update-name":
        dry = getattr(args, "dry_run", False)
        mode = "[미리보기]" if dry else "[상품명 수정]"
        print(f"\n{mode} {args.account} / SPID {args.id}")

        try:
            result = update_product_name(
                args.account, args.id, args.name, dry_run=dry,
            )
        except Exception as e:
            print(f"\n  오류: {e}")
            return

        if result["success"]:
            print(f"  변경: {result['before'][:60]}")
            print(f"     → {result['after'][:60]}")
        if result.get("warning"):
            print(f"  ⚠ {result['warning']}")

    elif action == "update-tags":
        dry = getattr(args, "dry_run", False)
        mode = "[미리보기]" if dry else "[태그 수정]"
        tags = [t.strip() for t in args.tags.split("/") if t.strip()]
        merge = getattr(args, "merge", False)
        print(f"\n{mode} {args.account} / SPID {args.id} ({'병합' if merge else '교체'})")

        try:
            result = update_product_tags(
                args.account, args.id, tags, merge=merge, dry_run=dry,
            )
        except Exception as e:
            print(f"\n  오류: {e}")
            return

        if result["success"]:
            before_str = "/".join(result["before"][:5])
            after_str = "/".join(result["after"][:5])
            print(f"  기존({len(result['before'])}): {before_str}...")
            print(f"  변경({len(result['after'])}): {after_str}...")
        if result.get("warning"):
            print(f"  ⚠ {result['warning']}")

    elif action == "update-price":
        dry = getattr(args, "dry_run", False)
        mode = "[미리보기]" if dry else "[가격 수정]"
        price = getattr(args, "price", 0) or 0
        orig_price = getattr(args, "original_price", 0) or 0
        print(f"\n{mode} {args.account} / SPID {args.id}")

        if not price and not orig_price:
            print("  --price 또는 --original-price를 지정하세요.")
            return

        try:
            result = update_product_price(
                args.account, args.id, price=price,
                original_price=orig_price, dry_run=dry,
            )
        except (PermissionError, Exception) as e:
            print(f"\n  오류: {e}")
            return

        if result["changes"]:
            for ch in result["changes"]:
                print(f"  {ch['field']}: {ch['before']:,} → {ch['after']:,}")
        if result.get("warning"):
            print(f"  ⚠ {result['warning']}")

    elif action == "register":
        dry = getattr(args, "dry_run", False)
        json_path = getattr(args, "json", "")
        mode = "[미리보기]" if dry else "[등록]"
        print(f"\n{mode} {args.account} / JSON: {json_path}")

        if not os.path.exists(json_path):
            print(f"  파일을 찾을 수 없습니다: {json_path}")
            return

        try:
            result = register_product(args.account, json_path, dry_run=dry)
        except (PermissionError, Exception) as e:
            print(f"\n  오류: {e}")
            return

        if result["success"]:
            if result.get("seller_product_id"):
                print(f"  등록 완료! SPID: {result['seller_product_id']}")
        if result.get("warning"):
            print(f"  {result['warning']}")

    elif action == "delete":
        dry = getattr(args, "dry_run", False)
        mode = "[미리보기]" if dry else "[삭제]"
        print(f"\n{mode} {args.account} / SPID {args.id}")

        try:
            result = delete_product(args.account, args.id, dry_run=dry)
        except (PermissionError, Exception) as e:
            print(f"\n  오류: {e}")
            return

        if result["success"]:
            print(f"  삭제{'(미리보기)' if dry else ''}: {result['name'][:60]}")
        if result.get("warning"):
            print(f"  ⚠ {result['warning']}")

    elif action == "stop":
        dry = getattr(args, "dry_run", False)
        mode = "[미리보기]" if dry else "[판매중지]"
        print(f"\n{mode} {args.account} / SPID {args.id}")

        try:
            result = stop_sale(args.account, args.id, dry_run=dry)
        except (PermissionError, Exception) as e:
            print(f"\n  오류: {e}")
            return

        if result["success"]:
            print(f"  {result['items_stopped']}개 아이템 판매중지")
        if result.get("warning"):
            print(f"  ⚠ {result['warning']}")

    elif action == "resume":
        dry = getattr(args, "dry_run", False)
        mode = "[미리보기]" if dry else "[판매재개]"
        print(f"\n{mode} {args.account} / SPID {args.id}")

        try:
            result = resume_sale(args.account, args.id, dry_run=dry)
        except Exception as e:
            print(f"\n  오류: {e}")
            return

        if result["success"]:
            print(f"  {result['items_resumed']}개 아이템 판매재개")
        if result.get("warning"):
            print(f"  ⚠ {result['warning']}")

    elif action == "history":
        spid = getattr(args, "id", "") or ""
        print(f"\n[변경이력] {args.account}" + (f" / SPID {spid}" if spid else ""))

        try:
            hist = get_change_history(args.account, spid)
        except Exception as e:
            print(f"\n  오류: {e}")
            return

        # API 이력
        api_hist = hist.get("api_history", [])
        if api_hist:
            print(f"\n  [API 상태변경 이력] ({len(api_hist)}건)")
            for h in api_hist[:10]:
                print(f"    {h}")

        # 로컬 이력
        local_hist = hist.get("local_history", [])
        if local_hist:
            print(f"\n  [로컬 변경 이력] ({len(local_hist)}건)")
            print(f"  {'시간':<20} {'작업':<15} {'필드':<20} {'결과':<10}")
            print("  " + "-" * 70)
            for h in local_hist:
                t = h.get("changed_at", "")[:19]
                act = h.get("action", "")
                field = h.get("field", "")
                code = h.get("result_code", "")
                print(f"  {t:<20} {act:<15} {field:<20} {code:<10}")
                before = h.get("before_value", "")
                after = h.get("after_value", "")
                if before or after:
                    print(f"    {before[:40]} → {after[:40]}")
        elif not api_hist:
            print("\n  변경 이력 없음")
