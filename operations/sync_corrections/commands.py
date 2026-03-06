"""CLI 디스패치 — sync 하위 명령 핸들러."""

import os
from datetime import datetime

import pandas as pd

from .mapper import (
    load_detailinfo, load_price_inventory, build_mapping,
    _safe_str, _safe_int,
)
from .corrector import (
    generate_corrected_detailinfo, generate_corrected_price,
    generate_mapping_report,
)
from .gap_filler import register_gap_products, apply_corrections
from .image_syncer import sync_images


def cmd_sync(args, config):
    """메인 CLI 핸들러"""
    action = getattr(args, "sync_action", None)

    if not action:
        print("\nsync 하위 명령을 지정하세요:")
        print("  map     매핑 리포트 생성 (먼저 실행)")
        print("  apply   API로 일괄 적용 (가격PATCH + 이름PUT + 갭POST)")
        print("  fix     수정 Excel 생성 (Wing 수동 업로드용)")
        print("  gap     미등록 상품 신규 등록 (API)")
        print("  images  이미지 동기화 (API)")
        return

    if action == "map":
        _cmd_map(args, config)
    elif action == "apply":
        _cmd_apply(args, config)
    elif action == "fix":
        _cmd_fix(args, config)
    elif action == "gap":
        _cmd_gap(args, config)
    elif action == "images":
        _cmd_images(args, config)


def _cmd_map(args, config):
    """매핑 리포트 생성"""
    sd_path = args.source_detail
    sp_path = args.source_price
    td_path = args.target_detail
    tp_path = args.target_price

    print(f"\n[매핑] 소스 detailinfo: {os.path.basename(sd_path)}")
    print(f"       소스 price:      {os.path.basename(sp_path)}")
    print(f"       타겟 detailinfo: {os.path.basename(td_path)}")
    print(f"       타겟 price:      {os.path.basename(tp_path)}")

    # 데이터 로드
    source_detail = load_detailinfo(sd_path)
    source_price = load_price_inventory(sp_path)
    target_detail = load_detailinfo(td_path)
    target_price = load_price_inventory(tp_path)

    # 매핑 구축
    print(f"\n  3단계 매핑 시작...")
    mapping = build_mapping(source_detail, source_price, target_detail, target_price)

    # 리포트 생성
    output = getattr(args, "output", "") or ""
    if not output:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output = os.path.join("reports", f"mapping_report_{timestamp}.xlsx")

    generate_mapping_report(mapping, output)

    # 변경 요약
    name_cnt = sum(1 for m in mapping["matched"] if m["name_changed"])
    price_cnt = sum(1 for m in mapping["matched"] if m["price_changed"])
    print(f"\n  [변경 요약]")
    print(f"    상품명 변경: {name_cnt}개")
    print(f"    가격 변경:   {price_cnt}개")
    print(f"    갭 상품:     {mapping['stats']['unmatched_source']}개")


def _cmd_apply(args, config):
    """API로 일괄 적용 (가격+이름+이미지+갭)"""
    source = args.source
    target = args.target
    report_path = args.mapping_report
    dry_run = getattr(args, "dry_run", False)
    test = getattr(args, "test", 0)
    include_fuzzy = getattr(args, "include_fuzzy", False)
    skip_images = getattr(args, "skip_images", False)

    if not os.path.exists(report_path):
        print(f"\n  매핑 리포트를 찾을 수 없습니다: {report_path}")
        print("  먼저 sync map을 실행하세요.")
        return

    # 매핑 리포트에서 데이터 읽기
    matched_items = []
    try:
        matched_df = pd.read_excel(report_path, sheet_name="매핑결과")
        for _, row in matched_df.iterrows():
            matched_items.append({
                "source_spid": _safe_str(row.get("소스_SPID", "")),
                "target_spid": _safe_str(row.get("타겟_SPID", "")),
                "match_key": _safe_str(row.get("매칭방법", "")),
                "source_name": _safe_str(row.get("소스_상품명", "")),
                "target_name": _safe_str(row.get("타겟_상품명", "")),
                "source_price": _safe_int(row.get("소스_가격")),
                "target_price": _safe_int(row.get("타겟_가격")),
                "name_changed": row.get("상품명변경") == "O",
                "price_changed": row.get("가격변경") == "O",
            })
    except Exception as e:
        print(f"\n  매핑결과 시트 읽기 실패: {e}")
        return

    gap_items = []
    try:
        gap_df = pd.read_excel(report_path, sheet_name="갭_소스에만")
        for _, row in gap_df.iterrows():
            gap_items.append({
                "spid": _safe_str(row.get("spid", "")),
                "display_name": _safe_str(row.get("display_name", "")),
                "price": _safe_int(row.get("price")),
            })
    except Exception:
        pass  # 갭 시트가 없을 수 있음

    mapping = {"matched": matched_items, "unmatched_source": gap_items}

    result = apply_corrections(
        source, target, mapping,
        dry_run=dry_run, test_limit=test,
        include_fuzzy=include_fuzzy,
        skip_images=skip_images,
    )

    # 결과 요약
    print(f"\n  {'=' * 50}")
    print(f"  [결과] {source} → {target}")
    print(f"    가격 수정: {result['price_updated']}개")
    print(f"    이름 수정: {result['name_updated']}개")
    if result['image_updated']:
        print(f"    이미지 동기화: {result['image_updated']}개")
    print(f"    갭 등록:   {result['gap_created']}개")
    if result['error']:
        print(f"    오류:      {result['error']}개")
    print(f"  {'=' * 50}")

    if result["name_updated"] > 0 and not dry_run:
        print(f"\n  ※ 이름 수정된 {result['name_updated']}개 상품은 '임시저장' 상태")
        print(f"  ※ Wing에서 일괄 재승인 필요")

    if result["errors"]:
        print(f"\n  [오류 목록]")
        for err in result["errors"][:10]:
            print(f"    - {err}")
        if len(result["errors"]) > 10:
            print(f"    ... 외 {len(result['errors']) - 10}건")


def _cmd_fix(args, config):
    """수정 Excel 생성 (Phase A)"""
    sd_path = args.source_detail
    sp_path = args.source_price
    td_path = args.target_detail
    tp_path = args.target_price
    output_dir = getattr(args, "output", "") or "corrected"
    include_fuzzy = getattr(args, "include_fuzzy", False)

    print(f"\n[수정] Excel 생성...")
    if not include_fuzzy:
        print("  ※ 퍼지 매칭은 제외 (오매핑 위험). --include-fuzzy로 포함 가능")

    # 데이터 로드 & 매핑
    source_detail = load_detailinfo(sd_path)
    source_price = load_price_inventory(sp_path)
    target_detail = load_detailinfo(td_path)
    target_price = load_price_inventory(tp_path)

    mapping = build_mapping(source_detail, source_price, target_detail, target_price)

    # 수정된 Excel 생성
    os.makedirs(output_dir, exist_ok=True)

    td_base = os.path.splitext(os.path.basename(td_path))[0]
    tp_base = os.path.splitext(os.path.basename(tp_path))[0]

    detail_out = os.path.join(output_dir, f"{td_base}_corrected.xlsx")
    price_out = os.path.join(output_dir, f"{tp_base}_corrected.xlsx")
    report_out = os.path.join(output_dir, "mapping_report.xlsx")

    generate_corrected_detailinfo(mapping, source_detail, td_path, detail_out,
                                  include_fuzzy=include_fuzzy)
    generate_corrected_price(mapping, source_price, tp_path, price_out,
                             include_fuzzy=include_fuzzy)
    generate_mapping_report(mapping, report_out)

    # 변경 요약
    safe_keys = {"barcode", "registered_name"}
    if include_fuzzy:
        safe_keys.add("fuzzy")
    name_cnt = sum(1 for m in mapping["matched"]
                   if m["name_changed"] and m["match_key"] in safe_keys)
    price_cnt = sum(1 for m in mapping["matched"]
                    if m["price_changed"] and m["match_key"] in safe_keys)
    print(f"\n  [적용된 변경]")
    print(f"    상품명 수정: {name_cnt}개")
    print(f"    가격 수정:   {price_cnt}개")
    print(f"\n  [완료] 출력 폴더: {output_dir}")


def _cmd_gap(args, config):
    """미등록 상품 신규 등록 (Phase B)"""
    source = args.source
    target = args.target
    report_path = args.mapping_report
    dry_run = getattr(args, "dry_run", False)
    test = getattr(args, "test", 0)

    mode = "[미리보기]" if dry_run else "[갭 채우기]"
    print(f"\n{mode} {source} → {target}")

    # 매핑 리포트에서 갭 상품 읽기
    if not os.path.exists(report_path):
        raise FileNotFoundError(f"매핑 리포트를 찾을 수 없습니다: {report_path}")

    gap_df = pd.read_excel(report_path, sheet_name="갭_소스에만")

    gap_items = []
    for _, row in gap_df.iterrows():
        gap_items.append({
            "spid": _safe_str(row.get("spid", "")),
            "display_name": _safe_str(row.get("display_name", "")),
            "registered_name": _safe_str(row.get("registered_name", "")),
            "barcode": _safe_str(row.get("barcode", "")),
            "price": _safe_int(row.get("price")),
        })

    mapping_for_gap = {"unmatched_source": gap_items, "matched": []}

    result = register_gap_products(
        source, target, mapping_for_gap,
        dry_run=dry_run, test_limit=test,
    )

    print(f"\n  갭 상품: {result['gap']}개")
    print(f"  등록: {result['created']}, 건너뜀: {result['skipped']}, 오류: {result['error']}")
    if result["errors"]:
        print(f"\n  [오류 목록]")
        for err in result["errors"][:5]:
            print(f"    - {err}")


def _cmd_images(args, config):
    """이미지 동기화 (Phase C)"""
    source = args.source
    target = args.target
    report_path = args.mapping_report
    dry_run = getattr(args, "dry_run", False)
    test = getattr(args, "test", 0)

    mode = "[미리보기]" if dry_run else "[이미지 동기화]"
    print(f"\n{mode} {source} → {target}")

    if not os.path.exists(report_path):
        raise FileNotFoundError(f"매핑 리포트를 찾을 수 없습니다: {report_path}")

    matched_df = pd.read_excel(report_path, sheet_name="매핑결과")

    matched_items = []
    for _, row in matched_df.iterrows():
        matched_items.append({
            "source_spid": _safe_str(row.get("소스_SPID", "")),
            "target_spid": _safe_str(row.get("타겟_SPID", "")),
            "match_key": _safe_str(row.get("매칭방법", "")),
            "source_name": _safe_str(row.get("소스_상품명", "")),
            "target_name": _safe_str(row.get("타겟_상품명", "")),
            "name_changed": row.get("상품명변경") == "O",
            "price_changed": row.get("가격변경") == "O",
        })

    mapping_for_images = {"matched": matched_items, "unmatched_source": []}

    result = sync_images(
        source, target, mapping_for_images,
        dry_run=dry_run, test_limit=test,
    )

    print(f"\n  전체: {result['total']}개")
    print(f"  변경: {result['changed']}, 동일: {result['same']}, 오류: {result['error']}")
    if result["errors"]:
        print(f"\n  [오류 목록]")
        for err in result["errors"][:5]:
            print(f"    - {err}")
    if result["changed"] > 0 and not dry_run:
        print(f"\n  ※ PUT 사용으로 상품 상태가 '임시저장'으로 변경됨")
        print(f"  ※ Wing에서 일괄 재승인이 필요합니다")
