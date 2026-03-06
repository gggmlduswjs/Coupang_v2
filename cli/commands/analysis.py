"""분석 관련 CLI 명령: collect, import, analyze, report, strategy, full"""

from analysis.collector import import_html_file, collect_keyword, enrich_products
from analysis.analyzer import run_full_analysis, print_analysis_report
from analysis.reporter import generate_report


def cmd_collect(args, config):
    """Playwright 자동 수집"""
    print(f"\n[수집] 키워드: '{args.keyword}', 페이지: {args.pages}")
    count = collect_keyword(args.keyword, max_pages=args.pages, config=config)
    if count > 0 and args.enrich:
        print(f"\n[보강] 상위 {config.enrich_top_n}개 상세페이지 수집...")
        enrich_products(args.keyword, top_n=config.enrich_top_n, config=config)
    print(f"\n완료: {count}개 상품 수집")


def cmd_import(args, config):
    """HTML 파일 임포트"""
    print(f"\n[임포트] 파일: {args.file}")
    keyword = args.keyword or ""
    count = import_html_file(args.file, keyword=keyword, config=config)
    print(f"\n완료: {count}개 상품 임포트")


def cmd_analyze(args, config):
    """분석 실행"""
    print(f"\n[분석] 키워드: '{args.keyword}'")
    analysis = run_full_analysis(args.keyword, config=config)
    if analysis:
        print_analysis_report(analysis)
    return analysis


def cmd_report(args, config):
    """Excel 리포트 생성"""
    print(f"\n[리포트] 키워드: '{args.keyword}'")
    analysis = run_full_analysis(args.keyword, config=config)
    if analysis:
        filepath = generate_report(analysis, config=config)
        if filepath:
            print(f"\n리포트 생성 완료: {filepath}")


def cmd_strategy(args, config):
    """전략 제안"""
    print(f"\n[전략] 키워드: '{args.keyword}'")
    analysis = run_full_analysis(args.keyword, config=config)
    if not analysis:
        return

    strat = analysis.get("strategy", {})
    SEP = "=" * 65

    print(f"\n{SEP}")
    print(f"  전략 제안: '{args.keyword}'")
    print(f"{SEP}")

    if strat.get("pricing"):
        print(f"\n  [가격 전략]")
        for k, v in strat["pricing"].items():
            print(f"    {k}: {v}")

    if strat.get("keyword"):
        print(f"\n  [키워드 전략]")
        for k, v in strat["keyword"].items():
            if isinstance(v, list):
                v = ", ".join(v)
            if v:
                print(f"    {k}: {v}")

    if strat.get("review"):
        print(f"\n  [리뷰 전략]")
        for k, v in strat["review"].items():
            print(f"    {k}: {v}")

    if strat.get("delivery"):
        print(f"\n  [배송 전략]")
        for k, v in strat["delivery"].items():
            print(f"    {k}: {v}")

    if strat.get("actions"):
        print(f"\n  [액션 체크리스트]")
        for a in strat["actions"]:
            print(f"    {a['우선순위']}. {a['항목']}")
            print(f"       {a['설명']}")

    ci = strat.get("competition_index", 0)
    level = "매우 높음" if ci >= 80 else "높음" if ci >= 60 else "보통" if ci >= 40 else "낮음"
    print(f"\n  경쟁 강도: {ci}/100 ({level})")
    print(f"{SEP}")


def cmd_full(args, config):
    """전체 파이프라인: 수집 → 분석 → 리포트 → 전략"""
    print(f"\n[전체 실행] 키워드: '{args.keyword}'")

    # 수집
    if args.file:
        print(f"\n{'='*40} 1. 임포트 {'='*40}")
        import_html_file(args.file, keyword=args.keyword, config=config)
    else:
        print(f"\n{'='*40} 1. 수집 {'='*40}")
        collect_keyword(args.keyword, max_pages=args.pages, config=config)

    # 분석
    print(f"\n{'='*40} 2. 분석 {'='*40}")
    analysis = run_full_analysis(args.keyword, config=config)
    if not analysis:
        print("분석 실패. 데이터를 확인해주세요.")
        return

    print_analysis_report(analysis)

    # 리포트
    print(f"\n{'='*40} 3. 리포트 {'='*40}")
    filepath = generate_report(analysis, config=config)

    # 전략
    print(f"\n{'='*40} 4. 전략 {'='*40}")
    strat = analysis.get("strategy", {})
    if strat.get("actions"):
        for a in strat["actions"]:
            print(f"  {a['우선순위']}. {a['항목']}: {a['설명']}")

    print(f"\n{'='*65}")
    print(f"  전체 파이프라인 완료!")
    if filepath:
        print(f"  리포트: {filepath}")
    print(f"{'='*65}")
