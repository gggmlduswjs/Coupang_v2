"""카탈로그 매칭 CLI 명령: catalog"""


def cmd_catalog(args, config):
    """카탈로그 매칭 (반자동화)"""
    from operations.catalog_matcher import (
        prepare_catalog_worksheet, batch_match,
        review_matches, generate_catalog_report,
    )

    if not args.catalog_action:
        print("\ncatalog 하위 명령을 지정하세요: prepare, match, review, report")
        return

    if args.catalog_action == "prepare":
        prepare_catalog_worksheet(args.account, config=config)

    elif args.catalog_action == "match":
        batch_match(args.account, config=config, limit=args.limit)

    elif args.catalog_action == "review":
        review_matches(args.account, config=config)

    elif args.catalog_action == "report":
        generate_catalog_report(args.account, config=config)
