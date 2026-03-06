# 쿠팡비즈니스 - 통합 프로젝트

## 프로젝트 개요
쿠팡데이터분석 + Coupong을 통합한 쿠팡 셀러 자동화 플랫폼.
도서 소싱(알라딘) → 검색 분석 → 상품 등록/관리 → 모니터링/최적화를 하나의 시스템으로 운영.

## 프로젝트 구조
```
쿠팡비즈니스/
├── core/                # 공통 기반 (config, DB, models, API)
│   ├── config.py        # Settings (pydantic) + CoupangConfig 통합
│   ├── constants.py     # 상수, 가격 계산, 출판사 마진율
│   ├── database.py      # SQLAlchemy + PostgreSQL (Supabase)
│   ├── models/          # 15+ SQLAlchemy 모델
│   ├── api/             # WING API, Aladin API 클라이언트
│   └── services/        # WingSyncBase, TransactionManager
├── analysis/            # 수집/분석/전략/리포트
│   ├── collector.py     # Playwright 수집 + HTML 파싱
│   ├── analyzer.py      # 통계 분석 + 전략 도출
│   ├── reverse_engineer.py  # 쿠팡 시스템 역공학
│   ├── reporter.py      # Excel 리포트 생성
│   ├── bundle_generator.py  # 세트물 생성
│   ├── margin_calculator.py # 마진 계산
│   └── exposure_strategy.py # 노출 전략
├── operations/          # 상품관리/동기화/엑셀/모니터링
│   ├── product_manager.py   # 개별 상품 CRUD
│   ├── product_api.py       # 일괄 상품 관리
│   ├── sync_corrections.py  # 계정 간 동기화
│   ├── upload_excel.py      # 엑셀 파일 관리
│   ├── uploader.py          # API 상품 등록
│   ├── bundle_manager.py    # 세트물 관리 (통합)
│   └── ...                  # backup, inventory, exposure 등
├── cli/                 # CLI 인터페이스
│   ├── main.py          # argparse + dispatch (~550줄)
│   └── commands/        # 핸들러 (analysis, management, upload, product, catalog)
├── dashboard/           # Streamlit 대시보드
├── scripts/             # 운영 스크립트 (카테고리별)
├── data/                # 런타임 데이터
├── reports/             # 생성된 리포트
└── tests/               # 테스트
```

## 출처 프로젝트
- **쿠팡데이터분석**: 검색 수집/분석/리포트/노출 모니터링
- **Coupong**: 도서 소싱/상품 등록/동기화/대시보드/주문관리

## 코딩 컨벤션
- Python 3.10+, type hints 사용
- SQLAlchemy ORM 모델 (core/models/)
- Pydantic Settings 기반 설정 (core/config.py)
- 한국어 출력, 영문 코드
- `sys.stdout.reconfigure(encoding="utf-8")` Windows UTF-8 처리
- loguru 로깅

## CLI 사용법
```bash
# 분석 기능 (← 쿠팡데이터분석)
python -m cli.main collect -k "키워드" -p 3
python -m cli.main analyze -k "키워드"
python -m cli.main report -k "키워드"
python -m cli.main strategy -k "키워드"
python -m cli.main xray -k "키워드"

# 상품 관리 (← 쿠팡데이터분석)
python -m cli.main product list -a 007-ez
python -m cli.main upload register -a 007-ez -f "파일.xlsx" --dry-run

# 대시보드 (← Coupong)
streamlit run dashboard/app.py
```

## 안전 규칙

### 세트물 (필수 준수)
- items 배열에 for문으로 동일 값 적용 금지 → 옵션별 개별 처리
- 가격 수정 시 다른 옵션 영향 확인 필수
- `constants.calc_original_price()` 사용 (하드코딩 1.11 금지)

### API 수정
- dry-run 먼저, 실행은 확인 후
- 세트물/단품 구분 처리 필수
- 안전 잠금: PRICE_LOCK, DELETE_LOCK, SALE_STOP_LOCK, REGISTER_LOCK

### DB 관련
- database.py 수정 전: 참조 파일 전부 확인 후 영향도 보고
- 스키마 변경 시 Alembic 마이그레이션 필수
- data/ 디렉토리 직접 수정 금지

### 큰 변경 시
- 새 파일 3개+ / DB 스키마 변경 / API 수정 / 세트물 작업 → Plan Mode 필수
- 새 기능 전 기존 코드 검색 필수
- 새 파일 생성 전 기존 파일 수정 가능 여부 확인

## Obsidian 문서
- Vault: `G:\내 드라이브\Obsidian\10. project\쿠팡비즈니스\`
- 템플릿: `G:\내 드라이브\Obsidian\90. Settings\91. Templates\`
- **00-Index/** (프로젝트 문서)
  - `01-요구사항정의서.md` — 기능 18개, P0/P1 우선순위, 비기능 요구사항
  - `02-API-명세서.md` — WING 51메서드 + Aladin 5메서드, 인증, 에러코드
  - `03-DB-설계서.md` — 21개 모델 ERD, 테이블 스키마, 인덱스, 마이그레이션
  - `04-서비스-아키텍처.md` — 4계층 구조, 기술 스택, 데이터 흐름, 보안
  - `05-서비스-IA.md` — CLI 50+ 명령 맵, Dashboard 9페이지 구조
  - `06-개발-명세서.md` — 환경 설정, 코딩 규칙, Git 워크플로우, 테스트
  - `07-마스터플랜.md` — 9 Phase 일정, Gantt, 마일스톤, 리스크
  - `08-배포-체크리스트.md` — 환경변수, 기능 동작, 안전/보안 체크
  - `09-화면정의서.md` — CLI 출력 패턴, Streamlit 와이어프레임
- **03-Technical/** (코드리뷰)
  - `프로젝트-구조.md` — 전체 구조 (545줄)
  - `Phase2-코드리뷰.md` — core/ 상세 (1,042줄)
  - `Phase3-코드리뷰.md` — analysis/ 상세 (695줄)
  - `Phase4-코드리뷰.md` — operations/ 상세 (1,079줄)

## 주의사항
- .env, credentials, API 키 파일 커밋 금지
- Playwright 수집 시 delay_min/delay_max 준수 (차단 방지)
- 기존 프로젝트(쿠팡데이터분석, Coupong)는 Phase 8까지 그대로 유지
