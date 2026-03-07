"""
ISBN 채우기 통합 서비스
=====================
3가지 전략으로 listings 테이블의 ISBN을 채운다:
  1. WingAPIStrategy   -- WING API get_product()에서 barcode/searchTags 추출
  2. BooksMatchStrategy -- books 테이블 제목 매칭
  3. AladinAPIStrategy  -- 알라딘 검색 API (연도/출판사 스마트 쿼리)

사용법 (서비스):
    from operations.isbn_filler import ISBNFillerService
    svc = ISBNFillerService(engine)
    result = svc.run(strategies=["wing", "books", "aladin"], account="007-book", limit=100)

사용법 (CLI):
    python scripts/fill_isbn.py --strategy wing,books,aladin --account 007-book --limit 100
"""
import os
import re
import time
import logging
from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

isbn_re = re.compile(r'97[89]\d{10}')


# --- 결과 데이터 ---

class FillResult:
    """전략 실행 결과"""
    def __init__(self, strategy: str):
        self.strategy = strategy
        self.filled = 0
        self.failed = 0
        self.skipped = 0

    @property
    def total(self):
        return self.filled + self.failed + self.skipped

    def __repr__(self):
        return f"<FillResult({self.strategy}: filled={self.filled}, failed={self.failed}, skipped={self.skipped})>"

    def to_dict(self):
        return {"strategy": self.strategy, "filled": self.filled,
                "failed": self.failed, "skipped": self.skipped}


# --- 공통 헬퍼 ---

def _update_isbn(conn, listing_id: int, account_id: int, isbn_str: str) -> bool:
    """ISBN 업데이트 (중복 체크 포함). 성공 시 True."""
    dup = conn.execute(
        text("SELECT 1 FROM listings WHERE account_id=:aid AND isbn=:isbn AND id!=:lid"),
        {"aid": account_id, "isbn": isbn_str, "lid": listing_id},
    ).first()
    if dup:
        return False
    conn.execute(
        text("UPDATE listings SET isbn=:isbn WHERE id=:lid"),
        {"isbn": isbn_str, "lid": listing_id},
    )
    return True


def _get_candidates(conn, account_name: Optional[str] = None, limit: int = 0,
                    extra_filter: str = "") -> list:
    """ISBN이 NULL인 listings 조회"""
    acct_filter = ""
    if account_name:
        acct_row = conn.execute(
            text("SELECT id FROM accounts WHERE account_name = :name"),
            {"name": account_name},
        ).first()
        if acct_row:
            acct_filter = f"AND account_id = {acct_row[0]}"

    limit_clause = f"LIMIT {limit}" if limit else ""
    return conn.execute(text(f"""
        SELECT id, account_id, product_name, coupang_product_id FROM listings
        WHERE isbn IS NULL AND product_name IS NOT NULL AND product_name != ''
        {extra_filter} {acct_filter}
        ORDER BY id {limit_clause}
    """)).fetchall()


def _clean_product_name(name: str, remove_year: bool = True) -> str:
    """상품명 정제 (검색용)"""
    t = re.sub(r'\[[^\]]*\]', '', name)
    t = re.sub(r'\([^)]*\)', '', t)
    if remove_year:
        t = re.sub(r'\d{4}년?', '', t)
    t = re.sub(r'세트\d*', '', t)
    t = re.sub(r'전\s*\d+권', '', t)
    t = re.sub(r'\s*[+&]\s*', ' ', t)
    for w in ['사은품', '선물', '증정', '포함', '무료배송', '쁘띠', '선택']:
        t = t.replace(w, '')
    return ' '.join(t.split()).strip()


# --- 전략 추상 클래스 ---

class BaseISBNStrategy(ABC):
    """ISBN 채우기 전략 추상 클래스"""
    name: str = "base"

    @abstractmethod
    def fill(self, engine: Engine, account: Optional[str] = None,
             limit: int = 0) -> FillResult:
        """전략 실행. FillResult 반환."""
        ...


# --- 전략 1: WING API ---

class WingAPIStrategy(BaseISBNStrategy):
    """WING API get_product()에서 barcode/externalVendorSku/searchTags로 ISBN 추출"""
    name = "wing"

    @staticmethod
    def _extract_isbn(detail: dict) -> str:
        data = detail.get("data", {})
        if not isinstance(data, dict):
            return ""
        isbn_set = set()
        for item in data.get("items", []):
            for field in ["barcode", "externalVendorSku"]:
                m = isbn_re.search(str(item.get(field, "")))
                if m:
                    isbn_set.add(m.group())
            for tag in (item.get("searchTags") or []):
                m = isbn_re.search(str(tag))
                if m:
                    isbn_set.add(m.group())
        return ",".join(sorted(isbn_set)) if isbn_set else ""

    def fill(self, engine: Engine, account: Optional[str] = None,
             limit: int = 0) -> FillResult:
        from core.api.wing_client import CoupangWingClient
        from core.constants import WING_ACCOUNT_ENV_MAP

        result = FillResult(self.name)
        print(f"\n=== Pass 1: WING API ISBN 추출 ===")

        with engine.connect() as conn:
            acct_filter = ""
            if account:
                acct_filter = f"AND account_name = '{account}'"
            accounts = conn.execute(text(f"""
                SELECT id, account_name, vendor_id, wing_access_key, wing_secret_key
                FROM accounts WHERE is_active=true AND wing_api_enabled=true {acct_filter}
            """)).fetchall()

            if not accounts:
                print("  활성 WING API 계정 없음")
                return result

            # 클라이언트 생성
            clients = {}
            for a in accounts:
                aid, aname, vid, ak, sk = a
                if not ak:
                    prefix = WING_ACCOUNT_ENV_MAP.get(aname, "")
                    if prefix:
                        vid = os.getenv(f"{prefix}_VENDOR_ID", vid or "")
                        ak = os.getenv(f"{prefix}_ACCESS_KEY", "")
                        sk = os.getenv(f"{prefix}_SECRET_KEY", "")
                if vid and ak and sk:
                    clients[aid] = (aname, CoupangWingClient(vid, ak, sk))

            rows = _get_candidates(
                conn, account, limit,
                extra_filter="AND coupang_product_id IS NOT NULL",
            )
            total = len(rows)
            print(f"  처리 대상: {total}건 (계정: {len(clients)}개)")

            for i, row in enumerate(rows):
                lid, aid, pname, cpid = row
                if i % 50 == 0 and i > 0:
                    print(f"  [{i}/{total}] filled={result.filled}, failed={result.failed}", flush=True)

                acct_name, client = clients.get(aid, ("?", None))
                if not client:
                    result.skipped += 1
                    continue

                try:
                    detail = client.get_product(int(cpid))
                    isbn_str = self._extract_isbn(detail)
                    if isbn_str:
                        if _update_isbn(conn, lid, aid, isbn_str):
                            result.filled += 1
                            if result.filled <= 5:
                                print(f"  [성공] [{acct_name}] {cpid} -> {isbn_str}")
                            if result.filled % 50 == 0:
                                conn.commit()
                        else:
                            result.skipped += 1
                    else:
                        result.failed += 1
                    time.sleep(0.1)
                except Exception as e:
                    result.failed += 1
                    if result.failed <= 3:
                        print(f"  [에러] {cpid}: {str(e)[:80]}")

            conn.commit()

        print(f"  Pass 1 완료: {result.to_dict()}")
        return result


# --- 전략 2: books 테이블 매칭 ---

class BooksMatchStrategy(BaseISBNStrategy):
    """books 테이블 제목 LIKE 매칭으로 ISBN 추출"""
    name = "books"

    def fill(self, engine: Engine, account: Optional[str] = None,
             limit: int = 0) -> FillResult:
        result = FillResult(self.name)
        print(f"\n=== Pass 2: books 테이블 매칭 ===")

        with engine.connect() as conn:
            rows = _get_candidates(conn, account, limit)
            total = len(rows)
            print(f"  처리 대상: {total}건")

            for i, row in enumerate(rows):
                lid, aid, pname, _ = row
                if i % 100 == 0 and i > 0:
                    print(f"  [{i}/{total}] filled={result.filled}, failed={result.failed}", flush=True)

                clean = _clean_product_name(pname)
                clean = clean.lower()
                if len(clean) < 5:
                    result.failed += 1
                    continue

                keyword = clean[:40]
                matches = conn.execute(text("""
                    SELECT DISTINCT isbn FROM books
                    WHERE LOWER(title) LIKE :kw AND isbn IS NOT NULL
                    LIMIT 3
                """), {"kw": f"%{keyword}%"}).fetchall()

                if matches:
                    isbn_str = matches[0][0]  # 첫 번째 매칭만 사용
                    if _update_isbn(conn, lid, aid, isbn_str):
                        result.filled += 1
                        if result.filled <= 5:
                            print(f"  [성공] {pname[:40]}... -> {isbn_str}")
                        if result.filled % 50 == 0:
                            conn.commit()
                    else:
                        result.skipped += 1
                else:
                    result.failed += 1

            conn.commit()

        print(f"  Pass 2 완료: {result.to_dict()}")
        return result


# --- 전략 3: 알라딘 API 검색 ---

class AladinAPIStrategy(BaseISBNStrategy):
    """알라딘 API 검색으로 ISBN 추출 (연도/출판사 스마트 쿼리)"""
    name = "aladin"

    def __init__(self, delay: float = 1.0):
        self.delay = delay

    @staticmethod
    def _extract_year(name: str) -> Optional[int]:
        m = re.search(r'(20\d{2})년?', name)
        if m:
            y = int(m.group(1))
            return y if 2020 <= y <= 2030 else None
        return None

    @staticmethod
    def _extract_publisher(name: str) -> Optional[str]:
        for pattern in [r'\[([^\]]+)\]', r'\(([^)]+)\)']:
            m = re.search(pattern, name)
            if m:
                pub = m.group(1).strip()
                if 2 <= len(pub) <= 20 and not re.search(r'^\d|권|판|세트', pub):
                    return pub
        return None

    def _search(self, crawler, product_name: str) -> Optional[str]:
        year = self._extract_year(product_name)
        publisher = self._extract_publisher(product_name)

        keyword = _clean_product_name(product_name, remove_year=(year is None))
        keyword = ' '.join(keyword.split()[:10])
        if not keyword or len(keyword) < 3:
            return None

        sort = "PublishTime" if year else "Accuracy"

        try:
            results = crawler.search_by_keyword(keyword=keyword, max_results=5, sort=sort)
            if not results:
                return None

            # 출판사+연도 필터
            if publisher:
                for item in results:
                    if publisher.lower() in item.get("publisher", "").lower():
                        if year and str(year) in item.get("pubDate", ""):
                            isbn = item.get("isbn13") or item.get("isbn")
                            if isbn:
                                return isbn
                        elif not year:
                            isbn = item.get("isbn13") or item.get("isbn")
                            if isbn:
                                return isbn

            # 연도 필터
            if year:
                for item in results:
                    if str(year) in item.get("pubDate", ""):
                        isbn = item.get("isbn13") or item.get("isbn")
                        if isbn:
                            return isbn

            # 폴백: 첫 번째 결과
            return results[0].get("isbn13") or results[0].get("isbn")

        except Exception:
            return None

    def fill(self, engine: Engine, account: Optional[str] = None,
             limit: int = 0) -> FillResult:
        result = FillResult(self.name)
        print(f"\n=== Pass 3: 알라딘 API 검색 ===")

        ttb_key = os.getenv("ALADIN_TTB_KEY")
        if not ttb_key:
            print("  ALADIN_TTB_KEY 환경변수 없음. 건너뜁니다.")
            return result

        from core.api.aladin_client import AladinAPICrawler
        crawler = AladinAPICrawler(ttb_key)

        with engine.connect() as conn:
            rows = _get_candidates(conn, account, limit)
            total = len(rows)
            print(f"  처리 대상: {total}건 ({self.delay}초/건)")

            for i, row in enumerate(rows):
                lid, aid, pname, _ = row
                if i % 20 == 0 and i > 0:
                    print(f"  [{i}/{total}] filled={result.filled}, failed={result.failed}", flush=True)

                isbn = self._search(crawler, pname)
                if isbn:
                    if _update_isbn(conn, lid, aid, isbn):
                        result.filled += 1
                        if result.filled <= 5:
                            print(f"  [성공] {pname[:40]}... -> {isbn}")
                        if result.filled % 50 == 0:
                            conn.commit()
                    else:
                        result.skipped += 1
                else:
                    result.failed += 1

                time.sleep(self.delay)

            conn.commit()

        print(f"  Pass 3 완료: {result.to_dict()}")
        return result


# --- 전략 레지스트리 ---

STRATEGY_MAP: Dict[str, type] = {
    "wing": WingAPIStrategy,
    "books": BooksMatchStrategy,
    "aladin": AladinAPIStrategy,
}


# --- 통합 서비스 ---

class ISBNFillerService:
    """ISBN 채우기 통합 서비스"""

    def __init__(self, engine: Engine):
        self.engine = engine

    def run(
        self,
        strategies: Optional[List[str]] = None,
        account: Optional[str] = None,
        limit: int = 0,
    ) -> Dict[str, Any]:
        """
        ISBN 채우기 실행.

        Args:
            strategies: 실행할 전략 목록 (기본: ["wing", "books", "aladin"])
            account: 특정 계정만 (예: "007-book")
            limit: 최대 처리 건수 (0=무제한)

        Returns:
            전략별 결과 딕셔너리
        """
        if strategies is None:
            strategies = ["wing", "books", "aladin"]

        print("=" * 60)
        print("통합 ISBN 채우기")
        print("=" * 60)
        self._print_coverage()

        results = {}
        total = FillResult("total")

        for name in strategies:
            cls = STRATEGY_MAP.get(name)
            if not cls:
                print(f"  알 수 없는 전략: {name}")
                continue

            strategy = cls()
            r = strategy.fill(self.engine, account=account, limit=limit)
            results[name] = r.to_dict()
            total.filled += r.filled
            total.failed += r.failed
            total.skipped += r.skipped

        print(f"\n=== 전체 결과 ===")
        print(f"  filled={total.filled}, failed={total.failed}, skipped={total.skipped}")
        self._print_coverage()

        results["total"] = total.to_dict()
        return results

    def _print_coverage(self):
        """계정별 ISBN 커버리지 출력"""
        with self.engine.connect() as conn:
            stats = conn.execute(text("""
                SELECT a.account_name,
                       COUNT(*) as total,
                       SUM(CASE WHEN l.isbn IS NOT NULL THEN 1 ELSE 0 END) as has_isbn
                FROM listings l
                JOIN accounts a ON l.account_id = a.id
                WHERE a.is_active = true
                GROUP BY a.account_name
            """)).fetchall()

        print("\n  ISBN 커버리지:")
        for name, total, has in stats:
            pct = (has / total * 100) if total > 0 else 0
            print(f"    {name}: {has}/{total} ({pct:.1f}%)")
