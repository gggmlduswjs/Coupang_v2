"""데이터 수집 (HTML 임포트 + Playwright 자동 수집)"""

import json
import os
import random
import re
import sys
import time
from datetime import datetime
from urllib.parse import quote

from bs4 import BeautifulSoup

from core.config import AnalysisConfig
from core.database import CoupangDB


def _fill_organic_rank(products):
    """쿠팡 배지(RankMark)가 없는 자연검색 상품에도 organic_rank를 계산해서 채운다.

    쿠팡은 1페이지 상위 10개에만 순위 배지를 표시하지만,
    2페이지 이후 자연검색 상품도 순서대로 rank를 매겨야 분석이 가능하다.
    """
    # exposure_order 순으로 정렬 후 자연검색만 순번 매기기
    sorted_products = sorted(products, key=lambda p: p.exposure_order or 0)
    rank = 1
    for p in sorted_products:
        if p.ad_type == "자연검색":
            p.organic_rank = rank
            rank += 1
from core.models import Product


# ──────────────────────────────────────────────
# 파싱 유틸 (extract_data.py에서 이식)
# ──────────────────────────────────────────────

def clean_text(text: str) -> str:
    if not text:
        return ""
    return re.sub(r'\s+', ' ', text).strip()


def parse_number(text: str):
    if not text:
        return None
    nums = re.sub(r'[^\d]', '', str(text))
    return int(nums) if nums else None


def extract_product_cards(soup: BeautifulSoup) -> list[dict]:
    """HTML 상품 카드에서 상세 데이터 추출"""
    products = []
    cards = soup.find_all("li", class_=re.compile(r"ProductUnit_productUnit"))
    if not cards:
        return products

    for order, card in enumerate(cards, 1):
        p = {"노출순서": order}
        p["vendorItemId"] = card.get("data-id", "")

        ad_mark = card.find(class_=re.compile(r"AdMark_adMark"))
        p["광고여부"] = "AD" if ad_mark else "자연검색"

        rank_el = card.find(class_=re.compile(r"RankMark_rank"))
        p["자연검색순위"] = parse_number(rank_el.get_text()) if rank_el else ""

        name_el = card.find(class_=re.compile(r"productName"))
        p["상품명"] = clean_text(name_el.get_text()) if name_el else ""

        price_area = card.find(class_=re.compile(r"PriceArea"))
        if price_area:
            original_el = price_area.find("del")
            p["정가"] = parse_number(original_el.get_text()) if original_el else ""

            all_text = price_area.get_text()
            discount_match = re.search(r'(\d+)%', all_text)
            p["할인율"] = f"{discount_match.group(1)}%" if discount_match else ""

            price_nums = re.findall(r'([\d,]+)원', all_text)
            if price_nums:
                p["판매가"] = parse_number(price_nums[-1])
            else:
                p["판매가"] = ""
        else:
            p["정가"] = ""
            p["할인율"] = ""
            p["판매가"] = ""

        rating_el = card.find(attrs={"aria-label": re.compile(r'^\d')})
        if rating_el:
            try:
                p["평점"] = float(rating_el["aria-label"])
            except (ValueError, KeyError):
                p["평점"] = ""
        else:
            p["평점"] = ""

        rating_area = card.find(class_=re.compile(r"[Pp]roduct[Rr]ating"))
        if rating_area:
            review_text = rating_area.get_text()
            review_match = re.search(r'\((\d[\d,]*)\)', review_text)
            p["리뷰수"] = parse_number(review_match.group(1)) if review_match else ""
        else:
            p["리뷰수"] = ""

        link = card.find("a", href=re.compile(r'/vp/products/'))
        if link:
            href = link.get("href", "")
            p["URL"] = "https://www.coupang.com" + href.split("&amp;")[0].split("&sourceType")[0]
            pid = re.search(r'/products/(\d+)', href)
            iid = re.search(r'itemId=(\d+)', href)
            p["productId"] = pid.group(1) if pid else ""
            p["itemId"] = iid.group(1) if iid else ""
        else:
            p["URL"] = ""
            p["productId"] = ""
            p["itemId"] = ""

        if link:
            st = re.search(r'sourceType=([a-z_]+)', link.get("href", ""))
            p["sourceType"] = st.group(1) if st else ""
        else:
            p["sourceType"] = ""

        card_html_str = str(card)
        if "txt_rocket" in card_html_str or "로켓배송" in card_html_str:
            p["배송유형"] = "로켓배송"
        elif "txt_jikgu" in card_html_str or "로켓직구" in card_html_str:
            p["배송유형"] = "로켓직구"
        elif "rocket_luxury" in card_html_str or "로켓럭셔리" in card_html_str:
            p["배송유형"] = "로켓럭셔리"
        else:
            p["배송유형"] = "마켓플레이스"

        card_text = card.get_text()
        arrival = re.search(r'(\d+/\d+\([^)]+\))\s*도착', card_text)
        p["도착예정"] = arrival.group(1) if arrival else ""

        p["무료배송"] = "O" if "무료배송" in card_text else "X"

        cashback = re.search(r'최대\s*([\d,]+)원\s*적립', card_text)
        p["적립금"] = parse_number(cashback.group(1)) if cashback else ""

        products.append(p)

    return products


def extract_keyword_from_html(soup: BeautifulSoup) -> str:
    """HTML에서 검색 키워드 추출"""
    canonical = soup.find("link", rel="canonical")
    if canonical:
        href = canonical.get("href", "")
        kw = re.search(r'[?&]q=([^&]+)', href)
        if kw:
            return kw.group(1)
    return ""


def extract_product_detail_from_html(html: str) -> dict:
    """상품 상세페이지 HTML에서 카테고리, 이미지수, SKU 추출"""
    soup = BeautifulSoup(html, "html.parser")
    result = {"category": "", "image_count": 0, "sku": ""}

    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string)
        except (json.JSONDecodeError, TypeError):
            continue
        if data.get("@type") != "Product":
            continue
        result["sku"] = data.get("sku", "")
        result["image_count"] = len(data.get("image", []))
        break

    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string)
        except (json.JSONDecodeError, TypeError):
            continue
        if data.get("@type") != "BreadcrumbList":
            continue
        items = data.get("itemListElement", [])
        flat = []
        for it in items:
            if isinstance(it, list):
                flat.extend(it)
            elif isinstance(it, dict):
                flat.append(it)
        cats = [x.get("name", "") for x in flat if isinstance(x, dict) and x.get("name")]
        result["category"] = " > ".join(cats)
        break

    return result


# ──────────────────────────────────────────────
# HTML 파일 임포트
# ──────────────────────────────────────────────

def import_html_file(filepath: str, keyword: str = "", config: AnalysisConfig = None) -> int:
    """로컬 HTML 파일을 파싱하여 DB에 저장. 저장된 상품 수 반환."""
    config = config or AnalysisConfig()
    db = CoupangDB(config)

    print(f"  파일 로딩: {filepath}")
    with open(filepath, "r", encoding="utf-8") as f:
        html = f.read()

    soup = BeautifulSoup(html, "html.parser")

    # 키워드 추출
    if not keyword:
        keyword = extract_keyword_from_html(soup)
    if not keyword:
        keyword = os.path.splitext(os.path.basename(filepath))[0]
    print(f"  키워드: {keyword}")

    # 상품 카드 파싱
    cards = extract_product_cards(soup)
    if not cards:
        print("  상품 카드를 찾을 수 없습니다.")
        db.close()
        return 0

    print(f"  파싱된 상품: {len(cards)}개")

    # DB 저장
    kw = db.get_or_create_keyword(keyword)
    snap = db.create_snapshot(kw.id, source="html_import")

    products = [Product.from_card_dict(c, snap.id, kw.id, keyword) for c in cards]
    _fill_organic_rank(products)
    db.insert_products(products)
    db.update_snapshot_count(snap.id, len(products))

    print(f"  DB 저장 완료: snapshot #{snap.id}, {len(products)}개 상품")
    db.close()
    return len(products)


# ──────────────────────────────────────────────
# Playwright 자동 수집
# ──────────────────────────────────────────────

STEALTH_JS = """
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
Object.defineProperty(navigator, 'languages', {get: () => ['ko-KR', 'ko', 'en-US', 'en']});
window.chrome = {runtime: {}};
const originalQuery = window.navigator.permissions.query;
window.navigator.permissions.query = (parameters) =>
    parameters.name === 'notifications'
        ? Promise.resolve({state: Notification.permission})
        : originalQuery(parameters);
"""


def _find_chrome_path() -> str:
    """시스템에 설치된 Chrome 경로 자동 탐색"""
    candidates = [
        os.path.expandvars(r"%ProgramFiles%\Google\Chrome\Application\chrome.exe"),
        os.path.expandvars(r"%ProgramFiles(x86)%\Google\Chrome\Application\chrome.exe"),
        os.path.expandvars(r"%LocalAppData%\Google\Chrome\Application\chrome.exe"),
    ]
    for path in candidates:
        if os.path.exists(path):
            return path
    return "chrome"


def _launch_chrome_debug(port: int = 9222) -> None:
    """Chrome을 디버깅 모드로 실행 (별도 프로필, 기존 Chrome과 충돌 방지)"""
    import subprocess
    chrome = _find_chrome_path()
    profile_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "chrome_profile")
    cmd = f'"{chrome}" --remote-debugging-port={port} --user-data-dir="{profile_dir}"'
    print(f"  Chrome 실행: {cmd}")
    subprocess.Popen(cmd, shell=True)
    time.sleep(5)


def _collect_pages(page, keyword: str, max_pages: int, snap, kw, config, db) -> int:
    """실제 페이지 수집 로직 (공통)"""
    all_products = []

    for page_num in range(1, max_pages + 1):
        url = f"https://www.coupang.com/np/search?component=&q={quote(keyword)}&page={page_num}"
        print(f"  [{page_num}/{max_pages}] {url}")

        try:
            page.goto(url, wait_until="domcontentloaded", timeout=45000)
        except Exception as e:
            print(f"    페이지 로딩 타임아웃, 현재 상태로 진행: {e}")

        # 상품 목록 대기
        try:
            page.wait_for_selector("li[class*='ProductUnit']", timeout=15000)
        except Exception:
            pass

        # 점진적 스크롤
        for _ in range(6):
            page.mouse.wheel(0, random.randint(300, 800))
            time.sleep(random.uniform(0.5, 1.2))

        time.sleep(random.uniform(1, 2))
        content = page.content()

        # Access Denied / CAPTCHA 감지
        if "Access Denied" in content or "captcha" in content.lower() or "로봇이 아닙니다" in content:
            print("    ⚠ 접근 차단 감지!")
            print("    브라우저에서 쿠팡을 직접 열어 확인 후 Enter를 누르세요.")
            try:
                input()
                content = page.content()
            except EOFError:
                print("    비대화형 환경 - 터미널에서 직접 실행해주세요.")
                break

        # HTML 캐시 저장
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        cache_path = os.path.join(config.html_cache_dir, f"{keyword}_p{page_num}_{timestamp}.html")
        with open(cache_path, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"    HTML 저장: {cache_path}")

        # 파싱
        soup = BeautifulSoup(content, "html.parser")
        cards = extract_product_cards(soup)
        if not cards:
            print(f"    상품 없음, 수집 종료")
            break

        offset = (page_num - 1) * 36
        for c in cards:
            c["노출순서"] = c["노출순서"] + offset

        products = [Product.from_card_dict(c, snap.id, kw.id, keyword) for c in cards]
        all_products.extend(products)
        print(f"    {len(cards)}개 상품 수집")

        if page_num < max_pages:
            delay = random.uniform(config.delay_min, config.delay_max)
            print(f"    대기 {delay:.1f}초...")
            time.sleep(delay)

    if all_products:
        _fill_organic_rank(all_products)
        db.insert_products(all_products)
        db.update_snapshot_count(snap.id, len(all_products))
        print(f"  수집 완료: {len(all_products)}개 상품 → DB snapshot #{snap.id}")
    else:
        print("  수집된 상품이 없습니다.")

    return len(all_products)


def collect_keyword(keyword: str, max_pages: int = 3, config: AnalysisConfig = None) -> int:
    """기존 Chrome에 연결하여 쿠팡 검색 수집.

    사용법 (2단계):
      1) Chrome을 먼저 실행:
         chrome.exe --remote-debugging-port=9222
      2) 수집 명령:
         python main.py collect --keyword "한끝" --pages 3

    Chrome이 감지되지 않으면 자동으로 실행을 시도합니다.
    """
    from playwright.sync_api import sync_playwright

    config = config or AnalysisConfig()
    db = CoupangDB(config)
    kw = db.get_or_create_keyword(keyword)
    snap = db.create_snapshot(kw.id, source="playwright", page_count=max_pages)

    cdp_url = f"http://127.0.0.1:{config.chrome_debug_port}"
    total = 0

    with sync_playwright() as pw:
        # 기존 Chrome에 CDP 연결 시도
        browser = None
        try:
            print(f"  Chrome 연결 중... (port {config.chrome_debug_port})")
            browser = pw.chromium.connect_over_cdp(cdp_url, timeout=5000)
            print("  ✓ 기존 Chrome에 연결됨")
        except Exception:
            print("  Chrome이 디버깅 모드로 실행되어 있지 않습니다.")
            print("  Chrome을 디버깅 모드로 실행합니다...")
            _launch_chrome_debug(config.chrome_debug_port)
            time.sleep(2)
            try:
                browser = pw.chromium.connect_over_cdp(cdp_url, timeout=10000)
                print("  ✓ Chrome 연결 성공")
            except Exception as e:
                print(f"  ✗ Chrome 연결 실패: {e}")
                print(f"\n  수동으로 Chrome을 실행해주세요:")
                print(f'    chrome.exe --remote-debugging-port={config.chrome_debug_port}')
                print(f"  그리고 다시 collect 명령을 실행하세요.")
                db.close()
                return 0

        # 새 탭에서 수집
        context = browser.contexts[0]
        page = context.new_page()

        try:
            total = _collect_pages(page, keyword, max_pages, snap, kw, config, db)
        finally:
            page.close()
            # CDP 연결만 해제 (Chrome 자체는 닫지 않음)
            browser.close()

    db.close()
    return total


# ──────────────────────────────────────────────
# 상품 상세 보강
# ──────────────────────────────────────────────

def enrich_products(keyword: str, top_n: int = 5, config: AnalysisConfig = None) -> int:
    """상위 N개 상품 상세페이지를 방문하여 카테고리/이미지수/SKU 보강 (Chrome CDP 연결)"""
    from playwright.sync_api import sync_playwright

    config = config or AnalysisConfig()
    db = CoupangDB(config)

    df = db.get_analysis_dataframe(keyword)
    if df.empty:
        print("  보강할 데이터가 없습니다.")
        db.close()
        return 0

    organic = df[df["ad_type"] == "자연검색"].head(top_n)
    if organic.empty:
        print("  자연검색 상품이 없습니다.")
        db.close()
        return 0

    enriched = 0
    cdp_url = f"http://127.0.0.1:{config.chrome_debug_port}"

    with sync_playwright() as pw:
        try:
            browser = pw.chromium.connect_over_cdp(cdp_url, timeout=5000)
        except Exception:
            print("  Chrome이 디버깅 모드로 실행되어 있지 않습니다.")
            print(f'  먼저 실행: chrome.exe --remote-debugging-port={config.chrome_debug_port}')
            db.close()
            return 0

        context = browser.contexts[0]
        page = context.new_page()

        try:
            for _, row in organic.iterrows():
                url = row["url"]
                if not url:
                    continue

                print(f"  상세페이지: {row['product_name'][:30]}...")
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=30000)
                    time.sleep(random.uniform(2, 4))
                    html = page.content()

                    detail = extract_product_detail_from_html(html)
                    db.update_product_enrichment(
                        int(row["id"]),
                        detail["category"],
                        detail["image_count"],
                        detail["sku"],
                    )
                    enriched += 1
                    print(f"    카테고리: {detail['category'][:40]}, 이미지: {detail['image_count']}개")

                except Exception as e:
                    print(f"    실패: {e}")

                delay = random.uniform(config.delay_min, config.delay_max)
                time.sleep(delay)
        finally:
            page.close()
            browser.close()

    print(f"  보강 완료: {enriched}개 상품")
    db.close()
    return enriched
