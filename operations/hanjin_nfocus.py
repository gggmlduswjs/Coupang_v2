"""
한진 N-Focus 브라우저 자동화
============================
Playwright sync API로 N-Focus 웹사이트(Nuxt + Element UI SPA)를 자동화.
출력자료등록 → 오류체크 → 오류 건 제외 출력 → 운송장 다운로드.
"""
import io
import logging
import tempfile
from pathlib import Path
from typing import Callable, Optional

import pandas as pd
from playwright.sync_api import sync_playwright, Page, Browser, TimeoutError as PwTimeout

logger = logging.getLogger(__name__)


# ── 예외 ──

class HanjinNFocusError(Exception):
    """N-Focus 자동화 기본 예외"""

class HanjinLoginError(HanjinNFocusError):
    """로그인 실패"""

class HanjinUploadError(HanjinNFocusError):
    """업로드/등록 실패"""


class HanjinNFocusClient:
    """Playwright sync API 기반 한진 N-Focus 자동화.

    with 문으로 사용하면 자동으로 브라우저를 정리한다:
        with HanjinNFocusClient(user, pw) as client:
            client.login()
            ...
    """

    # ── URL ──
    URL_LOGIN = "https://focus.hanjin.com/login"
    URL_UPLOAD = "https://focus.hanjin.com/release/listup"

    # ── CSS 셀렉터 ──
    SEL_LOGIN_BTN = "button:has-text('로그인')"
    SEL_FORMAT_SELECT = "input.el-input__inner[placeholder='양식을 선택하세요.']"
    SEL_UPLOAD_INPUT = "#input-file0"
    SEL_ERROR_CHECK_BTN = "button:has-text('오류체크')"
    SEL_REGISTER_BTN = "button:has-text('오류 건 제외 출력')"
    SEL_DOWNLOAD_BTN = "button:has-text('엑셀파일로 다운로드')"
    SEL_DIALOG_BTNS = ".el-message-box__btns button, .el-dialog__footer button"

    # ── 타임아웃 (ms) ──
    TIMEOUT_NAV = 30_000
    TIMEOUT_ACTION = 15_000
    TIMEOUT_DOWNLOAD = 30_000

    def __init__(
        self,
        user_id: str,
        password: str,
        headless: bool = False,
        download_dir: Optional[str] = None,
    ):
        self._user_id = user_id
        self._password = password
        self._headless = headless
        self._download_dir = download_dir or tempfile.mkdtemp(prefix="nfocus_")
        self._pw = None
        self._browser: Optional[Browser] = None
        self._page: Optional[Page] = None

    # ── context manager ──

    def __enter__(self):
        self._start_browser()
        return self

    def __exit__(self, *exc):
        self.close()

    def _start_browser(self):
        self._pw = sync_playwright().start()
        self._browser = self._pw.chromium.launch(
            headless=self._headless,
            args=["--disable-blink-features=AutomationControlled"],
        )
        ctx = self._browser.new_context(
            accept_downloads=True,
            viewport={"width": 1280, "height": 900},
        )
        self._page = ctx.new_page()

    # ── 로그인 ──

    def login(self) -> bool:
        if not self._page:
            self._start_browser()
        page = self._page

        try:
            page.goto(self.URL_LOGIN, wait_until="networkidle", timeout=self.TIMEOUT_NAV)

            # Element UI는 fill()이 아닌 keyboard.type()으로 입력해야 인식
            id_input = page.wait_for_selector(
                "input.el-input__inner[type='text']", timeout=self.TIMEOUT_ACTION
            )
            pw_input = page.wait_for_selector(
                "input.el-input__inner[type='password']", timeout=self.TIMEOUT_ACTION
            )

            id_input.click()
            page.keyboard.type(self._user_id, delay=30)
            pw_input.click()
            page.keyboard.type(self._password, delay=30)

            page.locator(self.SEL_LOGIN_BTN).first.click()
            # 대시보드로 이동할 때까지 대기
            page.wait_for_url("**/dashboard**", timeout=self.TIMEOUT_NAV)

            # 중복 로그인 경고 등 팝업 닫기
            self._dismiss_dialogs()

            logger.info("N-Focus 로그인 성공")
            return True
        except PwTimeout:
            # URL 변경 대기 실패 — 팝업 때문일 수 있음
            self._dismiss_dialogs()
            if "login" not in page.url:
                logger.info("N-Focus 로그인 성공 (팝업 처리 후)")
                return True
            self._save_screenshot("login_fail")
            raise HanjinLoginError("로그인 실패: 아이디/비밀번호를 확인하세요")
        except HanjinLoginError:
            raise
        except Exception as e:
            self._save_screenshot("login_fail")
            raise HanjinLoginError(f"N-Focus 로그인 실패: {e}") from e

    # ── 출력자료등록 (엑셀 업로드) ──

    def upload_delivery_list(self, excel_bytes: bytes, filename: str) -> dict:
        page = self._page
        try:
            page.goto(self.URL_UPLOAD, wait_until="networkidle", timeout=self.TIMEOUT_NAV)
            self._dismiss_dialogs()

            # 양식 선택: "쿠팡" (사용자기본)
            self._select_coupang_format()

            # hidden file input에 파일 설정
            tmp_path = Path(self._download_dir) / filename
            tmp_path.write_bytes(excel_bytes)
            page.set_input_files(self.SEL_UPLOAD_INPUT, str(tmp_path))

            # 업로드 후 테이블 로딩 대기
            page.wait_for_selector(
                "table tbody tr, .el-table__body tbody tr",
                timeout=self.TIMEOUT_NAV,
            )
            self._dismiss_dialogs()

            logger.info(f"N-Focus 파일 업로드 완료: {filename}")
            return {"success": True, "filename": filename}
        except Exception as e:
            self._save_screenshot("upload_fail")
            raise HanjinUploadError(f"파일 업로드 실패: {e}") from e

    # ── 오류체크 ──

    def check_errors(self) -> dict:
        page = self._page
        try:
            btn = page.locator(self.SEL_ERROR_CHECK_BTN)
            btn.wait_for(state="attached", timeout=self.TIMEOUT_ACTION)
            # disabled 해제 대기
            page.wait_for_function(
                "el => !el.disabled",
                arg=btn.element_handle(),
                timeout=self.TIMEOUT_ACTION,
            )
            btn.click()
            page.wait_for_load_state("networkidle", timeout=self.TIMEOUT_NAV)
            self._dismiss_dialogs()

            normal, error, error_details = self._parse_table_counts()
            logger.info(f"N-Focus 오류체크: 정상 {normal}건, 오류 {error}건")
            return {"normal": normal, "error": error, "error_details": error_details}
        except Exception as e:
            self._save_screenshot("error_check_fail")
            raise HanjinUploadError(f"오류체크 실패: {e}") from e

    # ── 오류 건 제외 출력 ──

    def register_shipments(self, expected_normal: int = 0) -> dict:
        page = self._page
        try:
            btn = page.locator(self.SEL_REGISTER_BTN)
            btn.wait_for(state="attached", timeout=self.TIMEOUT_ACTION)
            page.wait_for_function(
                "el => !el.disabled",
                arg=btn.element_handle(),
                timeout=self.TIMEOUT_ACTION,
            )
            btn.click()
            page.wait_for_load_state("networkidle", timeout=self.TIMEOUT_NAV)
            self._dismiss_dialogs()

            logger.info(f"N-Focus 출력 완료: {expected_normal}건")
            return {"registered": expected_normal}
        except Exception as e:
            self._save_screenshot("register_fail")
            raise HanjinUploadError(f"출력 실패: {e}") from e

    # ── 운송장 엑셀 다운로드 ──

    def download_invoice_excel(self) -> bytes:
        page = self._page
        try:
            # "엑셀파일로 다운로드" 중 영문/오류건 제외
            btns = page.locator(self.SEL_DOWNLOAD_BTN).all()
            dl_btn = None
            for b in btns:
                text = b.text_content() or ""
                if "영문" not in text and "오류건" not in text:
                    dl_btn = b
                    break
            if not dl_btn:
                raise HanjinUploadError("다운로드 버튼을 찾을 수 없습니다")

            page.wait_for_function(
                "el => !el.disabled",
                arg=dl_btn.element_handle(),
                timeout=self.TIMEOUT_ACTION,
            )

            with page.expect_download(timeout=self.TIMEOUT_DOWNLOAD) as dl_info:
                dl_btn.click()
            download = dl_info.value

            save_path = Path(self._download_dir) / download.suggested_filename
            download.save_as(str(save_path))
            raw_bytes = save_path.read_bytes()
            logger.info(f"운송장 엑셀 다운로드: {save_path.name} ({len(raw_bytes):,}B)")
            return raw_bytes
        except Exception as e:
            self._save_screenshot("download_fail")
            raise HanjinUploadError(f"운송장 다운로드 실패: {e}") from e

    # ── 전체 워크플로우 ──

    def take_screenshot(self) -> Optional[bytes]:
        """현재 페이지 스크린샷을 bytes로 반환"""
        try:
            if self._page:
                return self._page.screenshot(type="png")
        except Exception:
            pass
        return None

    def process_full_workflow(
        self,
        excel_bytes: bytes,
        filename: str,
        progress_callback: Optional[Callable[[str], None]] = None,
        screenshot_callback: Optional[Callable[[bytes], None]] = None,
    ) -> dict:
        def _p(msg: str):
            logger.info(msg)
            if progress_callback:
                progress_callback(msg)

        def _ss():
            if screenshot_callback:
                img = self.take_screenshot()
                if img:
                    screenshot_callback(img)

        result = {
            "success": False,
            "normal": 0,
            "error": 0,
            "error_details": [],
            "registered": 0,
            "invoice_excel": b"",
        }

        _p("1/5 N-Focus 로그인 중...")
        self.login()
        _ss()

        _p("2/5 배송리스트 업로드 중...")
        self.upload_delivery_list(excel_bytes, filename)
        _ss()

        _p("3/5 오류 체크 중...")
        err = self.check_errors()
        result["normal"] = err["normal"]
        result["error"] = err["error"]
        result["error_details"] = err["error_details"]
        _ss()

        if result["error"] > 0:
            _p(f"오류 {result['error']}건 발견 — 정상 건만 출력합니다.")

        if result["normal"] > 0:
            _p(f"4/5 오류 건 제외 출력 중 ({result['normal']}건)...")
            reg = self.register_shipments(expected_normal=result["normal"])
            result["registered"] = reg["registered"]
            _ss()

            _p("5/5 운송장 엑셀 다운로드 중...")
            raw = self.download_invoice_excel()
            result["invoice_excel"] = self._normalize_invoice_columns(raw)
            _ss()
        else:
            _p("정상 건이 없어 출력을 건너뜁니다.")

        result["success"] = True
        _p("N-Focus 처리 완료!")
        return result

    # ── 브라우저 종료 ──

    def close(self):
        for obj in (self._page, self._browser):
            try:
                if obj:
                    obj.close()
            except Exception:
                pass
        try:
            if self._pw:
                self._pw.stop()
        except Exception:
            pass
        self._page = self._browser = self._pw = None

    # ── 내부 유틸 ──

    def _dismiss_dialogs(self):
        """Element UI 팝업 모두 닫기 (최대 3회 시도)"""
        page = self._page
        for _ in range(3):
            btns = page.query_selector_all(self.SEL_DIALOG_BTNS)
            if not btns:
                break
            for b in btns:
                try:
                    b.click()
                except Exception:
                    pass
            page.wait_for_timeout(500)

    def _select_coupang_format(self):
        """양식 드롭다운에서 '쿠팡' (사용자기본) 선택"""
        page = self._page
        try:
            fmt = page.locator(self.SEL_FORMAT_SELECT)
            fmt.click(timeout=3000)
            page.wait_for_timeout(500)

            opts = page.locator(".el-select-dropdown__item:has-text('쿠팡')").all()
            for opt in opts:
                if "사용자기본" in (opt.text_content() or ""):
                    opt.click()
                    return
            if opts:
                opts[-1].click()
        except Exception:
            logger.debug("양식 선택 건너뜀 (이미 선택되어 있을 수 있음)")

    def _parse_table_counts(self) -> tuple[int, int, list[str]]:
        """페이지의 'Total: N / 정상: N / 수정대기: N' 텍스트에서 건수 파싱."""
        import re
        page = self._page
        body = page.text_content("body") or ""

        total = 0
        normal = 0

        # "Total : 15 / 정상 : 12 / 수정대기 : 3" 패턴
        m_total = re.search(r"Total\s*:\s*(\d+)", body)
        m_normal = re.search(r"정상\s*:\s*(\d+)", body)
        if m_total:
            total = int(m_total.group(1))
        if m_normal:
            normal = int(m_normal.group(1))

        error = total - normal

        # 오류 상세: "오류" 탭의 행 텍스트 수집
        error_details: list[str] = []
        if error > 0:
            try:
                err_tab = page.locator("text=오류").first
                err_tab.click(timeout=3000)
                page.wait_for_timeout(1000)
                rows = page.query_selector_all("table tbody tr, .el-table__body tbody tr")
                for row in rows:
                    text = (row.text_content() or "").strip()
                    if text:
                        error_details.append(text[:200])
                # 다시 전체 탭으로 복귀
                page.locator("text=전체").first.click(timeout=3000)
                page.wait_for_timeout(500)
            except Exception:
                pass

        return normal, error, error_details

    def _save_screenshot(self, name: str):
        try:
            path = Path(self._download_dir) / f"{name}.png"
            self._page.screenshot(path=str(path))
            logger.warning(f"스크린샷 저장: {path}")
        except Exception:
            pass

    @staticmethod
    def _normalize_invoice_columns(raw_excel: bytes) -> bytes:
        """N-Focus 다운로드 엑셀 → STEP 4 형식으로 컬럼 정규화.

        쿠팡 양식으로 업로드했으므로 묶음배송번호, 주문번호가 있고
        운송장번호가 추가된 형태로 내려옴.
        """
        try:
            df = pd.read_excel(io.BytesIO(raw_excel))
        except Exception:
            return raw_excel

        df.columns = [str(c).strip() for c in df.columns]

        # 가능한 컬럼명 매핑
        alias = {"송장번호": "운송장번호", "운송장": "운송장번호"}
        df = df.rename(columns={k: v for k, v in alias.items() if k in df.columns})

        need = ["묶음배송번호", "주문번호", "운송장번호"]
        if all(c in df.columns for c in need):
            buf = io.BytesIO()
            df.to_excel(buf, index=False, engine="openpyxl")
            return buf.getvalue()

        logger.warning(f"컬럼 매핑 실패 — 원본 반환 (컬럼: {list(df.columns)})")
        return raw_excel
