"""
Wing 셀러센터 바로가기
======================
- 로컬 PC: Playwright로 5개 계정 크롬 창 동시 자동 로그인
- Streamlit Cloud: 북마클릿 + PW 복사 방식
"""
import sys
import time
import threading
import streamlit as st
import streamlit.components.v1 as components

# 로컬(Windows) vs Streamlit Cloud 판별
_IS_LOCAL = sys.platform == "win32"


# ─────────────────────────────────────────
# 자격증명 로드
# ─────────────────────────────────────────

def _load_creds() -> dict:
    """session_state 우선 → st.secrets 폴백 (대소문자 무시)"""
    if "wing_creds_override" in st.session_state:
        return st.session_state["wing_creds_override"]
    try:
        raw = dict(st.secrets.get("wing_creds", {}))
        result = {}
        for name, val in raw.items():
            if hasattr(val, "items"):
                result[name.lower()] = {"id": val.get("id", ""), "pw": val.get("pw", "")}
            else:
                result[name.lower()] = {"id": str(val), "pw": ""}
        return result
    except Exception:
        return {}


def _match_cred(creds: dict, account_name: str) -> dict:
    return (
        creds.get(account_name)
        or creds.get(account_name.lower())
        or creds.get(account_name.upper())
        or {}
    )


# ─────────────────────────────────────────
# 로컬 전용: Playwright 자동 로그인
# ─────────────────────────────────────────

def _pw_open_one(name: str, wing_id: str, wing_pw: str, result_box: dict):
    """단일 계정 크롬 창 열기 (별도 스레드에서 실행)"""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        result_box[name] = "playwright 미설치"
        return

    pw = sync_playwright().start()
    try:
        browser = pw.chromium.launch(
            headless=False,
            channel="chrome",           # 설치된 Chrome 사용
            args=["--new-window"],
        )
        ctx = browser.new_context(no_viewport=True)
        page = ctx.new_page()
        page.goto("https://wing.coupang.com/login", wait_until="domcontentloaded")
        page.wait_for_selector("input[type='password']", timeout=15000)

        # ID 필드 — text / email / tel 순서로 첫 번째 찾기
        for sel in ["input[type='text']", "input[type='email']", "input[type='tel']"]:
            el = page.query_selector(sel)
            if el:
                el.fill(wing_id)
                break

        page.fill("input[type='password']", wing_pw)

        btn = page.query_selector("button[type='submit']")
        if btn:
            btn.click()

        page.wait_for_load_state("networkidle", timeout=15000)
        result_box[name] = "ok"

        # 브라우저 닫힐 때까지 대기 (스레드 유지)
        while True:
            try:
                if page.is_closed():
                    break
                time.sleep(2)
            except Exception:
                break

    except Exception as e:
        result_box[name] = f"오류: {e}"
    finally:
        try:
            pw.stop()
        except Exception:
            pass


def _open_all_with_playwright(creds: dict, names: list) -> tuple[int, list[str]]:
    """5개 계정 동시 실행, (성공수, 오류목록) 반환"""
    results = {}
    threads = []

    for name in names:
        c = _match_cred(creds, name)
        wid = c.get("id", "")
        wpw = c.get("pw", "")
        if not (wid and wpw):
            results[name] = "ID/PW 없음"
            continue
        t = threading.Thread(
            target=_pw_open_one,
            args=(name, wid, wpw, results),
            daemon=True,
        )
        threads.append((name, t))

    for name, t in threads:
        t.start()
        time.sleep(0.6)          # Chrome 창 순차 오픈 (동시 충돌 방지)

    # 3초 대기 후 결과 수집
    time.sleep(3)
    ok  = [n for n, r in results.items() if r == "ok"]
    err = [f"{n}: {r}" for n, r in results.items() if r != "ok"]
    return len(ok), err


# ─────────────────────────────────────────
# Cloud 전용: 북마클릿 + PW 복사 카드
# ─────────────────────────────────────────

def _card_html(account_name: str, wing_id: str, wing_pw: str) -> str:
    eid  = wing_id.replace("'", "\\'").replace("`", "\\`")
    epw  = wing_pw.replace("'", "\\'").replace("`", "\\`")
    name = account_name.replace("<", "").replace(">", "")

    bm_js = (
        "javascript:(function(){"
        "function fill(el,v){"
        "var s=Object.getOwnPropertyDescriptor(HTMLInputElement.prototype,'value').set;"
        "s.call(el,v);"
        "el.dispatchEvent(new Event('input',{bubbles:true}));"
        "el.dispatchEvent(new Event('change',{bubbles:true}));}"
        f"var t=document.querySelector('input[type=text],input[type=tel],input[type=email]');"
        f"var p=document.querySelector('input[type=password]');"
        f"if(t)fill(t,'{eid}');"
        f"if(p)fill(p,'{epw}');"
        "setTimeout(function(){"
        "var b=document.querySelector('button[type=submit]');"
        "if(b)b.click();"
        "},400);"
        "})();"
    )

    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8">
<style>
*{{box-sizing:border-box;margin:0;padding:0;font-family:sans-serif;}}
body{{padding:2px;}}
.open{{display:block;width:100%;height:42px;background:#E4002B;color:#fff;
  border:none;border-radius:8px;font-size:14px;font-weight:700;cursor:pointer;transition:opacity .15s;}}
.open:hover{{opacity:.85;}}
.row{{display:flex;align-items:center;gap:6px;margin-top:6px;}}
.bm{{flex:1;padding:7px 0;text-align:center;background:#fff3cd;color:#856404;
  border:1px dashed #ffc107;border-radius:6px;font-size:12px;font-weight:600;
  text-decoration:none;cursor:grab;white-space:nowrap;}}
.bm:hover{{background:#ffe69c;}}
.tip{{font-size:11px;color:#888;}}
.msg{{display:none;font-size:11px;color:#198754;margin-top:3px;text-align:center;}}
</style></head><body>
<button class="open" onclick="
  navigator.clipboard.writeText('{epw}').catch(()=>{{}});
  window.open('https://wing.coupang.com','_blank');
  document.getElementById('m').style.display='block';
  this.textContent='✅ 열림 — PW 복사됨';
  setTimeout(()=>this.textContent='🚀 Wing 열기 — {name}',3000);
">🚀 Wing 열기 — {name}</button>
<div id="m" class="msg">PW 복사됨 → Wing 탭에서 붙여넣기</div>
<div class="row">
  <a class="bm" href="{bm_js}" title="북마크 바에 드래그">🔖 {name} 자동로그인</a>
  <span class="tip">← 북마크 바에<br>드래그</span>
</div>
</body></html>"""


# ─────────────────────────────────────────
# 메인 render
# ─────────────────────────────────────────

def render(selected_account, accounts_df, account_names):
    st.title("🔐 Wing 셀러센터 바로가기")

    if accounts_df.empty:
        st.warning("활성 계정이 없습니다.")
        return

    creds = _load_creds()
    names = accounts_df["account_name"].tolist()

    # ── 자격증명 설정 ──────────────────────────────────────
    all_set = all(_match_cred(creds, n).get("id") and _match_cred(creds, n).get("pw") for n in names)
    with st.expander(f"⚙️ Wing 로그인 정보 설정{'  ✅' if all_set else '  ⚠️ 미설정'}", expanded=not all_set):
        st.caption("Streamlit Secrets(`wing_creds`)에 저장하면 매번 입력 불필요")
        edited = {}
        for name in names:
            ex = _match_cred(creds, name)
            c1, c2 = st.columns(2)
            with c1:
                wid = st.text_input(f"{name} ID", value=ex.get("id", ""), key=f"_wid_{name}")
            with c2:
                wpw = st.text_input(f"{name} PW", value=ex.get("pw", ""), type="password", key=f"_wpw_{name}")
            edited[name] = {"id": wid, "pw": wpw}
        if st.button("💾 저장 (이 세션)", type="primary", key="_wcred_save"):
            st.session_state["wing_creds_override"] = edited
            st.rerun()

    st.divider()
    creds = _load_creds()

    # ════════════════════════════════════════
    # 로컬 PC 모드: Playwright 동시 자동 로그인
    # ════════════════════════════════════════
    if _IS_LOCAL:
        st.subheader("🖥️ 로컬 자동 실행")

        c1, c2 = st.columns([2, 1])
        with c1:
            if st.button("🚀 5개 계정 전부 열기 (크롬 동시 실행)", type="primary", use_container_width=True, key="_open_all"):
                with st.spinner("크롬 창 열고 있어요... (5~10초)"):
                    ok_cnt, errors = _open_all_with_playwright(creds, names)
                if ok_cnt > 0:
                    st.success(f"✅ {ok_cnt}개 계정 크롬 창 열림!")
                if errors:
                    for e in errors:
                        st.warning(e)

        with c2:
            # 계정 개별 열기
            for name in names:
                c = _match_cred(creds, name)
                wid = c.get("id", "")
                wpw = c.get("pw", "")
                if wid and wpw:
                    if st.button(f"{name}", key=f"_open_one_{name}", use_container_width=True):
                        with st.spinner(f"{name} 열는 중..."):
                            res = {}
                            t = threading.Thread(target=_pw_open_one, args=(name, wid, wpw, res), daemon=True)
                            t.start()
                            time.sleep(3)
                        st.success(f"{name} 열림")

        st.divider()
        with st.expander("⚙️ ngrok 외부 접속 설정 (다른 기기에서 접속할 때)"):
            st.markdown("""
**ngrok으로 어디서든 이 대시보드 접속 가능:**

```bash
# 1. ngrok 설치 (최초 1회)
winget install ngrok

# 2. 실행 (로컬 Streamlit이 8501 포트에서 실행 중일 때)
ngrok http 8501
```

ngrok 실행 후 나오는 `https://xxxx.ngrok.io` 주소로 외부에서 접속하면
이 PC에서 크롬이 자동으로 열려요.
""")
        return   # 로컬 모드는 여기서 끝

    # ════════════════════════════════════════
    # Cloud 모드: 북마클릿 + PW 복사
    # ════════════════════════════════════════
    st.caption("☁️ Cloud에서는 브라우저를 직접 열 수 없어요. 북마클릿을 북마크 바에 드래그해두면 Wing 로그인 페이지에서 자동 입력됩니다.")

    col_count = min(len(accounts_df), 3)
    cols = st.columns(col_count)

    for i, (_, acct) in enumerate(accounts_df.iterrows()):
        name = acct["account_name"]
        vendor_id = acct.get("vendor_id", "-")
        cred = _match_cred(creds, name)
        wing_id = cred.get("id", "")
        wing_pw = cred.get("pw", "")

        with cols[i % col_count]:
            st.markdown(f"**{name}** `{vendor_id}`")
            if wing_id and wing_pw:
                components.html(_card_html(name, wing_id, wing_pw), height=110)
                st.caption(f"ID: `{wing_id}`")
            else:
                st.markdown("[Wing 열기 →](https://wing.coupang.com)")
                st.warning("⚠️ ID/PW 미설정")
            st.divider()

    with st.expander("💡 북마클릿 사용법"):
        st.markdown("""
1. 노란 **🔖 버튼**을 브라우저 북마크 바에 드래그
2. 🚀 버튼 클릭 → Wing 새 탭 열림
3. Wing 로그인 페이지에서 북마클릿 클릭 → ID/PW 자동 입력 + 로그인

**또는** Wing "로그인 유지" 체크 → 몇 주간 자동 유지
""")

    with st.expander("🔍 Secrets 진단"):
        try:
            raw_keys = list(st.secrets.get("wing_creds", {}).keys())
            st.success(f"wing_creds 키 {len(raw_keys)}개: `{raw_keys}`")
        except Exception as e:
            st.error(f"wing_creds 로드 실패: {e}")
        for _, acct in accounts_df.iterrows():
            nm = acct["account_name"]
            c = _match_cred(creds, nm)
            if c.get("id") and c.get("pw"):
                st.success(f"✅ `{nm}` → ID: `{c['id']}` / PW: {'*' * len(c['pw'])}")
            else:
                st.error(f"❌ `{nm}` → 매칭 안 됨")
