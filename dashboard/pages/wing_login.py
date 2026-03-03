"""
Wing 셀러센터 바로가기
======================
wing:// 커스텀 프로토콜로 Chrome 자동 열기
Wing자동로그인.exe 최초 1회 실행으로 등록
"""
import sys
import json
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
# wing:// 프로토콜 버튼 + 북마클릿 카드
# ─────────────────────────────────────────

def _launcher_buttons_html(names: list) -> str:
    """
    wing:// 커스텀 프로토콜 — Wing자동로그인.exe가 등록되어 있으면
    버튼 클릭 시 Chrome이 바로 열림 (서버 불필요)
    """
    names_js = json.dumps(names)
    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8">
<style>
*{{box-sizing:border-box;margin:0;padding:0;font-family:sans-serif;font-size:14px;}}
body{{padding:4px;}}
.info{{padding:6px 10px;border-radius:6px;margin-bottom:8px;font-size:12px;
  background:#d1e7dd;color:#0a3622;}}
.grid{{display:flex;flex-wrap:wrap;gap:6px;}}
.btn-all{{width:100%;height:44px;background:#E4002B;color:#fff;border:none;
  border-radius:8px;font-size:15px;font-weight:700;cursor:pointer;margin-bottom:8px;}}
.btn-all:hover{{opacity:.85;}}
.btn-one{{flex:1;min-width:80px;height:36px;background:#fff;color:#333;
  border:1px solid #ccc;border-radius:6px;cursor:pointer;font-size:12px;font-weight:600;}}
.btn-one:hover{{background:#f5f5f5;}}
</style></head>
<body>
<div class="info">💡 Wing자동로그인.exe 최초 1회 실행 후 버튼 사용 가능</div>
<button class="btn-all" onclick="openAll()">🚀 5개 계정 전부 열기</button>
<div class="grid" id="grid"></div>

<script>
const NAMES = {names_js};

// 개별 버튼 생성
const grid = document.getElementById('grid');
NAMES.forEach(n => {{
  const b = document.createElement('button');
  b.className = 'btn-one';
  b.textContent = n;
  b.onclick = () => openOne(n);
  grid.appendChild(b);
}});

function openAll() {{
  window.location.href = 'wing://open-all';
}}

function openOne(name) {{
  window.location.href = 'wing://open/' + name;
}}
</script>
</body></html>"""


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
    # wing:// 프로토콜 버튼
    # ════════════════════════════════════════
    st.subheader("🚀 Wing 자동 열기")

    launcher_html = _launcher_buttons_html(names)
    components.html(launcher_html, height=180)

    st.divider()

    # ════════════════════════════════════════
    # wing:// 설치 안내
    # ════════════════════════════════════════
    with st.expander("🛠️ 최초 설치 방법 (한 번만)", expanded=False):
        st.markdown("""
**Wing자동로그인.exe 한 번만 실행하면 위 버튼이 동작합니다.**

1. `dist\\Wing자동로그인.exe` 파일을 엄마 PC에 복사
2. 더블클릭해서 한 번 실행 (wing:// 프로토콜 자동 등록)
3. 이후 위 🚀 버튼 클릭 → Chrome이 바로 열림

**EXE 파일 빌드 방법 (개발 PC에서):**
```
build_launcher_exe.bat 실행
→ dist\\Wing자동로그인.exe 생성
```
""")

    if _IS_LOCAL:
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
