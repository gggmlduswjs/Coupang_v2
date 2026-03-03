"""
Wing 셀러센터 바로가기
======================
계정별 Wing 자동 로그인 + 빠른 접근
"""
import streamlit as st
import streamlit.components.v1 as components


def _load_creds() -> dict:
    """자격증명 로드: session_state 우선 → st.secrets 폴백 (대소문자 무시)"""
    if "wing_creds_override" in st.session_state:
        return st.session_state["wing_creds_override"]
    try:
        raw = dict(st.secrets.get("wing_creds", {}))
        result = {}
        for name, val in raw.items():
            if hasattr(val, "items"):          # TOML 섹션 (dict-like)
                result[name.lower()] = {"id": val.get("id", ""), "pw": val.get("pw", "")}
            else:
                result[name.lower()] = {"id": str(val), "pw": ""}
        return result
    except Exception:
        return {}


def _match_cred(creds: dict, account_name: str) -> dict:
    """대소문자 무시 매칭"""
    return (
        creds.get(account_name)
        or creds.get(account_name.lower())
        or creds.get(account_name.upper())
        or {}
    )


def _card_html(account_name: str, wing_id: str, wing_pw: str) -> str:
    """
    ① Wing 열기 버튼  — 새 탭 열기 + PW 클립보드 복사
    ② 북마클릿 링크   — 북마크 바에 드래그해두면 Wing 로그인 페이지에서
                        클릭 한 번으로 ID/PW 자동 입력 + 로그인 버튼 클릭
    """
    eid  = wing_id.replace("'", "\\'").replace("`", "\\`")
    epw  = wing_pw.replace("'", "\\'").replace("`", "\\`")
    name = account_name.replace("<", "").replace(">", "")

    # 북마클릿 JS: React 이벤트 호환 value setter 사용
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
.open{{
  display:block;width:100%;height:42px;
  background:#E4002B;color:#fff;
  border:none;border-radius:8px;
  font-size:14px;font-weight:700;cursor:pointer;
  transition:opacity .15s;
}}
.open:hover{{opacity:.85;}}
.row{{display:flex;align-items:center;gap:6px;margin-top:6px;}}
.bm{{
  flex:1;padding:7px 0;text-align:center;
  background:#fff3cd;color:#856404;
  border:1px dashed #ffc107;border-radius:6px;
  font-size:12px;font-weight:600;text-decoration:none;
  cursor:grab;white-space:nowrap;
}}
.bm:hover{{background:#ffe69c;}}
.tip{{font-size:11px;color:#888;}}
.msg{{display:none;font-size:11px;color:#198754;margin-top:3px;text-align:center;}}
</style>
</head><body>
<button class="open" onclick="
  navigator.clipboard.writeText('{epw}').catch(()=>{{}});
  window.open('https://wing.coupang.com','_blank');
  document.getElementById('m').style.display='block';
  this.textContent='✅ 열림 — PW 복사됨';
  setTimeout(()=>this.textContent='🚀 Wing 열기 — {name}',3000);
">🚀 Wing 열기 — {name}</button>
<div id="m" class="msg">PW 클립보드 복사됨 → Wing 탭에서 붙여넣기</div>
<div class="row">
  <a class="bm" href="{bm_js}" title="북마크 바에 드래그하세요">🔖 {name} 자동로그인</a>
  <span class="tip">← 북마크 바에<br>드래그</span>
</div>
</body></html>"""


def render(selected_account, accounts_df, account_names):
    st.title("🔐 Wing 셀러센터 바로가기")
    st.caption("버튼 클릭 → 새 탭에서 해당 계정으로 Wing 자동 로그인")

    if accounts_df.empty:
        st.warning("활성 계정이 없습니다.")
        return

    creds = _load_creds()

    # ── 설정 패널 ──────────────────────────────────────────
    names = accounts_df["account_name"].tolist()
    all_set = all(
        creds.get(n, {}).get("id") and creds.get(n, {}).get("pw")
        for n in names
    )
    status_badge = " ✅" if all_set else " ⚠️ ID/PW 미설정"

    with st.expander(f"⚙️ Wing 로그인 정보 설정{status_badge}", expanded=not all_set):
        st.caption(
            "입력한 정보는 **이 세션에만** 저장됩니다.\n"
            "매번 입력이 번거로우면 Streamlit Secrets(`wing_creds` 섹션)에 추가하세요."
        )
        st.code(
            '[wing_creds]\n'
            '"007-ez" = {id = "your_id", pw = "your_pw"}\n'
            '"007-bm" = {id = "your_id2", pw = "your_pw2"}',
            language="toml",
        )
        st.divider()

        edited = {}
        for name in names:
            existing = _match_cred(creds, name)
            c1, c2 = st.columns(2)
            with c1:
                wid = st.text_input(
                    f"{name} — Wing ID",
                    value=existing.get("id", ""),
                    key=f"_wcred_id_{name}",
                )
            with c2:
                wpw = st.text_input(
                    f"{name} — Wing PW",
                    value=existing.get("pw", ""),
                    type="password",
                    key=f"_wcred_pw_{name}",
                )
            edited[name] = {"id": wid, "pw": wpw}

        if st.button("💾 저장", type="primary", key="_wcred_save"):
            st.session_state["wing_creds_override"] = edited
            st.success("저장됐습니다. 아래 버튼으로 Wing에 접속하세요.")
            st.rerun()

    st.divider()

    # ── 계정 카드 ────────────────────────────────────────
    creds = _load_creds()   # 저장 후 최신값 재로드

    # ── 🔍 진단 패널 ──
    with st.expander("🔍 Secrets 로드 진단 (문제 발생 시 확인)", expanded=False):
        try:
            raw_keys = list(st.secrets.get("wing_creds", {}).keys())
            st.success(f"wing_creds 섹션 감지 — 키 {len(raw_keys)}개: `{raw_keys}`")
        except Exception as e:
            st.error(f"wing_creds 로드 실패: {e}")

        st.caption("DB 계정명 vs Secrets 키 매칭 결과")
        for _, acct in accounts_df.iterrows():
            nm = acct["account_name"]
            c = _match_cred(creds, nm)
            if c.get("id") and c.get("pw"):
                st.success(f"✅ `{nm}` → ID: `{c['id']}` / PW: {'*' * len(c['pw'])}")
            else:
                st.error(f"❌ `{nm}` → Secrets에서 매칭 안 됨 (키: `{nm.lower()}`를 찾는 중)")

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
                components.html(
                    _card_html(name, wing_id, wing_pw),
                    height=110,
                )
                st.caption(f"ID: `{wing_id}`")
            else:
                st.markdown(f"[Wing 열기 →](https://wing.coupang.com)")
                st.warning("⚠️ ID/PW 미설정")
            st.divider()

    # ── 안내 ──────────────────────────────────────────────
    st.divider()
    with st.expander("💡 북마클릿 사용법 (한 번만 설정하면 진짜 자동 로그인)"):
        st.markdown("""
위 **노란 🔖 버튼**을 북마크 바에 드래그해두세요.

**사용 순서:**
1. 🚀 버튼 클릭 → Wing 새 탭 열림
2. Wing 로그인 페이지가 뜨면 → 북마크 바의 **🔖 계정명 자동로그인** 클릭
3. ID/PW 자동 입력 + 로그인 버튼 자동 클릭 ✅

**또는** Wing에서 **"로그인 유지"** 체크하면 한 번 로그인 후 몇 주간 유지됩니다.
""")
