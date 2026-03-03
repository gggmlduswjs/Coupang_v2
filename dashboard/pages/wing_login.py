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


def _login_iframe(account_name: str, wing_id: str, wing_pw: str) -> str:
    """
    Wing 로그인 폼을 자동 제출하는 iframe HTML.
    사용자가 버튼을 클릭하면 새 탭에서 wing.coupang.com/login 으로 POST.
    (브라우저 팝업차단 우회: 직접 클릭 이벤트에서 열어야 동작)
    """
    safe_id = str(wing_id).replace('"', "&quot;").replace("'", "&#39;")
    safe_pw = str(wing_pw).replace('"', "&quot;").replace("'", "&#39;")
    safe_name = str(account_name).replace("<", "").replace(">", "")
    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8">
<style>
  * {{box-sizing:border-box;margin:0;padding:0;}}
  body {{padding:2px;}}
  button {{
    width:100%;height:44px;
    background:#E4002B;color:#fff;
    border:none;border-radius:8px;
    font-size:15px;font-weight:700;
    cursor:pointer;letter-spacing:0.3px;
    transition:opacity .15s;
  }}
  button:hover {{opacity:.85;}}
  button:active {{opacity:.7;}}
</style>
</head>
<body>
<form method="POST" action="https://wing.coupang.com/login" target="_blank">
  <input type="hidden" name="username" value="{safe_id}">
  <input type="hidden" name="password" value="{safe_pw}">
  <button type="submit">🚀 Wing 열기 — {safe_name}</button>
</form>
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
            with st.container(border=True):
                st.markdown(f"#### {name}")
                st.caption(f"Vendor: `{vendor_id}`")

                if wing_id and wing_pw:
                    # ── 자동 로그인 버튼 (iframe 내 직접 클릭) ──
                    components.html(
                        _login_iframe(name, wing_id, wing_pw),
                        height=52,
                    )
                    st.divider()

                    # ID 표시 + PW 복사용
                    st.caption("ID")
                    st.code(wing_id, language=None)
                    st.caption("PW (복사용 — 클릭하면 클립보드에 복사)")
                    # PW는 보안상 직접 표시 안 함 — 복사 버튼만
                    copy_js = f"""
                    <button onclick="navigator.clipboard.writeText('{wing_pw.replace(chr(39), '')}');
                      this.textContent='✅ 복사됨!';setTimeout(()=>this.textContent='📋 PW 복사',1500);"
                      style="padding:6px 14px;border:1px solid #ddd;border-radius:6px;
                             background:#f8f9fa;cursor:pointer;font-size:13px;width:100%;">
                      📋 PW 복사
                    </button>"""
                    components.html(copy_js, height=36)

                else:
                    st.link_button(
                        "Wing 열기 →",
                        "https://wing.coupang.com",
                        use_container_width=True,
                        key=f"_card_wing_{name}",
                    )
                    st.warning("⚠️ ID/PW가 설정되지 않았습니다.\n위 설정 패널에서 입력하세요.")

    # ── 안내 ──────────────────────────────────────────────
    st.divider()
    with st.expander("❓ 자동 로그인이 안 될 때"):
        st.markdown("""
**자동 로그인이 안 되는 주요 원인:**

1. **Wing 폼 필드명 불일치** — Wing 업데이트로 form field 이름이 바뀔 수 있음
   - 해결: Wing 로그인 페이지에서 `F12 → Network → 로그인 → Request payload` 확인 후 알려주세요

2. **팝업 차단** — 브라우저가 새 탭을 차단할 수 있음
   - 해결: 주소창 우측 팝업 허용 클릭

3. **CSRF 토큰** — 일부 사이트는 세션 토큰 필요
   - 해결: 위 ID/PW를 직접 복사해서 Wing에 붙여넣기

**Wing 바로가기 링크:**
""")
        st.markdown(" · ".join(f"[{n}](https://wing.coupang.com)" for n in names), unsafe_allow_html=False)
        st.link_button("Wing 셀러센터 열기", "https://wing.coupang.com", key="_direct_wing_main")
