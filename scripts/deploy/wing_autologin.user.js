// ==UserScript==
// @name         Wing 자동 로그인
// @namespace    coupang-biz
// @version      1.0
// @description  대시보드에서 계정 클릭 시 Wing 셀러센터 자동 로그인
// @match        https://wing.coupang.com/*
// @grant        none
// ==/UserScript==

(function() {
    'use strict';

    // URL 파라미터에서 계정 정보 추출
    const params = new URLSearchParams(window.location.search);
    const wingId = params.get('_wid');
    const wingPw = params.get('_wpw');

    if (!wingId || !wingPw) return;

    // URL에서 파라미터 제거 (보안)
    const cleanUrl = window.location.origin + window.location.pathname;
    window.history.replaceState({}, '', cleanUrl);

    function tryLogin() {
        const idEl = document.querySelector('input[placeholder="아이디를 입력해주세요"]');
        const pwEl = document.querySelector('input[placeholder="비밀번호를 입력해주세요"]');

        if (!idEl || !pwEl) {
            // 이미 로그인된 상태
            return;
        }

        // React input 값 설정
        const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value').set;

        setter.call(idEl, wingId);
        idEl.dispatchEvent(new Event('input', {bubbles: true}));
        idEl.dispatchEvent(new Event('change', {bubbles: true}));

        setter.call(pwEl, wingPw);
        pwEl.dispatchEvent(new Event('input', {bubbles: true}));
        pwEl.dispatchEvent(new Event('change', {bubbles: true}));

        // 로그인 버튼 클릭
        setTimeout(function() {
            const btns = document.querySelectorAll('button');
            for (let i = 0; i < btns.length; i++) {
                if (btns[i].textContent.indexOf('로그인') >= 0) {
                    btns[i].click();
                    break;
                }
            }
        }, 500);
    }

    // 페이지 로딩 후 로그인 시도 (최대 5초 대기)
    let attempts = 0;
    const interval = setInterval(function() {
        attempts++;
        const idEl = document.querySelector('input[placeholder="아이디를 입력해주세요"]');
        if (idEl) {
            clearInterval(interval);
            tryLogin();
        } else if (attempts > 10) {
            clearInterval(interval);
        }
    }, 500);
})();
