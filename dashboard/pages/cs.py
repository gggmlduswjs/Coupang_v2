"""
CS 관리 페이지
==============
고객 문의 조회 및 답변 처리.
"""

import streamlit as st


def render(selected_account, accounts_df, account_names):
    st.title("CS 관리")
    st.info("고객 문의 / 콜센터 문의 관리 기능 준비 중입니다.")
