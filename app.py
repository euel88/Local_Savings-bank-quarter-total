"""
저축은행 중앙회 통일경영공시 데이터 스크래퍼
Streamlit 웹 앱 버전
"""

import streamlit as st
import pandas as pd
import os
import time
from datetime import datetime

# 페이지 설정
st.set_page_config(
    page_title="저축은행 데이터 스크래퍼",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 스크래퍼 모듈 임포트
try:
    from scraper_core import (
        Config, BankScraper, StreamlitLogger,
        create_summary_dataframe
    )
    SCRAPER_AVAILABLE = True
except ImportError as e:
    SCRAPER_AVAILABLE = False
    st.error(f"스크래퍼 모듈 로드 실패: {e}")

# CSS 스타일
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        text-align: center;
        padding: 1rem;
        background: linear-gradient(90deg, #1E88E5, #42A5F5);
        color: white;
        border-radius: 10px;
        margin-bottom: 2rem;
    }
    .info-box {
        padding: 1rem;
        background-color: #E3F2FD;
        border-left: 5px solid #1E88E5;
        border-radius: 5px;
        margin: 1rem 0;
    }
    .success-box {
        padding: 1rem;
        background-color: #E8F5E9;
        border-left: 5px solid #4CAF50;
        border-radius: 5px;
    }
    .warning-box {
        padding: 1rem;
        background-color: #FFF3E0;
        border-left: 5px solid #FF9800;
        border-radius: 5px;
    }
    .bank-grid {
        display: grid;
        grid-template-columns: repeat(auto-fill, minmax(120px, 1fr));
        gap: 0.5rem;
    }
    .stProgress > div > div > div > div {
        background-color: #1E88E5;
    }
</style>
""", unsafe_allow_html=True)


def init_session_state():
    """세션 상태 초기화"""
    if 'scraping_running' not in st.session_state:
        st.session_state.scraping_running = False
    if 'results' not in st.session_state:
        st.session_state.results = []
    if 'logs' not in st.session_state:
        st.session_state.logs = []
    if 'selected_banks' not in st.session_state:
        st.session_state.selected_banks = []


def main():
    """메인 함수"""
    init_session_state()

    # 헤더
    st.markdown('<div class="main-header">🏦 저축은행 공시자료 크롤링 시스템</div>', unsafe_allow_html=True)
    st.markdown('<p style="text-align: center; color: #666;">79개 저축은행의 결산공시 및 분기공시 데이터 수집</p>', unsafe_allow_html=True)

    if not SCRAPER_AVAILABLE:
        st.error("스크래퍼 모듈을 로드할 수 없습니다. 필요한 패키지가 설치되어 있는지 확인하세요.")
        return

    # 사이드바 - 설정
    with st.sidebar:
        st.header("⚙️ 설정")

        # 스크래핑 유형 선택
        scrape_type = st.selectbox(
            "스크래핑 유형",
            options=["quarterly", "settlement"],
            format_func=lambda x: "📊 분기공시 (3개월)" if x == "quarterly" else "🏦 결산공시 (연말)"
        )

        st.divider()

        # 은행 선택
        st.subheader("🏦 은행 선택")

        config = Config(scrape_type)
        all_banks = config.BANKS

        # 전체 선택/해제
        col1, col2 = st.columns(2)
        with col1:
            if st.button("전체 선택", use_container_width=True):
                st.session_state.selected_banks = all_banks.copy()
        with col2:
            if st.button("전체 해제", use_container_width=True):
                st.session_state.selected_banks = []

        # 은행 목록 (멀티셀렉트)
        selected_banks = st.multiselect(
            "스크래핑할 은행 선택",
            options=all_banks,
            default=st.session_state.selected_banks if st.session_state.selected_banks else all_banks[:5],
            help="스크래핑할 은행을 선택하세요"
        )
        st.session_state.selected_banks = selected_banks

        st.info(f"선택된 은행: {len(selected_banks)}개")

        st.divider()

        # 옵션
        st.subheader("📋 옵션")
        save_md = st.checkbox("MD 파일도 함께 생성", value=False)
        auto_zip = st.checkbox("완료 후 자동 압축", value=True)

    # 메인 콘텐츠
    tab1, tab2, tab3 = st.tabs(["🚀 스크래핑", "📊 결과", "📝 로그"])

    with tab1:
        st.header("스크래핑 실행")

        # 스크래핑 정보
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("선택된 은행", f"{len(selected_banks)}개")
        with col2:
            type_name = "분기공시" if scrape_type == "quarterly" else "결산공시"
            st.metric("스크래핑 유형", type_name)
        with col3:
            st.metric("날짜", datetime.now().strftime("%Y-%m-%d"))

        st.divider()

        # 선택된 은행 표시
        if selected_banks:
            st.subheader("선택된 은행 목록")
            # 5열로 은행 표시
            cols = st.columns(5)
            for idx, bank in enumerate(selected_banks):
                with cols[idx % 5]:
                    st.write(f"• {bank}")
        else:
            st.warning("스크래핑할 은행을 선택하세요.")

        st.divider()

        # 스크래핑 시작 버튼
        if st.button("🚀 스크래핑 시작", type="primary", use_container_width=True, disabled=not selected_banks or st.session_state.scraping_running):
            if not selected_banks:
                st.error("스크래핑할 은행을 선택하세요.")
            else:
                run_scraping(selected_banks, scrape_type, auto_zip)

        if st.session_state.scraping_running:
            st.info("스크래핑이 진행 중입니다...")

    with tab2:
        st.header("스크래핑 결과")

        if st.session_state.results:
            # 결과 요약
            results = st.session_state.results
            success_count = sum(1 for r in results if r['success'])
            fail_count = len(results) - success_count

            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("전체", f"{len(results)}개")
            with col2:
                st.metric("성공", f"{success_count}개", delta=None)
            with col3:
                st.metric("실패", f"{fail_count}개", delta=None)

            st.divider()

            # 결과 테이블
            df = create_summary_dataframe(results)
            st.dataframe(df, use_container_width=True)

            # 다운로드 버튼
            st.divider()
            if 'zip_path' in st.session_state and st.session_state.zip_path:
                with open(st.session_state.zip_path, 'rb') as f:
                    st.download_button(
                        label="📥 결과 파일 다운로드 (ZIP)",
                        data=f,
                        file_name=os.path.basename(st.session_state.zip_path),
                        mime="application/zip",
                        use_container_width=True
                    )
        else:
            st.info("스크래핑 결과가 없습니다. 스크래핑을 실행하세요.")

    with tab3:
        st.header("실행 로그")

        if st.session_state.logs:
            log_text = "\n".join(st.session_state.logs)
            st.text_area("로그", value=log_text, height=400, disabled=True)

            if st.button("로그 지우기"):
                st.session_state.logs = []
                st.rerun()
        else:
            st.info("로그가 없습니다.")


def run_scraping(selected_banks, scrape_type, auto_zip):
    """스크래핑 실행"""
    st.session_state.scraping_running = True
    st.session_state.results = []
    st.session_state.logs = []

    # 진행 상태 표시
    progress_bar = st.progress(0)
    status_text = st.empty()
    log_container = st.empty()

    try:
        config = Config(scrape_type)
        logger = StreamlitLogger()
        scraper = BankScraper(config, logger)

        total_banks = len(selected_banks)
        results = []

        for idx, bank in enumerate(selected_banks):
            progress = (idx + 1) / total_banks
            progress_bar.progress(progress)
            status_text.text(f"처리 중: {bank} ({idx + 1}/{total_banks})")

            logger.log_message(f"[시작] {bank} 스크래핑")

            filepath, success = scraper.scrape_bank(bank)
            results.append({
                'bank': bank,
                'success': success,
                'filepath': filepath
            })

            status = "완료" if success else "실패"
            logger.log_message(f"[{status}] {bank}")

            # 로그 업데이트
            st.session_state.logs = logger.messages.copy()
            log_container.text_area("실시간 로그", value=logger.get_logs(), height=200, disabled=True)

            # 은행 간 딜레이
            time.sleep(1)

        # 결과 저장
        st.session_state.results = results

        # ZIP 압축
        if auto_zip:
            status_text.text("파일 압축 중...")
            zip_path = scraper.create_zip_archive(results)
            if zip_path:
                st.session_state.zip_path = zip_path
                logger.log_message(f"ZIP 파일 생성 완료: {os.path.basename(zip_path)}")

        # 완료
        progress_bar.progress(1.0)
        success_count = sum(1 for r in results if r['success'])
        status_text.text(f"완료! 성공: {success_count}/{total_banks}")

        st.success(f"스크래핑 완료! 성공: {success_count}개, 실패: {total_banks - success_count}개")

    except Exception as e:
        st.error(f"스크래핑 중 오류 발생: {str(e)}")
        st.session_state.logs.append(f"[오류] {str(e)}")

    finally:
        st.session_state.scraping_running = False
        st.session_state.logs = logger.messages.copy() if 'logger' in dir() else st.session_state.logs


# 앱 정보
def show_app_info():
    """앱 정보 표시"""
    with st.expander("ℹ️ 앱 정보"):
        st.markdown("""
        ### 저축은행 공시자료 크롤링 시스템 v3.0

        **기능:**
        - 79개 저축은행 분기공시/결산공시 데이터 수집
        - Excel 파일 형식으로 데이터 저장
        - ZIP 압축 다운로드 지원

        **사용 방법:**
        1. 사이드바에서 스크래핑 유형 선택 (분기공시/결산공시)
        2. 스크래핑할 은행 선택
        3. '스크래핑 시작' 버튼 클릭
        4. 완료 후 결과 탭에서 다운로드

        **데이터 출처:**
        - 저축은행중앙회 통일경영공시 (https://www.fsb.or.kr)
        """)


if __name__ == "__main__":
    main()
    show_app_info()
