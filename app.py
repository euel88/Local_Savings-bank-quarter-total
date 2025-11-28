"""
저축은행 중앙회 통일경영공시 데이터 스크래퍼
Streamlit 웹 앱 버전 v3.1
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
    initial_sidebar_state="collapsed"
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
    /* 메인 헤더 */
    .main-header {
        font-size: 2.2rem;
        font-weight: bold;
        text-align: center;
        padding: 1.5rem;
        background: linear-gradient(135deg, #1E88E5 0%, #1565C0 100%);
        color: white;
        border-radius: 15px;
        margin-bottom: 1.5rem;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }

    .sub-header {
        text-align: center;
        color: #666;
        margin-bottom: 2rem;
        font-size: 1.1rem;
    }

    /* 설정 카드 */
    .settings-card {
        background: #f8f9fa;
        padding: 1.5rem;
        border-radius: 10px;
        border: 1px solid #e0e0e0;
        margin-bottom: 1rem;
    }

    /* 은행 선택 그리드 */
    .bank-container {
        display: flex;
        flex-wrap: wrap;
        justify-content: center;
        gap: 8px;
        padding: 1rem;
        background: #fafafa;
        border-radius: 10px;
        border: 1px solid #e0e0e0;
    }

    .bank-chip {
        display: inline-block;
        padding: 6px 12px;
        background: #e3f2fd;
        border-radius: 20px;
        font-size: 0.85rem;
        color: #1565c0;
        border: 1px solid #90caf9;
    }

    .bank-chip.selected {
        background: #1E88E5;
        color: white;
        border-color: #1565C0;
    }

    /* 진행 상태 */
    .progress-card {
        background: linear-gradient(135deg, #e8f5e9 0%, #c8e6c9 100%);
        padding: 1rem;
        border-radius: 10px;
        text-align: center;
        margin: 1rem 0;
    }

    .elapsed-time {
        font-size: 1.5rem;
        font-weight: bold;
        color: #2e7d32;
    }

    /* 메트릭 카드 */
    .metric-card {
        background: white;
        padding: 1rem;
        border-radius: 10px;
        text-align: center;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
        border: 1px solid #e0e0e0;
    }

    /* 버튼 스타일 */
    .stButton > button {
        border-radius: 25px;
        padding: 0.5rem 2rem;
        font-weight: 600;
    }

    /* 프로그레스 바 */
    .stProgress > div > div > div > div {
        background: linear-gradient(90deg, #1E88E5, #42A5F5);
        border-radius: 10px;
    }

    /* 결과 테이블 */
    .dataframe {
        font-size: 0.9rem;
    }

    /* 섹션 제목 */
    .section-title {
        font-size: 1.3rem;
        font-weight: 600;
        color: #1565C0;
        margin: 1.5rem 0 1rem 0;
        padding-bottom: 0.5rem;
        border-bottom: 2px solid #e3f2fd;
    }
</style>
""", unsafe_allow_html=True)


def format_elapsed_time(seconds):
    """경과 시간을 포맷팅"""
    minutes, secs = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    if hours > 0:
        return f"{hours}시간 {minutes}분 {secs}초"
    elif minutes > 0:
        return f"{minutes}분 {secs}초"
    else:
        return f"{secs}초"


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
    if 'elapsed_time' not in st.session_state:
        st.session_state.elapsed_time = 0
    if 'bank_dates' not in st.session_state:
        st.session_state.bank_dates = {}


def main():
    """메인 함수"""
    init_session_state()

    # 헤더
    st.markdown('<div class="main-header">🏦 저축은행 공시자료 크롤링 시스템</div>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">79개 저축은행의 결산공시 및 분기공시 데이터를 자동으로 수집합니다</p>', unsafe_allow_html=True)

    if not SCRAPER_AVAILABLE:
        st.error("스크래퍼 모듈을 로드할 수 없습니다. 필요한 패키지가 설치되어 있는지 확인하세요.")
        return

    config = Config()
    all_banks = config.BANKS

    # ========== 설정 섹션 ==========
    st.markdown('<div class="section-title">⚙️ 스크래핑 설정</div>', unsafe_allow_html=True)

    col1, col2, col3 = st.columns([1, 1, 1])

    with col1:
        scrape_type = st.selectbox(
            "📋 스크래핑 유형",
            options=["quarterly", "settlement"],
            format_func=lambda x: "📊 분기공시 (3개월)" if x == "quarterly" else "🏦 결산공시 (연말)",
            help="수집할 공시 유형을 선택하세요"
        )

    with col2:
        download_filename = st.text_input(
            "📁 다운로드 파일명",
            value=f"저축은행_{scrape_type}_{datetime.now().strftime('%Y%m%d')}",
            help="다운로드할 ZIP 파일의 이름을 지정하세요"
        )

    with col3:
        auto_zip = st.checkbox("🗜️ 완료 후 자동 압축", value=True)
        save_md = st.checkbox("📝 MD 파일도 함께 생성", value=False)

    st.divider()

    # ========== 은행 선택 섹션 ==========
    st.markdown('<div class="section-title">🏦 은행 선택</div>', unsafe_allow_html=True)

    # 전체 선택/해제 버튼 (중앙 정렬)
    col1, col2, col3, col4, col5 = st.columns([1, 1, 1, 1, 1])
    with col2:
        if st.button("✅ 전체 선택", use_container_width=True, type="primary"):
            st.session_state.selected_banks = all_banks.copy()
            st.rerun()
    with col3:
        st.metric("선택된 은행", f"{len(st.session_state.selected_banks)}개 / 79개")
    with col4:
        if st.button("❌ 전체 해제", use_container_width=True):
            st.session_state.selected_banks = []
            st.rerun()

    st.write("")

    # 은행 체크박스 그리드 (중앙 정렬, 8열)
    st.markdown("**은행을 개별 선택하거나 전체 선택 버튼을 사용하세요:**")

    # 8열로 은행 체크박스 표시
    cols_per_row = 8
    rows = [all_banks[i:i + cols_per_row] for i in range(0, len(all_banks), cols_per_row)]

    selected_banks_temp = st.session_state.selected_banks.copy()

    for row in rows:
        cols = st.columns(cols_per_row)
        for idx, bank in enumerate(row):
            with cols[idx]:
                checked = bank in selected_banks_temp
                if st.checkbox(bank, value=checked, key=f"bank_{bank}"):
                    if bank not in selected_banks_temp:
                        selected_banks_temp.append(bank)
                else:
                    if bank in selected_banks_temp:
                        selected_banks_temp.remove(bank)

    st.session_state.selected_banks = selected_banks_temp
    selected_banks = st.session_state.selected_banks

    st.divider()

    # ========== 실행 섹션 ==========
    st.markdown('<div class="section-title">🚀 스크래핑 실행</div>', unsafe_allow_html=True)

    # 정보 표시
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("📊 선택된 은행", f"{len(selected_banks)}개")
    with col2:
        type_name = "분기공시" if scrape_type == "quarterly" else "결산공시"
        st.metric("📋 스크래핑 유형", type_name)
    with col3:
        st.metric("📅 실행 날짜", datetime.now().strftime("%Y-%m-%d"))
    with col4:
        if st.session_state.elapsed_time > 0:
            st.metric("⏱️ 소요 시간", format_elapsed_time(st.session_state.elapsed_time))
        else:
            st.metric("⏱️ 소요 시간", "-")

    st.write("")

    # 스크래핑 시작 버튼
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        start_disabled = not selected_banks or st.session_state.scraping_running
        if st.button("🚀 스크래핑 시작", type="primary", use_container_width=True, disabled=start_disabled):
            if not selected_banks:
                st.error("스크래핑할 은행을 선택하세요.")
            else:
                run_scraping(selected_banks, scrape_type, auto_zip, download_filename)

    if st.session_state.scraping_running:
        st.info("⏳ 스크래핑이 진행 중입니다. 잠시만 기다려주세요...")

    st.divider()

    # ========== 결과 섹션 ==========
    st.markdown('<div class="section-title">📊 스크래핑 결과</div>', unsafe_allow_html=True)

    if st.session_state.results:
        results = st.session_state.results
        success_count = sum(1 for r in results if r['success'])
        fail_count = len(results) - success_count

        # 결과 요약
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("📁 전체", f"{len(results)}개")
        with col2:
            st.metric("✅ 성공", f"{success_count}개")
        with col3:
            st.metric("❌ 실패", f"{fail_count}개")
        with col4:
            st.metric("⏱️ 총 소요시간", format_elapsed_time(st.session_state.elapsed_time))

        st.write("")

        # 결과 테이블 (은행명, 공시날짜, 상태, 파일)
        df = create_summary_dataframe(results, st.session_state.bank_dates)
        st.dataframe(df, use_container_width=True, hide_index=True)

        # 다운로드 버튼
        st.write("")
        if 'zip_path' in st.session_state and st.session_state.zip_path:
            col1, col2, col3 = st.columns([1, 2, 1])
            with col2:
                with open(st.session_state.zip_path, 'rb') as f:
                    st.download_button(
                        label="📥 결과 파일 다운로드 (ZIP)",
                        data=f,
                        file_name=f"{download_filename}.zip",
                        mime="application/zip",
                        use_container_width=True,
                        type="primary"
                    )
    else:
        st.info("📋 스크래핑 결과가 없습니다. 은행을 선택하고 스크래핑을 실행하세요.")

    st.divider()

    # ========== 로그 섹션 ==========
    with st.expander("📝 실행 로그 보기", expanded=False):
        if st.session_state.logs:
            log_text = "\n".join(st.session_state.logs)
            st.text_area("로그", value=log_text, height=300, disabled=True)

            if st.button("🗑️ 로그 지우기"):
                st.session_state.logs = []
                st.rerun()
        else:
            st.info("로그가 없습니다.")

    # ========== 앱 정보 ==========
    with st.expander("ℹ️ 앱 정보", expanded=False):
        st.markdown("""
        ### 저축은행 공시자료 크롤링 시스템 v3.1

        **주요 기능:**
        - 79개 저축은행 분기공시/결산공시 데이터 자동 수집
        - 은행별 공시 날짜 표시
        - Excel 파일 형식으로 데이터 저장
        - ZIP 압축 다운로드 지원
        - 실시간 진행 상태 및 경과 시간 표시

        **사용 방법:**
        1. 스크래핑 유형 선택 (분기공시/결산공시)
        2. 스크래핑할 은행 선택 (전체 또는 개별)
        3. '스크래핑 시작' 버튼 클릭
        4. 완료 후 결과 파일 다운로드

        **데이터 출처:**
        - 저축은행중앙회 통일경영공시 (https://www.fsb.or.kr)
        """)


def run_scraping(selected_banks, scrape_type, auto_zip, download_filename):
    """스크래핑 실행"""
    st.session_state.scraping_running = True
    st.session_state.results = []
    st.session_state.logs = []
    st.session_state.bank_dates = {}

    start_time = time.time()

    # 진행 상태 표시
    progress_container = st.container()
    with progress_container:
        progress_bar = st.progress(0)
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            status_text = st.empty()
            elapsed_text = st.empty()
        log_container = st.empty()

    try:
        config = Config(scrape_type)
        logger = StreamlitLogger()
        scraper = BankScraper(config, logger)

        total_banks = len(selected_banks)
        results = []
        bank_dates = {}

        for idx, bank in enumerate(selected_banks):
            # 경과 시간 업데이트
            elapsed = time.time() - start_time
            st.session_state.elapsed_time = elapsed

            progress = (idx + 1) / total_banks
            progress_bar.progress(progress)
            status_text.markdown(f"**처리 중:** {bank} ({idx + 1}/{total_banks})")
            elapsed_text.markdown(f"⏱️ 경과 시간: **{format_elapsed_time(elapsed)}**")

            logger.log_message(f"[시작] {bank} 스크래핑")

            filepath, success, date_info = scraper.scrape_bank(bank)
            results.append({
                'bank': bank,
                'success': success,
                'filepath': filepath,
                'date_info': date_info
            })

            # 날짜 정보 저장
            bank_dates[bank] = date_info

            status = "완료" if success else "실패"
            logger.log_message(f"[{status}] {bank} - 공시일: {date_info}")

            # 로그 업데이트
            st.session_state.logs = logger.messages.copy()
            log_container.text_area("실시간 로그", value=logger.get_logs(), height=150, disabled=True, key=f"log_{idx}")

            # 은행 간 딜레이
            time.sleep(0.5)

        # 최종 경과 시간
        final_elapsed = time.time() - start_time
        st.session_state.elapsed_time = final_elapsed

        # 결과 저장
        st.session_state.results = results
        st.session_state.bank_dates = bank_dates

        # ZIP 압축
        if auto_zip:
            status_text.markdown("**📦 파일 압축 중...**")
            zip_path = scraper.create_zip_archive(results, download_filename)
            if zip_path:
                st.session_state.zip_path = zip_path
                logger.log_message(f"ZIP 파일 생성 완료")

        # 완료
        progress_bar.progress(1.0)
        success_count = sum(1 for r in results if r['success'])
        status_text.markdown(f"**✅ 완료!** 성공: {success_count}/{total_banks}")
        elapsed_text.markdown(f"⏱️ 총 소요 시간: **{format_elapsed_time(final_elapsed)}**")

        st.success(f"🎉 스크래핑 완료! 성공: {success_count}개, 실패: {total_banks - success_count}개, 소요시간: {format_elapsed_time(final_elapsed)}")
        st.session_state.logs = logger.messages.copy()

    except Exception as e:
        st.error(f"❌ 스크래핑 중 오류 발생: {str(e)}")
        st.session_state.logs.append(f"[오류] {str(e)}")

    finally:
        st.session_state.scraping_running = False


if __name__ == "__main__":
    main()
