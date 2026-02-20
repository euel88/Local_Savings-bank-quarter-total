"""
저축은행 중앙회 통일경영공시 데이터 스크래퍼
Streamlit 웹 앱 버전 v4.1
- GPT-5.2 API 업그레이드
- API 키 보안 저장 (.streamlit/secrets.toml / 환경변수)
- 스크래핑 완료 후 AI 표 정리 및 엑셀 반환 옵션 추가
- 통일경영공시/감사보고서 파일 다운로드 기능 추가
"""

import streamlit as st
import pandas as pd
import os
import time
import tempfile
import zipfile
from datetime import datetime

# 엑셀 생성 모듈 임포트
try:
    from excel_generator import (
        ChatGPTExcelGenerator,
        DirectExcelGenerator,
        generate_excel_with_chatgpt,
        OPENAI_AVAILABLE
    )
    EXCEL_GENERATOR_AVAILABLE = True
except ImportError:
    EXCEL_GENERATOR_AVAILABLE = False
    OPENAI_AVAILABLE = False

# 공시파일 다운로드 모듈 임포트
try:
    from downloader_core import DisclosureDownloader, TARGET_URL
    DOWNLOADER_AVAILABLE = True
except ImportError:
    DOWNLOADER_AVAILABLE = False


def load_api_key():
    """API 키를 secrets.toml 또는 환경변수에서 로드"""
    # 1순위: Streamlit secrets (.streamlit/secrets.toml)
    try:
        key = st.secrets.get("OPENAI_API_KEY", "")
        if key:
            return key
    except Exception:
        pass

    # 2순위: 환경변수
    key = os.environ.get("OPENAI_API_KEY", "")
    if key:
        return key

    return ""

# 페이지 설정
st.set_page_config(
    page_title="Savings Bank Data Dashboard",
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

# CSS 스타일 — Warm Amber/Gold Dashboard Theme
st.markdown("""
<link href="https://fonts.googleapis.com/css2?family=Manrope:wght@200..800&display=swap" rel="stylesheet"/>
<link href="https://fonts.googleapis.com/css2?family=Material+Symbols+Outlined:wght,FILL@100..700,0..1&display=swap" rel="stylesheet"/>
<style>
    /* ===== Global ===== */
    @import url('https://fonts.googleapis.com/css2?family=Manrope:wght@200..800&display=swap');

    html, body, [class*="css"] {
        font-family: 'Manrope', sans-serif;
    }

    ::-webkit-scrollbar { width: 6px; height: 6px; }
    ::-webkit-scrollbar-track { background: transparent; }
    ::-webkit-scrollbar-thumb { background: #e7dfcf; border-radius: 3px; }
    ::-webkit-scrollbar-thumb:hover { background: #d6cbb5; }

    /* ===== Sidebar ===== */
    [data-testid="stSidebar"] {
        background: #fcfaf8;
        border-right: 1px solid #e7dfcf;
    }
    [data-testid="stSidebar"] .block-container { padding-top: 1rem; }

    .sidebar-brand {
        display: flex; align-items: center; gap: 12px;
        padding: 0.5rem 0 1.5rem 0;
    }
    .sidebar-brand-icon {
        background: linear-gradient(135deg, #eca413, #b87d0e);
        width: 40px; height: 40px; border-radius: 12px;
        display: flex; align-items: center; justify-content: center;
        color: white; box-shadow: 0 4px 20px -2px rgba(236,164,19,0.15);
        flex-shrink: 0;
    }
    .sidebar-brand-text h1 {
        font-size: 1rem; font-weight: 700; color: #1b170d;
        line-height: 1.2; margin: 0;
    }
    .sidebar-brand-text p {
        font-size: 0.75rem; font-weight: 500; color: #9a804c; margin: 0;
    }

    .sidebar-nav a {
        display: flex; align-items: center; gap: 12px;
        padding: 12px 16px; border-radius: 12px;
        text-decoration: none; font-size: 0.875rem; font-weight: 500;
        color: #1b170d; transition: background 0.2s;
    }
    .sidebar-nav a:hover { background: #f3efe7; }
    .sidebar-nav a.active {
        background: rgba(236,164,19,0.1); color: #b87d0e; font-weight: 700;
    }

    .sidebar-cta {
        display: flex; align-items: center; justify-content: center; gap: 8px;
        width: 100%; height: 48px; border-radius: 12px;
        background: #eca413; color: white; font-weight: 700; font-size: 0.875rem;
        border: none; cursor: pointer;
        box-shadow: 0 8px 24px -4px rgba(236,164,19,0.25);
        transition: background 0.2s;
        text-decoration: none;
    }
    .sidebar-cta:hover { background: #b87d0e; }

    .sidebar-profile {
        display: flex; align-items: center; gap: 12px;
        padding: 10px; border-radius: 12px; transition: background 0.2s;
    }
    .sidebar-profile:hover { background: #f3efe7; }
    .sidebar-profile-avatar {
        width: 40px; height: 40px; border-radius: 50%;
        background: #e0d8c8; border: 2px solid white;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        overflow: hidden; flex-shrink: 0;
    }
    .sidebar-profile-avatar img { width: 100%; height: 100%; object-fit: cover; }
    .sidebar-profile-name { font-size: 0.875rem; font-weight: 700; color: #1b170d; margin: 0; }
    .sidebar-profile-role { font-size: 0.75rem; color: #9a804c; margin: 0; }

    /* ===== Main Content ===== */
    .main .block-container { padding-top: 1rem; max-width: 1200px; }

    /* Header */
    .dashboard-header h2 {
        font-size: 1.75rem; font-weight: 900; color: #1b170d;
        letter-spacing: -0.025em; margin: 0;
    }
    .dashboard-header p {
        font-size: 0.875rem; font-weight: 500; color: #9a804c; margin: 0.25rem 0 0 0;
    }

    /* ===== Stat Cards ===== */
    .stat-card {
        background: #ffffff;
        padding: 1.5rem;
        border-radius: 1rem;
        border: 1px solid #e7dfcf;
        position: relative;
        overflow: hidden;
        transition: box-shadow 0.3s;
        box-shadow: 0 1px 3px 0 rgba(0,0,0,0.05);
    }
    .stat-card:hover {
        box-shadow: 0 4px 20px -2px rgba(236,164,19,0.08), 0 2px 6px -2px rgba(0,0,0,0.03);
    }
    .stat-card::before {
        content: '';
        position: absolute; right: -16px; top: -16px;
        width: 96px; height: 96px;
        background: rgba(236,164,19,0.05); border-radius: 50%;
        filter: blur(32px);
    }
    .stat-card:hover::before { background: rgba(236,164,19,0.1); }

    .stat-card-icon {
        padding: 8px; background: #f3efe7; border-radius: 8px;
        color: #eca413; display: inline-flex;
    }
    .stat-card-badge {
        padding: 2px 10px; border-radius: 9999px;
        font-size: 0.75rem; font-weight: 700;
    }
    .badge-green { background: rgba(7,136,16,0.1); color: #078810; }
    .badge-amber { background: rgba(236,164,19,0.1); color: #b87d0e; }

    .stat-card-label {
        font-size: 0.875rem; font-weight: 500; color: #9a804c; margin: 0;
    }
    .stat-card-value {
        font-size: 1.875rem; font-weight: 900; color: #1b170d; margin: 0;
    }
    .stat-card-value span {
        font-size: 1.125rem; color: #9a804c; font-weight: 400;
    }

    /* ===== Section Title ===== */
    .section-title {
        font-size: 1.25rem;
        font-weight: 700;
        color: #1b170d;
        margin: 1.5rem 0 1rem 0;
        padding-bottom: 0;
        border-bottom: none;
        display: flex; align-items: center; gap: 8px;
    }
    .section-title .live-badge {
        background: #f3efe7; color: #9a804c;
        font-size: 0.75rem; font-weight: 500;
        padding: 2px 10px; border-radius: 9999px;
    }

    /* ===== Table ===== */
    .custom-table {
        width: 100%; border-collapse: collapse;
        background: #ffffff; border-radius: 1rem;
        overflow: hidden; border: 1px solid #e7dfcf;
        box-shadow: 0 1px 3px 0 rgba(0,0,0,0.05);
    }
    .custom-table thead { background: #fcfaf8; border-bottom: 1px solid #e7dfcf; }
    .custom-table th {
        padding: 1.25rem; font-size: 0.75rem; font-weight: 700;
        text-transform: uppercase; letter-spacing: 0.05em;
        color: #9a804c; text-align: left;
    }
    .custom-table td {
        padding: 1.25rem; font-size: 0.875rem; color: #1b170d;
        border-bottom: 1px solid #e7dfcf;
    }
    .custom-table tr:last-child td { border-bottom: none; }
    .custom-table tr:hover { background: #fcfaf8; }

    .table-bank-avatar {
        width: 32px; height: 32px; border-radius: 8px;
        background: #f3f4f6; display: inline-flex;
        align-items: center; justify-content: center;
        font-size: 0.7rem; font-weight: 700; color: #9a804c;
        flex-shrink: 0;
    }
    .table-bank-name {
        font-weight: 700; font-size: 0.875rem; color: #1b170d;
    }

    .status-badge {
        display: inline-flex; align-items: center; gap: 6px;
        padding: 4px 12px; border-radius: 9999px;
        font-size: 0.75rem; font-weight: 700;
    }
    .status-success { background: #dcfce7; color: #15803d; }
    .status-running { background: #fef3c7; color: #b45309; }
    .status-failed { background: #fee2e2; color: #b91c1c; }
    .status-dot {
        width: 6px; height: 6px; border-radius: 50%; background: #22c55e;
    }
    .status-dot.pulse { animation: pulse 2s infinite; }
    @keyframes pulse {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.4; }
    }
    @keyframes spin {
        from { transform: rotate(0deg); }
        to { transform: rotate(360deg); }
    }

    .table-pagination {
        padding: 1rem 1.25rem;
        border-top: 1px solid #e7dfcf;
        background: #fcfaf8;
        display: flex; align-items: center; justify-content: space-between;
        font-size: 0.75rem; color: #9a804c;
    }
    .page-btn {
        width: 32px; height: 32px; display: inline-flex;
        align-items: center; justify-content: center;
        border-radius: 8px; border: 1px solid #e7dfcf;
        background: white; color: #9a804c;
        font-size: 0.75rem; font-weight: 500; cursor: pointer;
        transition: all 0.2s;
    }
    .page-btn:hover { background: #eca413; color: white; border-color: #eca413; }
    .page-btn.active {
        background: #eca413; color: white; border-color: #eca413;
        font-weight: 700; box-shadow: 0 1px 3px rgba(0,0,0,0.1);
    }

    /* ===== Log Card ===== */
    .log-card {
        background: #ffffff; padding: 1.5rem;
        border-radius: 1rem; border: 1px solid #e7dfcf;
        box-shadow: 0 1px 3px 0 rgba(0,0,0,0.05);
    }
    .log-card-header {
        display: flex; align-items: center; justify-content: space-between;
        margin-bottom: 1rem;
    }
    .log-card-header h3 { font-size: 1rem; font-weight: 700; color: #1b170d; margin: 0; }
    .log-card-header a {
        font-size: 0.75rem; font-weight: 700; color: #eca413;
        text-decoration: none;
    }
    .log-card-header a:hover { text-decoration: underline; }

    .log-item {
        display: flex; align-items: flex-start; gap: 12px;
        padding: 12px; border-radius: 12px;
        background: #fcfaf8; border: 1px solid #e7dfcf;
        margin-bottom: 0.75rem;
    }
    .log-item:last-child { margin-bottom: 0; }
    .log-item-text { font-size: 0.875rem; font-weight: 500; color: #1b170d; margin: 0; }
    .log-item-time { font-size: 0.75rem; color: #9a804c; margin: 0; }

    /* ===== Chart Card ===== */
    .chart-card {
        background: #ffffff; padding: 1.5rem;
        border-radius: 1rem; border: 1px solid #e7dfcf;
        box-shadow: 0 1px 3px 0 rgba(0,0,0,0.05);
    }
    .chart-header {
        display: flex; align-items: center; justify-content: space-between;
        margin-bottom: 1rem;
    }
    .chart-header h3 { font-size: 1rem; font-weight: 700; color: #1b170d; margin: 0; }
    .chart-legend {
        display: flex; align-items: center; gap: 8px;
    }
    .chart-legend-dot {
        width: 8px; height: 8px; border-radius: 50%; background: #eca413;
    }
    .chart-legend span { font-size: 0.75rem; color: #9a804c; }

    .chart-bars {
        display: flex; align-items: flex-end; justify-content: space-between;
        gap: 8px; height: 160px; padding: 0 8px;
    }
    .chart-bar {
        flex: 1; border-radius: 6px 6px 0 0;
        background: rgba(236,164,19,0.1); transition: background 0.2s;
        cursor: pointer; position: relative;
    }
    .chart-bar:hover { background: rgba(236,164,19,0.25); }
    .chart-bar.highlight {
        background: #eca413;
        box-shadow: 0 8px 24px -4px rgba(236,164,19,0.25);
    }
    .chart-labels {
        display: flex; justify-content: space-between;
        padding: 8px 8px 0; font-size: 0.75rem; color: #9a804c; font-weight: 500;
    }

    /* ===== Button Styles ===== */
    .stButton > button {
        border-radius: 12px;
        padding: 0.5rem 1.5rem;
        font-weight: 700;
        font-family: 'Manrope', sans-serif;
    }
    div[data-testid="stButton"] > button[kind="primary"] {
        background: #eca413;
        border: none;
        box-shadow: 0 8px 24px -4px rgba(236,164,19,0.25);
    }
    div[data-testid="stButton"] > button[kind="primary"]:hover {
        background: #b87d0e;
    }

    /* ===== Filter / Action Buttons ===== */
    .action-btn {
        display: inline-flex; align-items: center; gap: 8px;
        padding: 8px 16px; border-radius: 12px;
        border: 1px solid #e7dfcf; background: white;
        font-size: 0.875rem; font-weight: 700; color: #1b170d;
        cursor: pointer; transition: background 0.2s;
        box-shadow: 0 1px 2px rgba(0,0,0,0.04);
    }
    .action-btn:hover { background: #f3efe7; }

    /* ===== Progress Bar ===== */
    .stProgress > div > div > div > div {
        background: linear-gradient(90deg, #eca413, #f0c050);
        border-radius: 10px;
    }

    /* ===== Dataframe / Table overrides ===== */
    .dataframe { font-size: 0.9rem; }

    /* ===== Tabs ===== */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    .stTabs [data-baseweb="tab"] {
        border-radius: 12px; padding: 10px 20px;
        font-weight: 600; font-size: 0.875rem;
    }
    .stTabs [aria-selected="true"] {
        background: rgba(236,164,19,0.1);
    }

    /* Hide streamlit default header/footer */
    #MainMenu { visibility: hidden; }
    header[data-testid="stHeader"] { background: rgba(253,252,248,0.8); backdrop-filter: blur(10px); }
    footer { visibility: hidden; }

    /* ===== Metric overrides ===== */
    [data-testid="stMetric"] {
        background: white;
        padding: 1rem;
        border-radius: 12px;
        border: 1px solid #e7dfcf;
        box-shadow: 0 1px 3px rgba(0,0,0,0.05);
    }
    [data-testid="stMetricLabel"] { color: #9a804c; }
    [data-testid="stMetricValue"] { color: #1b170d; font-weight: 900; }
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
    if 'openai_api_key' not in st.session_state:
        st.session_state.openai_api_key = load_api_key()
    if 'summary_excel_path' not in st.session_state:
        st.session_state.summary_excel_path = None
    if 'ai_table_generated' not in st.session_state:
        st.session_state.ai_table_generated = False
    if 'validation_result' not in st.session_state:
        st.session_state.validation_result = None
    if 'disclosure_running' not in st.session_state:
        st.session_state.disclosure_running = False
    if 'disclosure_results' not in st.session_state:
        st.session_state.disclosure_results = []
    if 'disclosure_logs' not in st.session_state:
        st.session_state.disclosure_logs = []
    if 'disclosure_zip_path' not in st.session_state:
        st.session_state.disclosure_zip_path = None
    if 'scraping_save_path' not in st.session_state:
        st.session_state.scraping_save_path = ""
    if 'disclosure_save_path' not in st.session_state:
        st.session_state.disclosure_save_path = ""


def main():
    """메인 함수"""
    init_session_state()

    # ========== Sidebar ==========
    with st.sidebar:
        st.markdown("""
        <div class="sidebar-brand">
            <div class="sidebar-brand-icon">
                <span class="material-symbols-outlined" style="font-size:24px;">savings</span>
            </div>
            <div class="sidebar-brand-text">
                <h1>Savings Bank Data</h1>
                <p>Crawling System</p>
            </div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("""
        <nav class="sidebar-nav">
            <a class="active" href="#">
                <span class="material-symbols-outlined" style="font-size:20px;">dashboard</span>
                Dashboard
            </a>
            <a href="#">
                <span class="material-symbols-outlined" style="font-size:20px;">tune</span>
                Crawler Config
            </a>
            <a href="#">
                <span class="material-symbols-outlined" style="font-size:20px;">description</span>
                Data Logs
            </a>
            <a href="#">
                <span class="material-symbols-outlined" style="font-size:20px;">analytics</span>
                Reports
            </a>
        </nav>
        <hr style="border:none; border-top:1px solid #e7dfcf; margin:12px 0;">
        <nav class="sidebar-nav">
            <a href="#">
                <span class="material-symbols-outlined" style="font-size:20px;">settings</span>
                Settings
            </a>
        </nav>
        """, unsafe_allow_html=True)

        st.markdown("<div style='height:1rem'></div>", unsafe_allow_html=True)

        # New Crawl button (Streamlit button for actual functionality)
        st.markdown("""
        <div style="padding:0 0 1rem 0;">
            <div class="sidebar-cta">
                <span class="material-symbols-outlined" style="font-size:20px;">add_circle</span>
                New Crawl
            </div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("""
        <div class="sidebar-profile">
            <div class="sidebar-profile-avatar">
                <img src="https://lh3.googleusercontent.com/aida-public/AB6AXuDUVrXIHxhrmmheAOHvPOY9Bf8nbXVg-5dVUuad_vmS8buKJlyFF6t4jFsPQVO3KZH5l2tfeBHK4l41cMvgj7zYahKCZffWqK1mzKvZWMTYy0tItipKB05Q5Ll2Kwmofu98yezgXk7Htx4WlkpWyfZuOPFvEaUs8T6dN3aR_X40kwXAVguecQOJXuXOiLK8elrumbIPbGtT4OFp8Q7_VjeY5J9w5pNuln2A5rjDxFDrInkLGksAnSE0ygy6cYwgq49qs5ap1l7CPNo" alt="Profile"/>
            </div>
            <div>
                <p class="sidebar-profile-name">Admin User</p>
                <p class="sidebar-profile-role">System Administrator</p>
            </div>
        </div>
        """, unsafe_allow_html=True)

    # ========== Main Content Header ==========
    st.markdown("""
    <div class="dashboard-header">
        <h2>Dashboard Overview</h2>
        <p>Real-time monitoring of savings bank public disclosure data.</p>
    </div>
    """, unsafe_allow_html=True)
    st.markdown("<div style='height:0.5rem'></div>", unsafe_allow_html=True)

    # ========== Stat Cards ==========
    stat_col1, stat_col2, stat_col3 = st.columns(3)

    # Calculate live stats
    active_crawlers = len(st.session_state.selected_banks) if st.session_state.scraping_running else 0
    total_crawlers = 79
    data_collected = sum(1 for r in st.session_state.results if r.get('success', False)) if st.session_state.results else 0
    total_records = len(st.session_state.results) if st.session_state.results else 0
    health_pct = "99.9%"

    with stat_col1:
        st.markdown(f"""
        <div class="stat-card">
            <div style="display:flex; justify-content:space-between; align-items:flex-start; position:relative; z-index:1;">
                <div class="stat-card-icon">
                    <span class="material-symbols-outlined">bug_report</span>
                </div>
                <span class="stat-card-badge badge-green">+{active_crawlers} active</span>
            </div>
            <div style="margin-top:1rem; position:relative; z-index:1;">
                <p class="stat-card-label">Active Crawlers</p>
                <p class="stat-card-value">{active_crawlers} <span>/ {total_crawlers}</span></p>
            </div>
        </div>
        """, unsafe_allow_html=True)

    with stat_col2:
        display_data = f"{data_collected:,}" if data_collected > 0 else "12,840"
        today_count = f"+{total_records}" if total_records > 0 else "+1.5k today"
        st.markdown(f"""
        <div class="stat-card">
            <div style="display:flex; justify-content:space-between; align-items:flex-start; position:relative; z-index:1;">
                <div class="stat-card-icon">
                    <span class="material-symbols-outlined">database</span>
                </div>
                <span class="stat-card-badge badge-green">{today_count}</span>
            </div>
            <div style="margin-top:1rem; position:relative; z-index:1;">
                <p class="stat-card-label">Data Collected</p>
                <p class="stat-card-value">{display_data}</p>
            </div>
        </div>
        """, unsafe_allow_html=True)

    with stat_col3:
        st.markdown(f"""
        <div class="stat-card">
            <div style="display:flex; justify-content:space-between; align-items:flex-start; position:relative; z-index:1;">
                <div class="stat-card-icon">
                    <span class="material-symbols-outlined">health_and_safety</span>
                </div>
                <span class="stat-card-badge badge-amber">Stable</span>
            </div>
            <div style="margin-top:1rem; position:relative; z-index:1;">
                <p class="stat-card-label">System Health</p>
                <p class="stat-card-value">{health_pct}</p>
            </div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<div style='height:1rem'></div>", unsafe_allow_html=True)

    if not SCRAPER_AVAILABLE:
        st.error("스크래퍼 모듈을 로드할 수 없습니다. 필요한 패키지가 설치되어 있는지 확인하세요.")
        return

    config = Config()
    all_banks = config.BANKS

    # ========== 메인 탭 구조 ==========
    tab_scraping, tab_disclosure = st.tabs(["📊 데이터 스크래핑", "📥 경영공시/감사보고서 다운로드"])

    # ====================================================================
    # 탭 1: 데이터 스크래핑
    # ====================================================================
    with tab_scraping:

        # ========== 설정 섹션 ==========
        st.markdown('<div class="section-title"><span class="material-symbols-outlined" style="font-size:20px;color:#eca413;">tune</span> 스크래핑 설정</div>', unsafe_allow_html=True)

        col1, col2 = st.columns([1, 1])

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

        col3, col4 = st.columns([2, 1])

        with col3:
            scraping_save_path = st.text_input(
                "📂 파일 저장 경로",
                value=st.session_state.scraping_save_path,
                placeholder="예: /home/user/Downloads/scraping_data",
                help="스크래핑 결과 파일이 저장될 폴더 경로를 지정하세요. 비워두면 임시 폴더에 저장됩니다.",
                key="scraping_save_path_input"
            )
            st.session_state.scraping_save_path = scraping_save_path
            if scraping_save_path:
                if os.path.isdir(scraping_save_path):
                    st.caption("✅ 유효한 경로입니다.")
                else:
                    st.caption("📁 해당 경로가 없으면 자동으로 생성됩니다.")
            else:
                st.caption("💡 비워두면 시스템 임시 폴더에 저장됩니다.")

        with col4:
            auto_zip = st.checkbox("🗜️ 완료 후 자동 압축", value=True)
            save_md = st.checkbox("📝 MD 파일도 함께 생성", value=False)

        st.divider()

        # ========== GPT-5.2 API 설정 섹션 ==========
        st.markdown('<div class="section-title"><span class="material-symbols-outlined" style="font-size:20px;color:#eca413;">smart_toy</span> GPT-5.2 API 설정 (엑셀 자동 생성)</div>', unsafe_allow_html=True)

        if EXCEL_GENERATOR_AVAILABLE and OPENAI_AVAILABLE:
            api_key = st.session_state.openai_api_key

            col1, col2 = st.columns([2, 1])
            with col1:
                if api_key:
                    st.success("✅ API Key가 설정되어 있습니다. (`.streamlit/secrets.toml` 또는 환경변수)")
                else:
                    st.warning(
                        "⚠️ API Key가 설정되지 않았습니다.\n\n"
                        "**설정 방법 (택 1):**\n"
                        "1. `.streamlit/secrets.toml` 파일에 `OPENAI_API_KEY = \"sk-...\"` 입력\n"
                        "2. 환경변수 `OPENAI_API_KEY` 설정"
                    )

            with col2:
                use_chatgpt = st.checkbox(
                    "🤖 GPT-5.2로 엑셀 생성",
                    value=bool(api_key),
                    disabled=not api_key,
                    help="활성화하면 GPT-5.2가 데이터를 분석하여 요약 엑셀을 생성합니다."
                )
        else:
            use_chatgpt = False
            api_key = ""
            st.warning("⚠️ GPT-5.2 기능을 사용하려면 openai 패키지가 필요합니다: `pip install openai>=2.0.0`")

        st.divider()

        # ========== 은행 선택 섹션 ==========
        st.markdown('<div class="section-title"><span class="material-symbols-outlined" style="font-size:20px;color:#eca413;">account_balance</span> 은행 선택</div>', unsafe_allow_html=True)

        # 전체 선택/해제 버튼 (중앙 정렬)
        col1, col2, col3, col4, col5 = st.columns([1, 1, 1, 1, 1])
        with col2:
            if st.button("✅ 전체 선택", use_container_width=True, type="primary"):
                for bank in all_banks:
                    st.session_state[f"bank_{bank}"] = True
                st.session_state.selected_banks = all_banks.copy()
                st.rerun()
        with col3:
            st.metric("선택된 은행", f"{len(st.session_state.selected_banks)}개 / 79개")
        with col4:
            if st.button("❌ 전체 해제", use_container_width=True):
                for bank in all_banks:
                    st.session_state[f"bank_{bank}"] = False
                st.session_state.selected_banks = []
                st.rerun()

        st.write("")

        # 은행 체크박스 그리드 (중앙 정렬, 8열)
        st.markdown("**은행을 개별 선택하거나 전체 선택 버튼을 사용하세요:**")

        cols_per_row = 8
        rows = [all_banks[i:i + cols_per_row] for i in range(0, len(all_banks), cols_per_row)]

        for bank in all_banks:
            if f"bank_{bank}" not in st.session_state:
                st.session_state[f"bank_{bank}"] = bank in st.session_state.selected_banks

        for row in rows:
            cols = st.columns(cols_per_row)
            for idx, bank in enumerate(row):
                with cols[idx]:
                    st.checkbox(bank, key=f"bank_{bank}")

        selected_banks = [bank for bank in all_banks if st.session_state.get(f"bank_{bank}", False)]
        st.session_state.selected_banks = selected_banks

        st.divider()

        # ========== 실행 섹션 ==========
        st.markdown('<div class="section-title"><span class="material-symbols-outlined" style="font-size:20px;color:#eca413;">rocket_launch</span> 스크래핑 실행</div>', unsafe_allow_html=True)

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

        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            start_disabled = not selected_banks or st.session_state.scraping_running
            if st.button("🚀 스크래핑 시작", type="primary", use_container_width=True, disabled=start_disabled):
                if not selected_banks:
                    st.error("스크래핑할 은행을 선택하세요.")
                else:
                    st.session_state.ai_table_generated = False
                    run_scraping(
                        selected_banks,
                        scrape_type,
                        auto_zip,
                        download_filename,
                        use_chatgpt=use_chatgpt,
                        api_key=api_key,
                        save_path=scraping_save_path
                    )

        if st.session_state.scraping_running:
            st.info("⏳ 스크래핑이 진행 중입니다. 잠시만 기다려주세요...")

        st.divider()

        # ========== 결과 섹션 ==========
        st.markdown('<div class="section-title"><span class="material-symbols-outlined" style="font-size:20px;color:#eca413;">analytics</span> 스크래핑 결과 <span class="live-badge">Live</span></div>', unsafe_allow_html=True)

        if st.session_state.results:
            results = st.session_state.results
            success_count = sum(1 for r in results if r['success'])
            fail_count = len(results) - success_count

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

            df = create_summary_dataframe(results, st.session_state.bank_dates)
            st.dataframe(df, use_container_width=True, hide_index=True)

            st.write("")

            # ========== AI 표 정리 및 엑셀 반환 옵션 ==========
            st.markdown("#### 🤖 GPT-5.2 AI 표 정리 및 엑셀 반환")

            if EXCEL_GENERATOR_AVAILABLE and OPENAI_AVAILABLE and st.session_state.openai_api_key:
                if st.session_state.summary_excel_path and os.path.exists(st.session_state.summary_excel_path):
                    try:
                        preview_df = pd.read_excel(st.session_state.summary_excel_path, sheet_name='분기총괄')
                        st.markdown("**AI 분석 결과 미리보기:**")
                        st.dataframe(preview_df, use_container_width=True, hide_index=True)
                    except Exception:
                        pass

                    _display_validation_result(st.session_state.validation_result)

                    col1, col2, col3 = st.columns([1, 2, 1])
                    with col2:
                        with open(st.session_state.summary_excel_path, 'rb') as f:
                            st.download_button(
                                label="📊 분기총괄 엑셀 다운로드",
                                data=f,
                                file_name=f"저축은행_분기총괄_{datetime.now().strftime('%Y%m%d')}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                use_container_width=True,
                                type="secondary"
                            )
                else:
                    st.info("💡 GPT-5.2를 활용하여 스크래핑 데이터를 표로 정리하고 엑셀로 반환할 수 있습니다.")
                    col1, col2, col3 = st.columns([1, 2, 1])
                    with col2:
                        if st.button("🤖 AI로 표 정리 및 엑셀 생성", use_container_width=True, type="secondary"):
                            with st.spinner("GPT-5.2가 데이터를 분석하고 정합성을 검증하는 중..."):
                                try:
                                    gen_result = generate_excel_with_chatgpt(
                                        scraped_results=results,
                                        api_key=st.session_state.openai_api_key,
                                        use_ai=True,
                                        validate=True
                                    )
                                    summary_path = gen_result.get("filepath") if isinstance(gen_result, dict) else gen_result
                                    validation = gen_result.get("validation") if isinstance(gen_result, dict) else None

                                    if summary_path:
                                        st.session_state.summary_excel_path = summary_path
                                        st.session_state.validation_result = validation
                                        st.session_state.ai_table_generated = True
                                        st.success("✅ AI 표 정리, 정합성 검증 및 엑셀 생성 완료!")
                                        st.rerun()
                                    else:
                                        st.error("엑셀 생성에 실패했습니다.")
                                except Exception as e:
                                    st.error(f"AI 엑셀 생성 중 오류: {str(e)}")
            else:
                if not st.session_state.openai_api_key:
                    st.info("💡 `.streamlit/secrets.toml`에 API Key를 설정하면 AI 표 정리 기능을 사용할 수 있습니다.")
                elif not EXCEL_GENERATOR_AVAILABLE or not OPENAI_AVAILABLE:
                    st.info("💡 `pip install openai>=2.0.0` 설치 후 AI 표 정리 기능을 사용할 수 있습니다.")

            st.write("")

            # ZIP 파일 다운로드
            if 'zip_path' in st.session_state and st.session_state.zip_path:
                st.markdown("#### 📦 전체 데이터 압축 파일")
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
            # Show sample "Recent Crawling Activities" table when no results
            st.markdown("""
            <div style="border-radius:1rem; overflow:hidden; border:1px solid #e7dfcf; box-shadow:0 1px 3px rgba(0,0,0,0.05);">
            <table class="custom-table" style="margin:0;">
                <thead>
                    <tr>
                        <th>Bank Name</th>
                        <th>Status</th>
                        <th>Last Updated</th>
                        <th>Records Found</th>
                        <th style="text-align:right;">Action</th>
                    </tr>
                </thead>
                <tbody>
                    <tr>
                        <td>
                            <div style="display:flex;align-items:center;gap:12px;">
                                <div class="table-bank-avatar">OK</div>
                                <span class="table-bank-name">OK Savings Bank</span>
                            </div>
                        </td>
                        <td><span class="status-badge status-success"><span class="status-dot pulse"></span> Success</span></td>
                        <td><div><span style="font-weight:500;">2023-10-27</span><br/><span style="font-size:0.75rem;color:#9a804c;">14:30:22</span></div></td>
                        <td><span style="font-weight:700;">142</span> <span style="font-size:0.75rem;color:#9a804c;">items</span></td>
                        <td style="text-align:right;"><span class="material-symbols-outlined" style="color:#9a804c;font-size:20px;">visibility</span></td>
                    </tr>
                    <tr>
                        <td>
                            <div style="display:flex;align-items:center;gap:12px;">
                                <div class="table-bank-avatar">SB</div>
                                <span class="table-bank-name">SBI Savings Bank</span>
                            </div>
                        </td>
                        <td><span class="status-badge status-running"><span class="material-symbols-outlined" style="font-size:14px;animation:spin 1s linear infinite;">sync</span> Running</span></td>
                        <td><div><span style="font-weight:500;">2023-10-27</span><br/><span style="font-size:0.75rem;color:#9a804c;">14:25:10</span></div></td>
                        <td><span style="font-weight:700;color:#9a804c;font-style:italic;">Pending...</span></td>
                        <td style="text-align:right;"><span class="material-symbols-outlined" style="color:#d32f2f;font-size:20px;">stop_circle</span></td>
                    </tr>
                    <tr>
                        <td>
                            <div style="display:flex;align-items:center;gap:12px;">
                                <div class="table-bank-avatar">WC</div>
                                <span class="table-bank-name">Welcome Savings Bank</span>
                            </div>
                        </td>
                        <td><span class="status-badge status-success"><span class="status-dot"></span> Success</span></td>
                        <td><div><span style="font-weight:500;">2023-10-27</span><br/><span style="font-size:0.75rem;color:#9a804c;">13:15:00</span></div></td>
                        <td><span style="font-weight:700;">98</span> <span style="font-size:0.75rem;color:#9a804c;">items</span></td>
                        <td style="text-align:right;"><span class="material-symbols-outlined" style="color:#9a804c;font-size:20px;">visibility</span></td>
                    </tr>
                    <tr>
                        <td>
                            <div style="display:flex;align-items:center;gap:12px;">
                                <div class="table-bank-avatar">PP</div>
                                <span class="table-bank-name">Pepper Savings Bank</span>
                            </div>
                        </td>
                        <td><span class="status-badge status-failed"><span class="material-symbols-outlined" style="font-size:14px;">error</span> Failed</span></td>
                        <td><div><span style="font-weight:500;">2023-10-27</span><br/><span style="font-size:0.75rem;color:#9a804c;">12:00:45</span></div></td>
                        <td><span style="font-weight:700;color:#9a804c;">0</span> <span style="font-size:0.75rem;color:#9a804c;">items</span></td>
                        <td style="text-align:right;"><span style="font-size:0.75rem;font-weight:700;color:#eca413;">Retry</span> <span class="material-symbols-outlined" style="color:#eca413;font-size:18px;vertical-align:middle;">replay</span></td>
                    </tr>
                    <tr>
                        <td>
                            <div style="display:flex;align-items:center;gap:12px;">
                                <div class="table-bank-avatar">KI</div>
                                <span class="table-bank-name">Korea Investment</span>
                            </div>
                        </td>
                        <td><span class="status-badge status-success"><span class="status-dot"></span> Success</span></td>
                        <td><div><span style="font-weight:500;">2023-10-27</span><br/><span style="font-size:0.75rem;color:#9a804c;">11:45:12</span></div></td>
                        <td><span style="font-weight:700;">210</span> <span style="font-size:0.75rem;color:#9a804c;">items</span></td>
                        <td style="text-align:right;"><span class="material-symbols-outlined" style="color:#9a804c;font-size:20px;">visibility</span></td>
                    </tr>
                </tbody>
            </table>
            <div class="table-pagination">
                <span style="font-weight:500;">Showing 1-5 of 120 items</span>
                <div style="display:flex;gap:8px;">
                    <span class="page-btn" style="opacity:0.5;cursor:default;"><span class="material-symbols-outlined" style="font-size:14px;">chevron_left</span></span>
                    <span class="page-btn active">1</span>
                    <span class="page-btn">2</span>
                    <span class="page-btn">3</span>
                    <span class="page-btn"><span class="material-symbols-outlined" style="font-size:14px;">chevron_right</span></span>
                </div>
            </div>
            </div>
            """, unsafe_allow_html=True)

        # ========== 로그 섹션 ==========
        st.divider()
        with st.expander("📝 실행 로그 보기", expanded=False):
            if st.session_state.logs:
                log_text = "\n".join(st.session_state.logs)
                st.text_area("로그", value=log_text, height=300, disabled=True)

                if st.button("🗑️ 로그 지우기"):
                    st.session_state.logs = []
                    st.rerun()
            else:
                st.info("로그가 없습니다.")

    # ====================================================================
    # 탭 2: 경영공시/감사보고서 파일 다운로드
    # ====================================================================
    with tab_disclosure:

        st.markdown('<div class="section-title"><span class="material-symbols-outlined" style="font-size:20px;color:#eca413;">download</span> 통일경영공시/감사보고서 파일 다운로드</div>', unsafe_allow_html=True)

        if DOWNLOADER_AVAILABLE:
            st.info(
                "💡 저축은행중앙회 사이트에서 **통일경영공시 파일**과 **감사(검토)보고서**를 "
                "자동으로 일괄 다운로드합니다. (Selenium 기반)\n\n"
                f"**대상 URL:** `{TARGET_URL}`"
            )

            # 저장 경로 설정
            disclosure_save_path = st.text_input(
                "📂 파일 저장 경로",
                value=st.session_state.disclosure_save_path,
                placeholder="예: /home/user/Downloads/disclosure_files",
                help="공시파일이 저장될 폴더 경로를 지정하세요. 비워두면 임시 폴더에 저장됩니다.",
                key="disclosure_save_path_input"
            )
            st.session_state.disclosure_save_path = disclosure_save_path
            if disclosure_save_path:
                if os.path.isdir(disclosure_save_path):
                    st.caption("✅ 유효한 경로입니다.")
                else:
                    st.caption("📁 해당 경로가 없으면 자동으로 생성됩니다.")
            else:
                st.caption("💡 비워두면 시스템 임시 폴더에 저장됩니다.")

            st.markdown("<div style='height:0.5rem'></div>", unsafe_allow_html=True)

            col1, col2, col3 = st.columns([1, 2, 1])
            with col2:
                disclosure_disabled = st.session_state.disclosure_running or st.session_state.scraping_running
                if st.button(
                    "📥 공시파일 일괄 다운로드 시작",
                    type="primary",
                    use_container_width=True,
                    disabled=disclosure_disabled,
                    key="btn_disclosure_download"
                ):
                    run_disclosure_download(disclosure_save_path)

            if st.session_state.disclosure_running:
                st.info("⏳ 공시파일 다운로드가 진행 중입니다...")

            # 다운로드 결과 표시
            if st.session_state.disclosure_results:
                st.divider()
                st.markdown("#### 📊 다운로드 결과")

                dl_results = st.session_state.disclosure_results
                success = len([r for r in dl_results if r['상태'] == '완료'])
                partial = len([r for r in dl_results if r['상태'] == '부분완료'])
                failed = len([r for r in dl_results if r['상태'] == '실패'])

                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("전체", f"{len(dl_results)}개")
                with col2:
                    st.metric("완료", f"{success}개")
                with col3:
                    st.metric("부분완료", f"{partial}개")
                with col4:
                    st.metric("실패", f"{failed}개")

                st.dataframe(
                    pd.DataFrame(dl_results),
                    use_container_width=True,
                    hide_index=True
                )

                # ZIP 다운로드 버튼
                if st.session_state.disclosure_zip_path and os.path.exists(st.session_state.disclosure_zip_path):
                    col1, col2, col3 = st.columns([1, 2, 1])
                    with col2:
                        with open(st.session_state.disclosure_zip_path, 'rb') as f:
                            st.download_button(
                                label="📥 공시파일 ZIP 다운로드",
                                data=f,
                                file_name=f"저축은행_공시파일_{datetime.now().strftime('%Y%m%d')}.zip",
                                mime="application/zip",
                                use_container_width=True,
                                type="primary",
                                key="btn_disclosure_zip"
                            )

            # 다운로드 로그
            if st.session_state.disclosure_logs:
                with st.expander("📝 다운로드 로그", expanded=False):
                    st.text_area(
                        "로그",
                        value="\n".join(st.session_state.disclosure_logs[-100:]),
                        height=200,
                        disabled=True,
                        key="disclosure_log_area"
                    )
        else:
            st.warning(
                "⚠️ 공시파일 다운로드 기능을 사용할 수 없습니다.\n\n"
                "**필요 조건:**\n"
                "- `selenium` 패키지 설치\n"
                "- `downloader_core.py` 파일이 프로젝트 루트에 존재"
            )

    # ========== Bottom Grid: System Logs + API Usage ==========
    st.markdown("<div style='height:1rem'></div>", unsafe_allow_html=True)

    bottom_col1, bottom_col2 = st.columns(2)

    with bottom_col1:
        # System Logs Card
        log_entries = st.session_state.logs[-3:] if st.session_state.logs else []
        log_html_items = ""
        if log_entries:
            for log_msg in log_entries:
                # Determine icon based on log content
                if "완료" in log_msg or "성공" in log_msg or "Success" in log_msg:
                    icon = '<span class="material-symbols-outlined" style="font-size:16px;color:#078810;margin-top:2px;">check_circle</span>'
                elif "오류" in log_msg or "실패" in log_msg or "Failed" in log_msg or "에러" in log_msg:
                    icon = '<span class="material-symbols-outlined" style="font-size:16px;color:#d32f2f;margin-top:2px;">error</span>'
                elif "경고" in log_msg or "Warning" in log_msg or "주의" in log_msg:
                    icon = '<span class="material-symbols-outlined" style="font-size:16px;color:#e6a700;margin-top:2px;">warning</span>'
                else:
                    icon = '<span class="material-symbols-outlined" style="font-size:16px;color:#4a90d9;margin-top:2px;">info</span>'
                log_html_items += f"""
                <div class="log-item">
                    {icon}
                    <div>
                        <p class="log-item-text">{log_msg[:80]}</p>
                        <p class="log-item-time">Recent</p>
                    </div>
                </div>"""
        else:
            log_html_items = """
            <div class="log-item">
                <span class="material-symbols-outlined" style="font-size:16px;color:#078810;margin-top:2px;">check_circle</span>
                <div>
                    <p class="log-item-text">Cron job completed successfully</p>
                    <p class="log-item-time">Today, 14:30 PM</p>
                </div>
            </div>
            <div class="log-item">
                <span class="material-symbols-outlined" style="font-size:16px;color:#e6a700;margin-top:2px;">warning</span>
                <div>
                    <p class="log-item-text">High latency detected on SBI crawler</p>
                    <p class="log-item-time">Today, 14:22 PM</p>
                </div>
            </div>
            <div class="log-item">
                <span class="material-symbols-outlined" style="font-size:16px;color:#4a90d9;margin-top:2px;">info</span>
                <div>
                    <p class="log-item-text">System maintenance scheduled</p>
                    <p class="log-item-time">Yesterday, 09:00 AM</p>
                </div>
            </div>"""

        st.markdown(f"""
        <div class="log-card">
            <div class="log-card-header">
                <h3>System Logs</h3>
                <a href="#">View All</a>
            </div>
            {log_html_items}
        </div>
        """, unsafe_allow_html=True)

    with bottom_col2:
        # API Usage Chart Card
        st.markdown("""
        <div class="chart-card">
            <div class="chart-header">
                <h3>API Usage</h3>
                <div class="chart-legend">
                    <div class="chart-legend-dot"></div>
                    <span>Requests</span>
                </div>
            </div>
            <div class="chart-bars">
                <div class="chart-bar" style="height:40%;"></div>
                <div class="chart-bar" style="height:65%;"></div>
                <div class="chart-bar" style="height:45%;"></div>
                <div class="chart-bar" style="height:80%;"></div>
                <div class="chart-bar highlight" style="height:95%;"></div>
                <div class="chart-bar" style="height:50%;"></div>
                <div class="chart-bar" style="height:60%;"></div>
            </div>
            <div class="chart-labels">
                <span>Mon</span><span>Tue</span><span>Wed</span><span>Thu</span><span>Fri</span><span>Sat</span><span>Sun</span>
            </div>
        </div>
        """, unsafe_allow_html=True)

    # ========== 앱 정보 (탭 바깥) ==========
    st.markdown("<div style='height:1rem'></div>", unsafe_allow_html=True)
    with st.expander("ℹ️ 앱 정보", expanded=False):
        st.markdown("""
        ### 저축은행 공시자료 크롤링 시스템 v4.1

        **주요 기능:**
        - 79개 저축은행 분기공시/결산공시 데이터 자동 수집
        - 은행별 공시 날짜 표시
        - Excel 파일 형식으로 데이터 저장
        - ZIP 압축 다운로드 지원
        - 실시간 진행 상태 및 경과 시간 표시
        - GPT-5.2 API를 활용한 AI 표 정리 및 엑셀 자동 생성
        - API 키 보안 저장 지원 (.streamlit/secrets.toml, 환경변수)
        - 통일경영공시/감사보고서 파일 일괄 다운로드

        **사용 방법:**
        1. 스크래핑 유형 선택 (분기공시/결산공시)
        2. 스크래핑할 은행 선택 (전체 또는 개별)
        3. '스크래핑 시작' 버튼 클릭
        4. 완료 후 결과 파일 다운로드
        5. (선택) AI 표 정리 버튼으로 데이터 분석 엑셀 생성
        6. (선택) 공시파일 일괄 다운로드로 원본 파일 수집

        **API 키 설정:**
        - `.streamlit/secrets.toml` 파일에 `OPENAI_API_KEY = "sk-..."` 입력
        - 또는 환경변수 `OPENAI_API_KEY` 설정

        **데이터 출처:**
        - 저축은행중앙회 통일경영공시 (https://www.fsb.or.kr)
        """)


def _display_validation_result(validation):
    """정합성 검증 결과를 UI에 표시"""
    if not validation:
        return

    st.markdown("---")
    st.markdown("#### 🔍 정합성 검증 결과")

    score = validation.get("score", 0)
    is_valid = validation.get("is_valid", False)
    errors = validation.get("errors", [])
    warnings = validation.get("warnings", [])

    # 점수 및 판정 표시
    col1, col2, col3 = st.columns(3)
    with col1:
        if score >= 80:
            st.metric("정합성 점수", f"{score}점", delta="양호")
        elif score >= 50:
            st.metric("정합성 점수", f"{score}점", delta="주의", delta_color="off")
        else:
            st.metric("정합성 점수", f"{score}점", delta="미흡", delta_color="inverse")
    with col2:
        if is_valid:
            st.metric("판정", "✅ 통과")
        else:
            st.metric("판정", "⚠️ 오류 있음")
    with col3:
        st.metric("오류/경고", f"{len(errors)}건 / {len(warnings)}건")

    # AI 검증 요약
    ai_summary = validation.get("ai_checks", {}).get("summary", "")
    if ai_summary:
        st.info(f"🤖 **AI 검증 요약:** {ai_summary}")

    # 오류 목록
    if errors:
        with st.expander(f"❌ 오류 ({len(errors)}건)", expanded=True):
            for err in errors:
                st.error(f"• {err}")

    # 경고 목록
    if warnings:
        with st.expander(f"⚠️ 경고 ({len(warnings)}건)", expanded=False):
            for warn in warnings:
                st.warning(f"• {warn}")

    # 은행별 상세
    details = validation.get("details", {})
    if details:
        with st.expander("📋 은행별 검증 상세", expanded=False):
            detail_rows = []
            for bank, detail in details.items():
                status = detail.get("status", "unknown")
                status_icon = {"pass": "✅", "warn": "⚠️", "fail": "❌"}.get(status, "❓")
                issues = ", ".join(detail.get("issues", [])) or "이상 없음"
                detail_rows.append({"은행명": bank, "판정": f"{status_icon} {status}", "상세": issues})
            if detail_rows:
                st.dataframe(pd.DataFrame(detail_rows), use_container_width=True, hide_index=True)

    # 검증 시트 안내
    st.caption("💡 엑셀 파일의 '정합성검증' 시트에서 전체 검증 결과를 확인할 수 있습니다.")


def run_scraping(selected_banks, scrape_type, auto_zip, download_filename, use_chatgpt=False, api_key=None, save_path=None):
    """스크래핑 실행"""
    st.session_state.scraping_running = True
    st.session_state.results = []
    st.session_state.logs = []
    st.session_state.bank_dates = {}
    st.session_state.summary_excel_path = None
    st.session_state.validation_result = None

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
        config = Config(scrape_type, output_dir=save_path if save_path else None)
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

        # GPT-5.2로 분기총괄 엑셀 생성 및 정합성 검증
        if use_chatgpt and api_key and EXCEL_GENERATOR_AVAILABLE:
            status_text.markdown("**🤖 GPT-5.2가 분기총괄 엑셀 생성 및 정합성 검증 중...**")
            logger.log_message("GPT-5.2 API로 분기총괄 엑셀 생성 및 정합성 검증 시작")

            try:
                gen_result = generate_excel_with_chatgpt(
                    scraped_results=results,
                    api_key=api_key,
                    use_ai=True,
                    validate=True
                )
                summary_excel_path = gen_result.get("filepath") if isinstance(gen_result, dict) else gen_result
                validation = gen_result.get("validation") if isinstance(gen_result, dict) else None

                if summary_excel_path:
                    st.session_state.summary_excel_path = summary_excel_path
                    st.session_state.validation_result = validation
                    st.session_state.ai_table_generated = True
                    logger.log_message("GPT-5.2 분기총괄 엑셀 생성 완료")

                    if validation:
                        score = validation.get("score", 0)
                        error_count = len(validation.get("errors", []))
                        warn_count = len(validation.get("warnings", []))
                        logger.log_message(
                            f"정합성 검증 완료 - 점수: {score}/100, "
                            f"오류: {error_count}건, 경고: {warn_count}건"
                        )
                        if not validation.get("is_valid"):
                            logger.log_message("⚠️ 정합성 검증에서 오류가 발견되었습니다. 결과를 확인하세요.")
            except Exception as e:
                logger.log_message(f"AI 엑셀 생성 오류: {str(e)}")
                st.warning(f"⚠️ AI 엑셀 생성 중 오류 발생: {str(e)}")

        # 완료
        progress_bar.progress(1.0)
        success_count = sum(1 for r in results if r['success'])
        status_text.markdown(f"**✅ 완료!** 성공: {success_count}/{total_banks}")
        elapsed_text.markdown(f"⏱️ 총 소요 시간: **{format_elapsed_time(final_elapsed)}**")

        completion_msg = f"🎉 스크래핑 완료! 성공: {success_count}개, 실패: {total_banks - success_count}개, 소요시간: {format_elapsed_time(final_elapsed)}"
        if st.session_state.summary_excel_path:
            completion_msg += " | 🤖 GPT-5.2 엑셀 생성 완료"
            if st.session_state.validation_result:
                v_score = st.session_state.validation_result.get("score", 0)
                completion_msg += f" | 🔍 정합성: {v_score}점"
        st.success(completion_msg)
        st.session_state.logs = logger.messages.copy()

    except Exception as e:
        st.error(f"❌ 스크래핑 중 오류 발생: {str(e)}")
        st.session_state.logs.append(f"[오류] {str(e)}")

    finally:
        st.session_state.scraping_running = False


def run_disclosure_download(save_path=None):
    """통일경영공시/감사보고서 파일 다운로드 실행"""
    st.session_state.disclosure_running = True
    st.session_state.disclosure_results = []
    st.session_state.disclosure_logs = []
    st.session_state.disclosure_zip_path = None

    if save_path:
        download_path = os.path.abspath(save_path)
        os.makedirs(download_path, exist_ok=True)
    else:
        download_path = tempfile.mkdtemp(prefix="저축은행_공시파일_")
    logs = []

    def log_callback(msg):
        logs.append(msg)

    progress_container = st.container()
    with progress_container:
        progress_bar = st.progress(0)
        status_text = st.empty()
        log_area = st.empty()

    try:
        status_text.markdown("**📥 공시파일 다운로드 초기화 중...**")

        downloader = DisclosureDownloader(
            download_path=download_path,
            log_callback=log_callback,
            headless=True
        )

        # 은행 목록 추출
        status_text.markdown("**🌐 웹사이트 접속 및 은행 목록 추출 중...**")
        bank_list = downloader.start_and_extract_banks()

        if not bank_list:
            st.error("은행 목록을 추출할 수 없습니다.")
            return

        status_text.markdown(f"**📥 {len(bank_list)}개 은행 공시파일 다운로드 중...**")

        # 다운로드 실행
        def progress_callback(current, total, bank_name):
            progress = (current + 1) / total
            progress_bar.progress(progress)
            status_text.markdown(f"**📥 처리 중:** {bank_name} ({current + 1}/{total})")
            st.session_state.disclosure_logs = logs.copy()
            log_area.text_area(
                "실시간 로그",
                value="\n".join(logs[-30:]),
                height=150,
                disabled=True,
                key=f"dl_log_{current}"
            )

        total_downloaded = downloader.download_all(bank_list, progress_callback)

        # 보고서 생성
        downloader.create_report()

        # 다운로드된 파일 ZIP 압축
        downloaded_files = [
            os.path.join(download_path, f)
            for f in os.listdir(download_path)
            if not f.endswith(('.json', '.tmp', '.crdownload'))
        ]

        if downloaded_files:
            zip_path = os.path.join(
                download_path,
                f"저축은행_공시파일_{datetime.now().strftime('%Y%m%d')}.zip"
            )
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for fpath in downloaded_files:
                    if os.path.isfile(fpath) and not fpath.endswith('.zip'):
                        zipf.write(fpath, os.path.basename(fpath))
            st.session_state.disclosure_zip_path = zip_path

        # 결과 저장
        st.session_state.disclosure_results = downloader.results
        st.session_state.disclosure_logs = logs

        # 완료
        progress_bar.progress(1.0)
        success = len([r for r in downloader.results if r['상태'] in ['완료', '부분완료']])
        status_text.markdown(f"**✅ 완료!** 성공: {success}/{len(bank_list)}, 총 {total_downloaded}개 파일")
        st.success(f"🎉 공시파일 다운로드 완료! {total_downloaded}개 파일 다운로드됨")

        downloader.cleanup()

    except Exception as e:
        st.error(f"❌ 공시파일 다운로드 중 오류: {str(e)}")
        st.session_state.disclosure_logs = logs

    finally:
        st.session_state.disclosure_running = False


if __name__ == "__main__":
    main()
