"""
저축은행 중앙회 통일경영공시 데이터 스크래퍼
Streamlit 웹 앱 버전 v4.1
- GPT-5.2 API 업그레이드
- API 키 보안 저장 (.streamlit/secrets.toml / 환경변수)
- 스크래핑 완료 후 AI 표 정리 및 엑셀 반환 옵션 추가
- 통일경영공시/감사보고서 파일 다운로드 기능 추가
"""

import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import os
import time
import tempfile
import threading
import zipfile
import base64
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

    /* ===== Folder Browser ===== */
    .folder-browser-panel {
        background: #ffffff;
        border: 1px solid #e7dfcf;
        border-radius: 12px;
        padding: 1rem;
        margin-top: 0.5rem;
    }
    .folder-browser-path {
        display: flex; align-items: center; gap: 8px;
        padding: 8px 12px;
        background: #fcfaf8;
        border: 1px solid #e7dfcf;
        border-radius: 8px;
        font-size: 0.8rem;
        color: #1b170d;
        font-family: 'Manrope', monospace;
        word-break: break-all;
        margin-bottom: 0.75rem;
    }
    .folder-browser-path .material-symbols-outlined {
        color: #eca413; font-size: 18px; flex-shrink: 0;
    }
    .folder-grid {
        display: grid;
        grid-template-columns: repeat(auto-fill, minmax(160px, 1fr));
        gap: 6px;
        max-height: 240px;
        overflow-y: auto;
        padding: 4px;
    }
    .folder-item {
        display: flex; align-items: center; gap: 6px;
        padding: 8px 10px;
        border-radius: 8px;
        border: 1px solid transparent;
        font-size: 0.8rem;
        font-weight: 500;
        color: #1b170d;
        cursor: pointer;
        transition: all 0.15s;
        background: #fcfaf8;
        text-decoration: none;
        overflow: hidden;
        white-space: nowrap;
        text-overflow: ellipsis;
    }
    .folder-item:hover {
        background: #f3efe7;
        border-color: #e7dfcf;
    }
    .folder-item .material-symbols-outlined {
        color: #eca413; font-size: 18px; flex-shrink: 0;
    }
</style>
""", unsafe_allow_html=True)


def _get_default_download_path():
    """기본 다운로드 폴더 경로를 반환"""
    default = r"C:\Users\OK\Downloads"
    if os.path.isdir(default):
        return default
    # fallback: 홈 디렉토리의 Downloads
    fallback = os.path.join(os.path.expanduser("~"), "Downloads")
    if os.path.isdir(fallback):
        return fallback
    return os.path.expanduser("~")


def folder_picker(key_prefix, label="📂 서버 저장 경로", default_path=""):
    """인터랙티브 폴더 브라우저 위젯 (서버 파일시스템)

    Args:
        key_prefix: 세션 상태 키 접두어 (고유해야 함)
        label: 위젯 라벨
        default_path: 기본 선택 경로 (비어있으면 OS 다운로드 폴더)

    Returns:
        선택된 폴더 경로 (str) 또는 빈 문자열
    """
    # 세션 상태 키
    browse_key = f"{key_prefix}_browsing"
    nav_key = f"{key_prefix}_nav_path"
    selected_key = f"{key_prefix}_selected"

    resolved_default = default_path if default_path else _get_default_download_path()

    if browse_key not in st.session_state:
        st.session_state[browse_key] = False
    if nav_key not in st.session_state:
        st.session_state[nav_key] = resolved_default
    if selected_key not in st.session_state:
        st.session_state[selected_key] = resolved_default

    selected_path = st.session_state[selected_key]

    st.caption("💡 서버에 파일이 저장됩니다. 스크래핑 완료 후 다운로드 버튼으로 로컬 PC에 받을 수 있습니다.")

    # 경로 직접 입력 + 찾아보기 버튼
    col_input, col_btn = st.columns([5, 1])
    with col_input:
        typed_path = st.text_input(
            label,
            value=selected_path,
            placeholder="서버 경로를 직접 입력하거나 찾아보기를 클릭하세요",
            key=f"{key_prefix}_text_input"
        )
        # 사용자가 직접 경로를 입력/수정한 경우 반영
        if typed_path != selected_path:
            st.session_state[selected_key] = typed_path
            if os.path.isdir(typed_path):
                st.session_state[nav_key] = typed_path
            selected_path = typed_path
    with col_btn:
        st.markdown("<div style='height:1.6rem'></div>", unsafe_allow_html=True)
        browse_label = "📂 찾아보기" if not st.session_state[browse_key] else "✕ 닫기"
        if st.button(browse_label, key=f"{key_prefix}_toggle_btn", width="stretch"):
            st.session_state[browse_key] = not st.session_state[browse_key]
            st.rerun()

    # 폴더 브라우저 패널
    if st.session_state[browse_key]:
        current = st.session_state[nav_key]

        st.markdown('<div class="folder-browser-panel">', unsafe_allow_html=True)

        # 현재 경로 표시
        st.markdown(f"""
        <div class="folder-browser-path">
            <span class="material-symbols-outlined">location_on</span>
            {current}
        </div>
        """, unsafe_allow_html=True)

        # 네비게이션 버튼
        nav_c1, nav_c2, nav_c3, nav_c4 = st.columns([1, 1, 1, 2])
        with nav_c1:
            if st.button("⬆️ 상위 폴더", key=f"{key_prefix}_up", width="stretch"):
                parent = os.path.dirname(current)
                if parent != current:
                    st.session_state[nav_key] = parent
                    st.rerun()
        with nav_c2:
            if st.button("📥 Downloads", key=f"{key_prefix}_home", width="stretch"):
                st.session_state[nav_key] = _get_default_download_path()
                st.rerun()
        with nav_c3:
            if st.button("🏠 홈", key=f"{key_prefix}_root", width="stretch"):
                st.session_state[nav_key] = os.path.expanduser("~")
                st.rerun()
        with nav_c4:
            if st.button("✅ 이 폴더 선택", key=f"{key_prefix}_select", type="primary", width="stretch"):
                st.session_state[selected_key] = current
                st.session_state[browse_key] = False
                st.rerun()

        # 하위 폴더 목록
        try:
            entries = sorted(os.listdir(current))
            dirs = [e for e in entries if os.path.isdir(os.path.join(current, e)) and not e.startswith('.')]

            if dirs:
                cols_per_row = 4
                for i in range(0, len(dirs), cols_per_row):
                    cols = st.columns(cols_per_row)
                    for j, d in enumerate(dirs[i:i + cols_per_row]):
                        with cols[j]:
                            if st.button(f"📁 {d}", key=f"{key_prefix}_d_{i + j}", width="stretch"):
                                st.session_state[nav_key] = os.path.join(current, d)
                                st.rerun()
            else:
                st.caption("📭 하위 폴더가 없습니다.")
        except PermissionError:
            st.error("🔒 접근 권한이 없습니다. 다른 폴더를 선택하세요.")
        except OSError as e:
            st.error(f"폴더를 읽을 수 없습니다: {e}")

        # 새 폴더 만들기
        new_c1, new_c2 = st.columns([3, 1])
        with new_c1:
            new_folder_name = st.text_input(
                "새 폴더 이름",
                placeholder="새 폴더명을 입력하세요",
                key=f"{key_prefix}_new_name",
                label_visibility="collapsed"
            )
        with new_c2:
            if st.button("📁+ 새 폴더", key=f"{key_prefix}_mkdir", width="stretch"):
                if new_folder_name:
                    new_path = os.path.join(current, new_folder_name)
                    try:
                        os.makedirs(new_path, exist_ok=True)
                        st.session_state[nav_key] = new_path
                        st.rerun()
                    except OSError as e:
                        st.error(f"폴더 생성 실패: {e}")
                else:
                    st.warning("폴더 이름을 입력하세요.")

        # 기본 경로로 초기화 버튼
        if selected_path != resolved_default:
            if st.button("🔄 기본 경로로 초기화 (Downloads)", key=f"{key_prefix}_clear", width="stretch"):
                st.session_state[selected_key] = resolved_default
                st.session_state[nav_key] = resolved_default
                st.session_state[browse_key] = False
                st.rerun()

        st.markdown('</div>', unsafe_allow_html=True)

    return st.session_state[selected_key]


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


def _auto_download_file(file_path, download_name):
    """브라우저 자동 다운로드를 JavaScript로 트리거"""
    if not file_path or not os.path.exists(file_path):
        return
    with open(file_path, 'rb') as f:
        data = f.read()
    b64 = base64.b64encode(data).decode()
    # MIME 판별
    if download_name.endswith('.xlsx'):
        mime = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    elif download_name.endswith('.zip'):
        mime = 'application/zip'
    else:
        mime = 'application/octet-stream'
    components.html(
        f"""
        <script>
        const link = document.createElement('a');
        link.href = 'data:{mime};base64,{b64}';
        link.download = '{download_name}';
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        </script>
        """,
        height=0
    )


def _sync_shared_to_session():
    """공유 dict의 결과를 st.session_state에 동기화 (메인 스레드에서만 호출)."""
    shared = st.session_state._scraping_shared
    st.session_state.scraping_running = shared.get('scraping_running', False)
    st.session_state.elapsed_time = shared.get('elapsed_time', 0)
    st.session_state.logs = shared.get('logs', [])
    st.session_state.scraping_progress = shared.get('scraping_progress', {})

    # 완료 시에만 최종 결과 동기화
    phase = shared.get('scraping_progress', {}).get('phase', '')
    if phase in ('done', 'error'):
        st.session_state.results = shared.get('results', [])
        st.session_state.bank_dates = shared.get('bank_dates', {})
        if shared.get('zip_path'):
            st.session_state.zip_path = shared['zip_path']
        if shared.get('summary_excel_path'):
            st.session_state.summary_excel_path = shared['summary_excel_path']
        if shared.get('validation_result') is not None:
            st.session_state.validation_result = shared['validation_result']
        if shared.get('ai_table_generated'):
            st.session_state.ai_table_generated = True


@st.fragment(run_every=2)
def _render_scraping_progress():
    """스크래핑 실시간 진행 상태를 표시하는 fragment (2초마다 자동 갱신)"""
    # shared dict에서 직접 읽기 (스레드가 쓰는 데이터)
    shared = st.session_state.get('_scraping_shared', {})
    progress = shared.get('scraping_progress', {})
    phase = progress.get('phase', '')
    current_idx = progress.get('current_idx', 0)
    total = progress.get('total_banks', 1) or 1
    current_bank = progress.get('current_bank', '')
    start_time = progress.get('start_time', 0)
    partial_results = progress.get('partial_results', [])
    logs = shared.get('logs', [])

    elapsed = time.time() - start_time if start_time else 0
    pct = current_idx / total

    # 단계 텍스트
    if phase == 'scraping':
        phase_text = f"처리 중: **{current_bank}** ({current_idx}/{total})"
    elif phase == 'zipping':
        phase_text = "📦 파일 압축 중..."
        pct = 1.0
    elif phase == 'ai_excel':
        phase_text = "🤖 GPT-5.2가 분기총괄 엑셀 생성 중..."
        pct = 1.0
    elif phase == 'done':
        phase_text = "✅ 완료!"
        pct = 1.0
    elif phase == 'error':
        phase_text = "❌ 오류 발생"
        pct = 1.0
    else:
        phase_text = "준비 중..."

    st.progress(pct)
    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown(phase_text)
    with col2:
        st.markdown(f"⏱️ **{format_elapsed_time(elapsed)}**")

    # 실시간 로그 (최근 5줄)
    if logs:
        recent_logs = logs[-5:]
        st.text_area("실시간 로그", value="\n".join(recent_logs), height=120, disabled=True, key="bg_log_area")

    # 실시간 부분 결과 요약
    if partial_results:
        success = sum(1 for r in partial_results if r.get('success'))
        fail = len(partial_results) - success
        st.caption(f"현재까지: 성공 {success}개 / 실패 {fail}개 / 전체 {total}개")

    # 완료 시 shared → session_state 동기화 후 페이지 전체 리로드
    is_running = shared.get('scraping_running', False)
    if not is_running:
        _sync_shared_to_session()
        st.rerun()


@st.fragment(run_every=2)
def _render_disclosure_progress():
    """공시파일 다운로드 실시간 진행 상태를 표시하는 fragment (2초마다 자동 갱신)"""
    shared = st.session_state.get('_disclosure_shared', {})
    progress = shared.get('progress', {})
    phase = progress.get('phase', '')
    current_idx = progress.get('current_idx', 0)
    total = progress.get('total_banks', 1) or 1
    current_bank = progress.get('current_bank', '')
    start_time = progress.get('start_time', 0)
    logs = shared.get('logs', [])

    elapsed = time.time() - start_time if start_time else 0
    pct = min(current_idx / total, 1.0) if total > 0 else 0

    if phase == 'init':
        phase_text = "📥 공시파일 다운로드 초기화 중..."
    elif phase == 'extracting':
        phase_text = "🌐 웹사이트 접속 및 은행 목록 추출 중..."
    elif phase == 'downloading':
        phase_text = f"📥 다운로드 중: **{current_bank}** ({current_idx}/{total})"
    elif phase == 'zipping':
        phase_text = "📦 파일 압축 중..."
        pct = 1.0
    elif phase == 'done':
        phase_text = "✅ 완료!"
        pct = 1.0
    elif phase == 'error':
        phase_text = f"❌ 오류 발생: {progress.get('error_msg', '')}"
        pct = 1.0
    else:
        phase_text = "준비 중..."

    st.progress(pct)
    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown(phase_text)
    with col2:
        st.markdown(f"⏱️ **{format_elapsed_time(elapsed)}**")

    # 실시간 로그 (최근 8줄)
    if logs:
        recent_logs = logs[-8:]
        st.text_area("실시간 로그", value="\n".join(recent_logs), height=150, disabled=True, key="dl_log_area")

    # 실시간 부분 결과 요약
    partial_results = shared.get('results', [])
    if partial_results:
        success = sum(1 for r in partial_results if r.get('상태') in ['완료', '부분완료'])
        fail = sum(1 for r in partial_results if r.get('상태') == '실패')
        st.caption(f"현재까지: 성공 {success}개 / 실패 {fail}개 / 전체 {total}개")

    # 완료 시 session_state 동기화 후 리로드
    is_running = shared.get('running', False)
    if not is_running and phase in ('done', 'error'):
        st.session_state.disclosure_running = False
        st.session_state.disclosure_results = shared.get('results', [])
        st.session_state.disclosure_logs = shared.get('logs', [])
        st.session_state.disclosure_zip_path = shared.get('zip_path')
        st.rerun()


@st.fragment(run_every=3)
def _render_global_scraping_banner():
    """페이지와 관계없이 표시되는 스크래핑 진행 배너"""
    shared = st.session_state.get('_scraping_shared', {})
    is_running = shared.get('scraping_running', False)
    if not is_running:
        if st.session_state.scraping_running:
            _sync_shared_to_session()
            st.rerun()
        return

    progress = shared.get('scraping_progress', {})
    phase = progress.get('phase', '')
    current_idx = progress.get('current_idx', 0)
    total = progress.get('total_banks', 1) or 1
    current_bank = progress.get('current_bank', '')
    start_time = progress.get('start_time', 0)

    elapsed = time.time() - start_time if start_time else 0

    if phase == 'scraping':
        msg = f"🔄 스크래핑 진행 중: **{current_bank}** ({current_idx}/{total}) — ⏱️ {format_elapsed_time(elapsed)}"
    elif phase == 'zipping':
        msg = f"📦 파일 압축 중... — ⏱️ {format_elapsed_time(elapsed)}"
    elif phase == 'ai_excel':
        msg = f"🤖 GPT-5.2 엑셀 생성 중... — ⏱️ {format_elapsed_time(elapsed)}"
    else:
        msg = f"🔄 스크래핑 진행 중... — ⏱️ {format_elapsed_time(elapsed)}"

    st.info(msg)


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
    if '_disclosure_shared' not in st.session_state:
        st.session_state._disclosure_shared = {}
    if '_disclosure_thread' not in st.session_state:
        st.session_state._disclosure_thread = None
    if 'scraping_save_path' not in st.session_state:
        st.session_state.scraping_save_path = ""
    if 'disclosure_save_path' not in st.session_state:
        st.session_state.disclosure_save_path = ""
    # 백그라운드 스크래핑 진행 상태
    if 'scraping_progress' not in st.session_state:
        st.session_state.scraping_progress = {
            'current_bank': '',
            'current_idx': 0,
            'total_banks': 0,
            'phase': '',        # 'scraping', 'zipping', 'ai_excel', 'done', 'error'
            'start_time': 0,
            'partial_results': [],  # 진행 중 실시간 결과
        }
    if '_scraping_thread' not in st.session_state:
        st.session_state._scraping_thread = None
    if '_auto_downloaded' not in st.session_state:
        st.session_state._auto_downloaded = False
    if '_scraping_shared' not in st.session_state:
        st.session_state._scraping_shared = {
            'scraping_running': False,
            'results': [],
            'logs': [],
            'bank_dates': {},
            'elapsed_time': 0,
            'summary_excel_path': None,
            'validation_result': None,
            'ai_table_generated': False,
            'zip_path': None,
            'scraping_progress': {
                'current_bank': '',
                'current_idx': 0,
                'total_banks': 0,
                'phase': '',
                'start_time': 0,
                'partial_results': [],
            },
        }


def main():
    """메인 함수"""
    init_session_state()

    # ========== Sidebar ==========
    # 사이드바 페이지 상태 초기화
    if 'sidebar_page' not in st.session_state:
        st.session_state.sidebar_page = "dashboard"

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

        # 기능이 있는 네비게이션 라디오 버튼
        page = st.radio(
            "Navigation",
            options=["Dashboard", "Crawler Config", "Data Logs", "Reports", "Settings"],
            index=["dashboard", "config", "logs", "reports", "settings"].index(st.session_state.sidebar_page)
                   if st.session_state.sidebar_page in ["dashboard", "config", "logs", "reports", "settings"] else 0,
            key="sidebar_nav_radio",
            label_visibility="collapsed"
        )
        page_map = {
            "Dashboard": "dashboard",
            "Crawler Config": "config",
            "Data Logs": "logs",
            "Reports": "reports",
            "Settings": "settings"
        }
        st.session_state.sidebar_page = page_map.get(page, "dashboard")

        st.divider()

        # 프로필 영역 — 앱 버전 정보 표시
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

    # ========== 페이지별 콘텐츠 라우팅 ==========
    current_page = st.session_state.sidebar_page

    # --- Crawler Config 페이지: 데이터 스크래핑 탭으로 바로 이동 ---
    if current_page == "config":
        st.session_state.sidebar_page = "dashboard"
        current_page = "dashboard"

    # 스크래핑 진행 중이면 Dashboard 외 페이지에서 글로벌 배너 표시
    if st.session_state.scraping_running and current_page != "dashboard":
        _render_global_scraping_banner()

    # --- Data Logs 페이지 ---
    if current_page == "logs":
        st.markdown("""
        <div class="dashboard-header">
            <h2>Data Logs</h2>
            <p>View all crawling and system logs.</p>
        </div>
        """, unsafe_allow_html=True)
        st.markdown("<div style='height:0.5rem'></div>", unsafe_allow_html=True)

        if st.session_state.logs:
            log_text = "\n".join(st.session_state.logs)
            st.text_area("스크래핑 실행 로그", value=log_text, height=400, disabled=True)
            if st.button("🗑️ 로그 지우기", key="clear_logs_page"):
                st.session_state.logs = []
                st.rerun()
        else:
            st.info("📋 아직 로그가 없습니다. 스크래핑을 실행하면 여기에 로그가 표시됩니다.")

        if st.session_state.disclosure_logs:
            st.divider()
            log_text_dl = "\n".join(st.session_state.disclosure_logs)
            st.text_area("공시파일 다운로드 로그", value=log_text_dl, height=300, disabled=True)
        return

    # --- Reports 페이지 ---
    if current_page == "reports":
        st.markdown("""
        <div class="dashboard-header">
            <h2>Reports</h2>
            <p>View and download generated reports and Excel files.</p>
        </div>
        """, unsafe_allow_html=True)
        st.markdown("<div style='height:0.5rem'></div>", unsafe_allow_html=True)

        if st.session_state.results:
            results = st.session_state.results
            success_count = sum(1 for r in results if r['success'])
            fail_count = len(results) - success_count

            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("전체", f"{len(results)}개")
            with col2:
                st.metric("성공", f"{success_count}개")
            with col3:
                st.metric("실패", f"{fail_count}개")

            df = create_summary_dataframe(results, st.session_state.bank_dates)
            st.dataframe(df, width="stretch", hide_index=True)

            # 엑셀 다운로드
            if st.session_state.summary_excel_path and os.path.exists(st.session_state.summary_excel_path):
                with open(st.session_state.summary_excel_path, 'rb') as f:
                    st.download_button(
                        label="📊 분기총괄 엑셀 다운로드",
                        data=f,
                        file_name=f"저축은행_분기총괄_{datetime.now().strftime('%Y%m%d')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        width="stretch"
                    )
            if 'zip_path' in st.session_state and st.session_state.zip_path and os.path.exists(st.session_state.zip_path):
                with open(st.session_state.zip_path, 'rb') as f:
                    st.download_button(
                        label="📥 전체 데이터 ZIP 다운로드",
                        data=f,
                        file_name=f"저축은행_데이터_{datetime.now().strftime('%Y%m%d')}.zip",
                        mime="application/zip",
                        width="stretch"
                    )
        else:
            st.info("📋 아직 보고서가 없습니다. 스크래핑을 실행하면 여기에 결과가 표시됩니다.")
        return

    # --- Settings 페이지 ---
    if current_page == "settings":
        st.markdown("""
        <div class="dashboard-header">
            <h2>Settings</h2>
            <p>System configuration and API key management.</p>
        </div>
        """, unsafe_allow_html=True)
        st.markdown("<div style='height:0.5rem'></div>", unsafe_allow_html=True)

        # ===== GPT-5.2 API 설정 =====
        st.markdown('<div class="section-title"><span class="material-symbols-outlined" style="font-size:20px;color:#eca413;">smart_toy</span> GPT-5.2 API 설정</div>', unsafe_allow_html=True)

        current_key = st.session_state.openai_api_key

        if EXCEL_GENERATOR_AVAILABLE and OPENAI_AVAILABLE:
            if current_key:
                st.success(f"✅ API Key가 설정되어 있습니다. (마지막 4자리: ...{current_key[-4:]})")
                st.caption("스크래핑 시 GPT-5.2가 데이터를 분석하여 요약 엑셀을 자동 생성합니다.")
            else:
                st.warning("⚠️ API Key가 설정되지 않았습니다.")
                st.markdown("""
                **설정 방법 (택 1):**
                1. `.streamlit/secrets.toml` 파일에 `OPENAI_API_KEY = "sk-..."` 입력
                2. 환경변수 `OPENAI_API_KEY` 설정
                """)
        else:
            st.error("⚠️ GPT-5.2 기능을 사용하려면 openai 패키지가 필요합니다: `pip install openai>=2.0.0`")

        st.divider()

        # ===== 파일 저장 경로 설정 =====
        st.markdown('<div class="section-title"><span class="material-symbols-outlined" style="font-size:20px;color:#eca413;">folder</span> 서버 저장 경로 설정</div>', unsafe_allow_html=True)

        scraping_save_path = folder_picker("scraping_path", label="📂 스크래핑 파일 저장 경로")
        st.session_state.scraping_save_path = scraping_save_path

        disclosure_save_path_settings = folder_picker("disclosure_path", label="📂 공시파일 저장 경로")
        st.session_state.disclosure_save_path = disclosure_save_path_settings

        st.divider()

        # ===== 앱 정보 =====
        st.markdown('<div class="section-title"><span class="material-symbols-outlined" style="font-size:20px;color:#eca413;">info</span> 앱 정보</div>', unsafe_allow_html=True)
        st.markdown("""
        **저축은행 공시자료 크롤링 시스템 v4.1**
        - 79개 저축은행 분기공시/결산공시 데이터 자동 수집
        - GPT-5.2 API를 활용한 AI 표 정리 및 엑셀 자동 생성
        - 통일경영공시/감사보고서 파일 일괄 다운로드

        **사용 방법:**
        1. Dashboard → 스크래핑 유형 선택 (분기공시/결산공시)
        2. 은행 선택 (전체 또는 개별)
        3. '스크래핑 시작' 클릭
        4. 완료 후 결과 파일 다운로드
        5. (선택) AI 표 정리 버튼으로 데이터 분석 엑셀 생성
        6. (선택) 공시파일 일괄 다운로드로 원본 파일 수집

        **데이터 출처:** 저축은행중앙회 통일경영공시 (https://www.fsb.or.kr)
        """)
        return

    # ========== Dashboard 페이지 (기본) ==========
    st.markdown("""
    <div class="dashboard-header">
        <h2>Dashboard Overview</h2>
        <p>Real-time monitoring of savings bank public disclosure data.</p>
    </div>
    """, unsafe_allow_html=True)
    st.markdown("<div style='height:0.5rem'></div>", unsafe_allow_html=True)

    # ========== Stat Cards ==========
    stat_col1, stat_col2, stat_col3 = st.columns(3)

    # Calculate live stats (진행 중이면 shared dict의 partial_results 참조)
    shared = st.session_state.get('_scraping_shared', {})
    is_scraping = st.session_state.scraping_running
    is_disclosure = st.session_state.get('disclosure_running', False)
    selected_count = len(st.session_state.selected_banks)
    live_results = shared.get('scraping_progress', {}).get('partial_results', []) if is_scraping else st.session_state.results
    data_collected = sum(1 for r in live_results if r.get('success', False)) if live_results else 0
    total_records = len(live_results) if live_results else 0

    with stat_col1:
        if is_scraping:
            crawl_badge = f'<span class="stat-card-badge badge-green">진행 중</span>'
            crawl_value = f"{shared.get('scraping_progress', {}).get('current_idx', 0)} <span>/ {selected_count}</span>"
        elif is_disclosure:
            dl_progress = st.session_state.get('_disclosure_shared', {}).get('progress', {})
            dl_current = dl_progress.get('current_idx', 0)
            dl_total = dl_progress.get('total_banks', 0)
            crawl_badge = f'<span class="stat-card-badge badge-green">다운로드 중</span>'
            crawl_value = f"{dl_current} <span>/ {dl_total}</span>" if dl_total > 0 else "준비 중"
        else:
            crawl_badge = f'<span class="stat-card-badge badge-amber">대기</span>'
            crawl_value = f"{selected_count} <span>선택됨</span>"
        st.markdown(f"""
        <div class="stat-card">
            <div style="display:flex; justify-content:space-between; align-items:flex-start; position:relative; z-index:1;">
                <div class="stat-card-icon">
                    <span class="material-symbols-outlined">monitoring</span>
                </div>
                {crawl_badge}
            </div>
            <div style="margin-top:1rem; position:relative; z-index:1;">
                <p class="stat-card-label">진행 현황</p>
                <p class="stat-card-value">{crawl_value}</p>
            </div>
        </div>
        """, unsafe_allow_html=True)

    with stat_col2:
        if data_collected > 0 or total_records > 0:
            display_data = f"{data_collected}"
            today_count = f"{data_collected}/{total_records} 완료"
            data_badge_class = "badge-green"
        else:
            display_data = "0"
            today_count = "수집 대기"
            data_badge_class = "badge-amber"
        st.markdown(f"""
        <div class="stat-card">
            <div style="display:flex; justify-content:space-between; align-items:flex-start; position:relative; z-index:1;">
                <div class="stat-card-icon">
                    <span class="material-symbols-outlined">database</span>
                </div>
                <span class="stat-card-badge {data_badge_class}">{today_count}</span>
            </div>
            <div style="margin-top:1rem; position:relative; z-index:1;">
                <p class="stat-card-label">수집 데이터</p>
                <p class="stat-card-value">{display_data} <span>건</span></p>
            </div>
        </div>
        """, unsafe_allow_html=True)

    with stat_col3:
        if is_scraping or is_disclosure:
            health_badge = '<span class="stat-card-badge badge-green">실행 중</span>'
            health_icon = "play_circle"
            health_label = "실행 중"
        elif data_collected > 0 and total_records > 0:
            success_rate = round(data_collected / total_records * 100, 1)
            health_badge = f'<span class="stat-card-badge badge-green">완료</span>'
            health_icon = "check_circle"
            health_label = f"성공률 {success_rate}%"
        else:
            health_badge = '<span class="stat-card-badge badge-amber">대기</span>'
            health_icon = "hourglass_empty"
            health_label = "대기 중"
        st.markdown(f"""
        <div class="stat-card">
            <div style="display:flex; justify-content:space-between; align-items:flex-start; position:relative; z-index:1;">
                <div class="stat-card-icon">
                    <span class="material-symbols-outlined">{health_icon}</span>
                </div>
                {health_badge}
            </div>
            <div style="margin-top:1rem; position:relative; z-index:1;">
                <p class="stat-card-label">시스템 상태</p>
                <p class="stat-card-value">{health_label}</p>
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

        scraping_save_path = st.session_state.scraping_save_path

        col3, col4 = st.columns([1, 1])
        with col3:
            auto_zip = st.checkbox("🗜️ 완료 후 자동 압축", value=True)
        with col4:
            save_md = st.checkbox("📝 MD 파일도 함께 생성", value=False)

        st.divider()

        # GPT 사용 여부는 Settings에서 설정된 API Key 기반으로 자동 판단
        api_key = st.session_state.openai_api_key
        use_chatgpt = bool(api_key) and EXCEL_GENERATOR_AVAILABLE and OPENAI_AVAILABLE

        # ========== 은행 선택 섹션 ==========
        st.markdown('<div class="section-title"><span class="material-symbols-outlined" style="font-size:20px;color:#eca413;">account_balance</span> 은행 선택</div>', unsafe_allow_html=True)

        # 전체 선택/해제 버튼 (중앙 정렬)
        col1, col2, col3, col4, col5 = st.columns([1, 1, 1, 1, 1])
        with col2:
            if st.button("✅ 전체 선택", width="stretch", type="primary"):
                for bank in all_banks:
                    st.session_state[f"bank_{bank}"] = True
                st.session_state.selected_banks = all_banks.copy()
                st.rerun()
        with col3:
            st.metric("선택된 은행", f"{len(st.session_state.selected_banks)}개 / 79개")
        with col4:
            if st.button("❌ 전체 해제", width="stretch"):
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
            if st.button("🚀 스크래핑 시작", type="primary", width="stretch", disabled=start_disabled):
                if not selected_banks:
                    st.error("스크래핑할 은행을 선택하세요.")
                else:
                    st.session_state.ai_table_generated = False
                    st.session_state._auto_downloaded = False
                    start_scraping(
                        selected_banks,
                        scrape_type,
                        auto_zip,
                        download_filename,
                        use_chatgpt=use_chatgpt,
                        api_key=api_key,
                        save_path=scraping_save_path
                    )
                    st.rerun()

        # ========== 실시간 진행 상태 ==========
        if st.session_state.scraping_running:
            _render_scraping_progress()

        st.divider()

        # ========== 결과 섹션 ==========
        st.markdown('<div class="section-title"><span class="material-symbols-outlined" style="font-size:20px;color:#eca413;">analytics</span> 스크래핑 결과 <span class="live-badge">Live</span></div>', unsafe_allow_html=True)

        if st.session_state.results:
            results = st.session_state.results
            success_count = sum(1 for r in results if r['success'])
            fail_count = len(results) - success_count

            # 자동 다운로드: 스크래핑 완료 후 최초 1회
            if not st.session_state._auto_downloaded and not st.session_state.scraping_running:
                st.session_state._auto_downloaded = True
                # ZIP 파일 자동 다운로드
                if 'zip_path' in st.session_state and st.session_state.zip_path and os.path.exists(st.session_state.zip_path):
                    _auto_download_file(
                        st.session_state.zip_path,
                        f"{download_filename}.zip"
                    )
                    st.toast("ZIP 파일이 자동으로 다운로드됩니다.", icon="📥")
                # AI 엑셀 자동 다운로드
                if st.session_state.summary_excel_path and os.path.exists(st.session_state.summary_excel_path):
                    _auto_download_file(
                        st.session_state.summary_excel_path,
                        f"저축은행_분기총괄_{datetime.now().strftime('%Y%m%d')}.xlsx"
                    )
                    st.toast("분기총괄 엑셀이 자동으로 다운로드됩니다.", icon="📊")

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
            st.dataframe(df, width="stretch", hide_index=True)

            st.write("")

            # ========== AI 표 정리 및 엑셀 반환 옵션 ==========
            st.markdown("#### 🤖 GPT-5.2 AI 표 정리 및 엑셀 반환")

            if EXCEL_GENERATOR_AVAILABLE and OPENAI_AVAILABLE and st.session_state.openai_api_key:
                if st.session_state.summary_excel_path and os.path.exists(st.session_state.summary_excel_path):
                    try:
                        preview_df = pd.read_excel(st.session_state.summary_excel_path, sheet_name='분기총괄')
                        st.markdown("**AI 분석 결과 미리보기:**")
                        st.dataframe(preview_df, width="stretch", hide_index=True)
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
                                width="stretch",
                                type="secondary"
                            )
                else:
                    st.info("💡 GPT-5.2를 활용하여 스크래핑 데이터를 표로 정리하고 엑셀로 반환할 수 있습니다.")
                    col1, col2, col3 = st.columns([1, 2, 1])
                    with col2:
                        if st.button("🤖 AI로 표 정리 및 엑셀 생성", width="stretch", type="secondary"):
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
                    st.info("💡 Settings 페이지에서 API Key를 설정하면 AI 표 정리 기능을 사용할 수 있습니다.")
                elif not EXCEL_GENERATOR_AVAILABLE or not OPENAI_AVAILABLE:
                    st.info("💡 `pip install openai>=2.0.0` 설치 후 AI 표 정리 기능을 사용할 수 있습니다.")

            st.write("")

            # ZIP 파일 다운로드
            if 'zip_path' in st.session_state and st.session_state.zip_path:
                st.markdown("#### 📦 전체 데이터 압축 파일")
                st.caption("아래 버튼을 클릭하면 브라우저 다운로드를 통해 로컬 PC에 저장됩니다.")
                col1, col2, col3 = st.columns([1, 2, 1])
                with col2:
                    with open(st.session_state.zip_path, 'rb') as f:
                        st.download_button(
                            label="📥 내 PC로 다운로드 (ZIP)",
                            data=f,
                            file_name=f"{download_filename}.zip",
                            mime="application/zip",
                            width="stretch",
                            type="primary"
                        )
        else:
            st.info("아직 스크래핑 결과가 없습니다. 은행을 선택하고 스크래핑을 실행하세요.")

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

            disclosure_save_path = st.session_state.disclosure_save_path

            st.markdown("<div style='height:0.5rem'></div>", unsafe_allow_html=True)

            col1, col2, col3 = st.columns([1, 2, 1])
            with col2:
                disclosure_disabled = st.session_state.disclosure_running
                if st.button(
                    "📥 공시파일 일괄 다운로드 시작",
                    type="primary",
                    width="stretch",
                    disabled=disclosure_disabled,
                    key="btn_disclosure_download"
                ):
                    _start_disclosure_download(disclosure_save_path)

            if st.session_state.disclosure_running:
                _render_disclosure_progress()

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
                    width="stretch",
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
                                width="stretch",
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

    # (System Logs와 API Usage는 각각 Data Logs, Settings 페이지로 이동됨)

    # 하단 여백
    st.markdown("<div style='height:2rem'></div>", unsafe_allow_html=True)


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
                st.dataframe(pd.DataFrame(detail_rows), width="stretch", hide_index=True)

    # 검증 시트 안내
    st.caption("💡 엑셀 파일의 '정합성검증' 시트에서 전체 검증 결과를 확인할 수 있습니다.")


def _scraping_worker(shared, selected_banks, scrape_type, auto_zip, download_filename, use_chatgpt=False, api_key=None, save_path=None):
    """백그라운드 스레드에서 실행되는 스크래핑 워커.

    shared: 일반 Python dict (스레드 안전한 공유 상태).
            st.session_state 프록시가 아닌 plain dict이므로 ScriptRunContext 불필요.
    """
    progress = shared['scraping_progress']
    start_time = time.time()
    progress['start_time'] = start_time
    progress['phase'] = 'scraping'
    progress['partial_results'] = []

    try:
        config = Config(scrape_type, output_dir=save_path if save_path else None)
        logger = StreamlitLogger()
        scraper = BankScraper(config, logger)

        total_banks = len(selected_banks)
        progress['total_banks'] = total_banks
        results = []
        bank_dates = {}

        for idx, bank in enumerate(selected_banks):
            progress['current_bank'] = bank
            progress['current_idx'] = idx + 1

            elapsed = time.time() - start_time
            shared['elapsed_time'] = elapsed

            logger.log_message(f"[시작] {bank} 스크래핑")

            filepath, success, date_info = scraper.scrape_bank(bank)
            result = {
                'bank': bank,
                'success': success,
                'filepath': filepath,
                'date_info': date_info
            }
            results.append(result)
            bank_dates[bank] = date_info

            progress['partial_results'] = list(results)

            status = "완료" if success else "실패"
            logger.log_message(f"[{status}] {bank} - 공시일: {date_info}")
            shared['logs'] = logger.messages.copy()

            time.sleep(0.5)

        # 최종 경과 시간
        final_elapsed = time.time() - start_time
        shared['elapsed_time'] = final_elapsed
        shared['results'] = results
        shared['bank_dates'] = bank_dates

        # ZIP 압축
        if auto_zip:
            progress['phase'] = 'zipping'
            logger.log_message("ZIP 파일 압축 중...")
            zip_path = scraper.create_zip_archive(results, download_filename)
            if zip_path:
                shared['zip_path'] = zip_path
                logger.log_message("ZIP 파일 생성 완료")

        # GPT-5.2 엑셀 생성
        if use_chatgpt and api_key and EXCEL_GENERATOR_AVAILABLE:
            progress['phase'] = 'ai_excel'
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
                    shared['summary_excel_path'] = summary_excel_path
                    shared['validation_result'] = validation
                    shared['ai_table_generated'] = True
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

        # 완료
        shared['logs'] = logger.messages.copy()
        progress['phase'] = 'done'

    except Exception as e:
        shared['logs'].append(f"[오류] {str(e)}")
        progress['phase'] = 'error'

    finally:
        shared['scraping_running'] = False


def start_scraping(selected_banks, scrape_type, auto_zip, download_filename, use_chatgpt=False, api_key=None, save_path=None):
    """스크래핑을 백그라운드 스레드로 시작"""
    st.session_state.scraping_running = True
    st.session_state.results = []
    st.session_state.logs = []
    st.session_state.bank_dates = {}
    st.session_state.summary_excel_path = None
    st.session_state.validation_result = None
    st.session_state.elapsed_time = 0

    # 스레드와 공유할 plain dict 생성
    shared = {
        'scraping_running': True,
        'results': [],
        'logs': [],
        'bank_dates': {},
        'elapsed_time': 0,
        'summary_excel_path': None,
        'validation_result': None,
        'ai_table_generated': False,
        'zip_path': None,
        'scraping_progress': {
            'current_bank': '',
            'current_idx': 0,
            'total_banks': len(selected_banks),
            'phase': 'scraping',
            'start_time': time.time(),
            'partial_results': [],
        },
    }
    st.session_state._scraping_shared = shared
    st.session_state.scraping_progress = shared['scraping_progress']

    thread = threading.Thread(
        target=_scraping_worker,
        args=(shared, selected_banks, scrape_type, auto_zip, download_filename),
        kwargs={'use_chatgpt': use_chatgpt, 'api_key': api_key, 'save_path': save_path},
        daemon=True
    )
    st.session_state._scraping_thread = thread
    thread.start()


def _start_disclosure_download(save_path=None):
    """공시파일 다운로드를 백그라운드 스레드로 시작"""
    shared = {
        'running': True,
        'progress': {
            'phase': 'init',
            'current_idx': 0,
            'total_banks': 0,
            'current_bank': '',
            'start_time': time.time(),
            'error_msg': '',
        },
        'logs': [],
        'results': [],
        'zip_path': None,
    }
    st.session_state._disclosure_shared = shared
    st.session_state.disclosure_running = True
    st.session_state.disclosure_results = []
    st.session_state.disclosure_logs = []
    st.session_state.disclosure_zip_path = None

    thread = threading.Thread(
        target=_disclosure_worker,
        args=(shared, save_path),
        daemon=True
    )
    st.session_state._disclosure_thread = thread
    thread.start()


def _disclosure_worker(shared, save_path=None):
    """백그라운드 스레드에서 실행되는 공시파일 다운로드 워커"""
    progress = shared['progress']

    try:
        if save_path:
            download_path = os.path.abspath(save_path)
            os.makedirs(download_path, exist_ok=True)
        else:
            download_path = tempfile.mkdtemp(prefix="저축은행_공시파일_")

        def log_callback(msg):
            shared['logs'].append(msg)

        progress['phase'] = 'init'
        log_callback("공시파일 다운로드 초기화 중...")

        downloader = DisclosureDownloader(
            download_path=download_path,
            log_callback=log_callback,
            headless=True
        )

        # 은행 목록 추출
        progress['phase'] = 'extracting'
        log_callback("웹사이트 접속 및 은행 목록 추출 중...")
        bank_list = downloader.start_and_extract_banks()

        if not bank_list:
            progress['phase'] = 'error'
            progress['error_msg'] = '은행 목록을 추출할 수 없습니다.'
            log_callback("오류: 은행 목록 추출 실패")
            shared['running'] = False
            return

        total = len(bank_list)
        progress['total_banks'] = total
        progress['phase'] = 'downloading'
        log_callback(f"{total}개 은행 공시파일 다운로드 시작")

        # 다운로드 실행
        def progress_callback(current, total_count, bank_name):
            progress['current_idx'] = current + 1
            progress['current_bank'] = bank_name
            shared['results'] = list(downloader.results) if hasattr(downloader, 'results') else []
            log_callback(f"[{current + 1}/{total_count}] {bank_name} 처리 중")

        total_downloaded = downloader.download_all(bank_list, progress_callback)

        # 보고서 생성
        downloader.create_report()

        # 다운로드된 파일 ZIP 압축
        progress['phase'] = 'zipping'
        log_callback("파일 압축 중...")

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
            shared['zip_path'] = zip_path

        # 결과 저장
        shared['results'] = list(downloader.results) if hasattr(downloader, 'results') else []
        success = len([r for r in shared['results'] if r.get('상태') in ['완료', '부분완료']])
        log_callback(f"완료! 성공: {success}/{total}, 총 {total_downloaded}개 파일 다운로드")

        downloader.cleanup()

        # 완료 표시
        progress['phase'] = 'done'

    except Exception as e:
        progress['phase'] = 'error'
        progress['error_msg'] = str(e)
        shared['logs'].append(f"오류 발생: {str(e)}")

    finally:
        shared['running'] = False


if __name__ == "__main__":
    main()
