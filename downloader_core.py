"""
저축은행 통일경영공시 및 감사보고서 파일 다운로드 핵심 모듈
- Selenium 기반 파일 다운로드 엔진
- Streamlit / Tkinter 양쪽에서 사용 가능
- 브라우저 재시작 없는 안정적 다운로드
버전: 3.0
"""

import os
import sys
import time
import shutil
import re
import json
import gc
import traceback
import tempfile
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any, Optional, Callable

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    StaleElementReferenceException,
    WebDriverException,
    InvalidSessionIdException
)

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False


# ============================================================
# 설정 상수
# ============================================================
TARGET_URL = "https://www.fsb.or.kr/busmagepbnf_0100.act"
MAX_RETRY_ATTEMPTS = 3
DOWNLOAD_TIMEOUT = 45
REFRESH_INTERVAL = 15
TABLE_SELECTOR = "table tbody tr"
MEMORY_THRESHOLD = 80


# ============================================================
# 유틸리티 함수
# ============================================================
def clean_filename(filename: str) -> str:
    """파일명에서 사용할 수 없는 문자 제거"""
    return re.sub(r'[<>:"/\\|?*]', '_', filename)


def get_memory_usage() -> float:
    """현재 시스템 메모리 사용률 반환"""
    if PSUTIL_AVAILABLE:
        return psutil.virtual_memory().percent
    return 0.0


def check_system_health() -> bool:
    """시스템 상태 체크 (메모리, CPU)"""
    if not PSUTIL_AVAILABLE:
        return True
    memory_percent = get_memory_usage()
    cpu_percent = psutil.cpu_percent(interval=0.1)
    if memory_percent > MEMORY_THRESHOLD:
        gc.collect()
        time.sleep(1)
        return False
    return True


# ============================================================
# 메인 다운로더 클래스
# ============================================================
class DisclosureDownloader:
    """
    저축은행 통일경영공시 및 감사보고서 파일 다운로더

    사용법:
        downloader = DisclosureDownloader(
            download_path="/path/to/downloads",
            log_callback=print,
            headless=True
        )
        bank_list = downloader.start_and_extract_banks()
        downloader.download_all(bank_list, progress_callback=my_progress_fn)
        report_path = downloader.create_report()
        downloader.cleanup()
    """

    def __init__(
        self,
        download_path: str,
        log_callback: Optional[Callable[[str], None]] = None,
        headless: bool = True,
        driver_path: Optional[str] = None
    ):
        """
        Args:
            download_path: 파일 다운로드 경로
            log_callback: 로그 메시지를 받을 콜백 함수
            headless: 헤드리스 모드 여부 (Streamlit=True, 로컬=False 가능)
            driver_path: ChromeDriver 수동 경로 (None이면 자동 감지)
        """
        self.download_path = os.path.abspath(download_path)
        os.makedirs(self.download_path, exist_ok=True)
        self.log_callback = log_callback or (lambda msg: print(msg))
        self.headless = headless
        self.driver_path = driver_path
        self.driver = None
        self.results: List[Dict[str, Any]] = []
        self.is_running = True
        self.progress_file = os.path.join(self.download_path, "progress.json")

    def log(self, message: str, level: int = 0):
        """로그 메시지 출력"""
        indent = "  " * level
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_callback(f"[{timestamp}] {indent}{message}")

    # ----------------------------------------------------------
    # WebDriver 관리
    # ----------------------------------------------------------
    def create_driver(self) -> webdriver.Chrome:
        """Chrome WebDriver 생성 (다운로드 설정 포함)"""
        self.log("Chrome WebDriver 초기화 중...")

        chrome_options = Options()

        # 기본 설정
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument('--ignore-certificate-errors')
        chrome_options.add_argument('--disable-web-security')

        if self.headless:
            chrome_options.add_argument('--headless=new')

        # 메모리 최적화 설정
        chrome_options.add_argument('--memory-pressure-off')
        chrome_options.add_argument('--max_old_space_size=512')
        chrome_options.add_argument('--disable-features=TranslateUI')
        chrome_options.add_argument('--disable-background-timer-throttling')
        chrome_options.add_argument('--disable-plugins')
        chrome_options.add_argument('--disable-images')
        chrome_options.add_argument('--disable-default-apps')
        chrome_options.add_argument('--disable-extensions')
        chrome_options.add_argument('--disable-background-networking')
        chrome_options.add_argument('--disable-sync')
        chrome_options.add_argument('--disable-translate')
        chrome_options.add_argument('--disable-application-cache')
        chrome_options.add_argument('--disk-cache-size=0')

        chrome_options.add_argument(
            '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
            'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)

        # 다운로드 설정
        prefs = {
            "download.default_directory": self.download_path,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": False,
            "safebrowsing_for_trusted_sources_enabled": False,
            "plugins.always_open_pdf_externally": True,
            "profile.default_content_settings.popups": 0,
            "profile.default_content_setting_values.automatic_downloads": 1,
            "profile.content_settings.exceptions.automatic_downloads.*.setting": 1
        }
        chrome_options.add_experimental_option("prefs", prefs)
        chrome_options.page_load_strategy = 'normal'

        # Streamlit Cloud 환경 지원
        chromium_paths = [
            '/usr/bin/chromium', '/usr/bin/chromium-browser',
            '/usr/bin/google-chrome', '/usr/bin/google-chrome-stable'
        ]
        for path in chromium_paths:
            if os.path.exists(path):
                chrome_options.binary_location = path
                break

        # 드라이버 서비스 설정
        service = None

        if self.driver_path and os.path.exists(self.driver_path):
            self.log(f"수동 드라이버 경로: {self.driver_path}")
            service = Service(executable_path=self.driver_path)
        else:
            # 시스템 chromedriver 경로 탐색
            chromedriver_paths = [
                '/usr/bin/chromedriver',
                '/usr/lib/chromium/chromedriver',
                '/usr/lib/chromium-browser/chromedriver'
            ]
            for path in chromedriver_paths:
                if os.path.exists(path):
                    service = Service(executable_path=path)
                    break

            # webdriver-manager 폴백
            if service is None:
                try:
                    from webdriver_manager.chrome import ChromeDriverManager
                    os.environ['WDM_SSL_VERIFY'] = '0'
                    service = Service(ChromeDriverManager().install())
                except Exception:
                    pass

        if service:
            driver = webdriver.Chrome(service=service, options=chrome_options)
        else:
            driver = webdriver.Chrome(options=chrome_options)

        driver.set_page_load_timeout(60)
        driver.implicitly_wait(10)

        # Chrome DevTools Protocol 다운로드 설정
        driver.execute_cdp_cmd("Page.setDownloadBehavior", {
            "behavior": "allow",
            "downloadPath": self.download_path
        })

        self.log(f"WebDriver 초기화 성공! (메모리: {get_memory_usage():.1f}%)")
        return driver

    def verify_driver_alive(self) -> bool:
        """드라이버 세션 생존 확인"""
        if not self.driver:
            return False
        try:
            self.driver.title
            return True
        except (InvalidSessionIdException, WebDriverException):
            return False
        except Exception:
            return False

    def _recover_driver(self):
        """드라이버 세션 복구"""
        self.log("드라이버 세션 복구 중...", 1)
        if self.driver:
            try:
                self.driver.quit()
            except Exception:
                pass

        self.driver = self.create_driver()
        self.driver.get(TARGET_URL)
        WebDriverWait(self.driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, TABLE_SELECTOR))
        )
        time.sleep(1)

    # ----------------------------------------------------------
    # 페이지 및 데이터 추출
    # ----------------------------------------------------------
    def refresh_page(self) -> bool:
        """안전한 페이지 새로고침"""
        try:
            self.log("페이지 새로고침 중...", 1)
            self.driver.refresh()
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, TABLE_SELECTOR))
            )
            time.sleep(1)
            self.log("페이지 새로고침 완료", 1)
            return True
        except Exception as e:
            self.log(f"페이지 새로고침 실패: {str(e)}", 1)
            return False

    def extract_bank_list(self) -> List[Dict[str, Any]]:
        """페이지에서 은행 목록과 다운로드 링크 정보 추출 (JavaScript 활용)"""
        try:
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, TABLE_SELECTOR))
            )

            script = """
            const rows = document.querySelectorAll('table tbody tr');
            const headers = Array.from(document.querySelectorAll('table thead th'))
                .map(h => h.textContent.trim());
            const unifyIdx = headers.indexOf('통일경영공시파일');
            const auditIdx = headers.indexOf('감사(검토)보고서');

            const bankData = [];
            rows.forEach((row, idx) => {
                const cells = row.querySelectorAll('td');
                if (cells.length === 0) return;

                const bankName = cells[0].textContent.trim();
                if (!bankName) return;

                const unifyHasLink = cells[unifyIdx]
                    ? cells[unifyIdx].querySelector('a') !== null : false;
                const auditHasLink = cells[auditIdx]
                    ? cells[auditIdx].querySelector('a') !== null : false;

                bankData.push({
                    index: idx,
                    name: bankName,
                    unify_cell_idx: unifyHasLink ? unifyIdx : -1,
                    audit_cell_idx: auditHasLink ? auditIdx : -1
                });
            });

            return bankData;
            """

            bank_list = self.driver.execute_script(script)
            self.log(f"총 {len(bank_list)}개 은행 데이터 추출 완료")
            return bank_list

        except Exception as e:
            self.log(f"은행 데이터 추출 오류: {str(e)}")
            return []

    # ----------------------------------------------------------
    # 다운로드 핵심 로직
    # ----------------------------------------------------------
    def wait_for_downloads(self, timeout: int = DOWNLOAD_TIMEOUT) -> bool:
        """다운로드 완료 대기"""
        start_time = time.time()

        # 다운로드 시작 감지
        download_started = False
        while not download_started and (time.time() - start_time) < 5:
            temp_files = [
                f for f in os.listdir(self.download_path)
                if f.endswith(('.crdownload', '.tmp', '.download', '.part'))
            ]
            if temp_files:
                download_started = True
            else:
                time.sleep(0.2)

        # 다운로드 완료 대기
        while (time.time() - start_time) < timeout:
            downloading = [
                f for f in os.listdir(self.download_path)
                if f.endswith(('.crdownload', '.tmp', '.download', '.part'))
            ]
            if not downloading:
                if download_started:
                    time.sleep(0.5)
                    return True
                else:
                    return False
            time.sleep(0.3)

        # 타임아웃 시 임시 파일 정리
        for f in os.listdir(self.download_path):
            if f.endswith(('.crdownload', '.tmp', '.download', '.part')):
                try:
                    os.remove(os.path.join(self.download_path, f))
                except Exception:
                    pass
        return False

    def download_bank(self, bank_data: Dict[str, Any], max_retries: int = 3) -> int:
        """
        단일 은행 파일 다운로드

        Args:
            bank_data: 은행 데이터 딕셔너리 (index, name, unify_cell_idx, audit_cell_idx)
            max_retries: 최대 재시도 횟수

        Returns:
            다운로드된 파일 수
        """
        bank_name = bank_data["name"]
        self.log(f"[{bank_data['index'] + 1}] 처리 중: {bank_name}")

        downloaded = 0
        result = {
            "은행명": bank_name,
            "통일경영공시": "미다운로드",
            "감사보고서": "미다운로드",
            "상태": "실패",
            "비고": ""
        }

        try:
            link_map = {
                "통일경영공시": bank_data.get("unify_cell_idx", -1),
                "감사보고서": bank_data.get("audit_cell_idx", -1)
            }

            for file_type, cell_idx in link_map.items():
                if cell_idx == -1:
                    self.log(f"  {file_type} 링크 없음", 1)
                    result[file_type] = "링크없음"
                    continue

                for retry in range(max_retries):
                    try:
                        files_before = set(os.listdir(self.download_path))

                        # JavaScript로 안전하게 클릭
                        click_script = f"""
                        const rows = document.querySelectorAll('{TABLE_SELECTOR}');
                        const row = rows[{bank_data['index']}];
                        const cells = row.querySelectorAll('td');
                        const link = cells[{cell_idx}].querySelector('a');
                        if (link) {{
                            link.scrollIntoView({{block: 'center'}});
                            link.click();
                            return true;
                        }}
                        return false;
                        """

                        clicked = self.driver.execute_script(click_script)

                        if not clicked:
                            self.log(f"  클릭 실패 (시도 {retry + 1}/{max_retries})", 2)
                            continue

                        # 새 창 처리
                        if len(self.driver.window_handles) > 1:
                            self.driver.switch_to.window(self.driver.window_handles[-1])
                            self.driver.close()
                            self.driver.switch_to.window(self.driver.window_handles[0])

                        # 다운로드 대기
                        if self.wait_for_downloads():
                            files_after = set(os.listdir(self.download_path))
                            new_files = files_after - files_before
                            actual_files = [
                                f for f in new_files
                                if not f.endswith(('.crdownload', '.tmp', '.download', '.part'))
                            ]

                            if actual_files:
                                downloaded_file = max(
                                    actual_files,
                                    key=lambda f: os.path.getmtime(
                                        os.path.join(self.download_path, f)
                                    )
                                )

                                old_path = os.path.join(self.download_path, downloaded_file)
                                ext = os.path.splitext(downloaded_file)[1] or '.pdf'
                                safe_name = clean_filename(f"{bank_name}_{file_type}")
                                new_filename = f"{safe_name}{ext}"
                                new_path = os.path.join(self.download_path, new_filename)

                                if os.path.exists(new_path):
                                    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                                    new_filename = f"{safe_name}_{ts}{ext}"
                                    new_path = os.path.join(self.download_path, new_filename)

                                if self._rename_file(old_path, new_path):
                                    self.log(f"  {file_type} 다운로드 성공: {new_filename}", 1)
                                    downloaded += 1
                                    result[file_type] = "성공"
                                else:
                                    self.log(f"  {file_type} 다운로드됨 (원본 파일명 유지)", 1)
                                    downloaded += 1
                                    result[file_type] = "성공(원본파일명)"
                                break
                            else:
                                result[file_type] = "파일없음"
                        else:
                            result[file_type] = "타임아웃"

                    except StaleElementReferenceException:
                        self.log(f"  DOM 변경 감지, 재시도... ({retry + 1})", 2)
                        time.sleep(1)
                    except Exception as e:
                        self.log(f"  {file_type} 오류: {str(e)[:50]}", 1)
                        result[file_type] = f"오류: {str(e)[:30]}"

                time.sleep(0.5)

            # 결과 판정
            if downloaded == 2:
                result["상태"] = "완료"
            elif downloaded >= 1:
                result["상태"] = "부분완료"
            else:
                result["상태"] = "실패"

            self.log(f"  → {bank_name}: {result['상태']} ({downloaded}개)", 1)

        except Exception as e:
            self.log(f"{bank_name} 처리 중 치명적 오류: {str(e)}", 1)
            result["비고"] = str(e)[:50]

        self.results.append(result)
        return downloaded

    def _rename_file(self, old_path: str, new_path: str) -> bool:
        """파일명 변경 (재시도 포함)"""
        for attempt in range(5):
            try:
                shutil.move(old_path, new_path)
                return True
            except PermissionError:
                if attempt < 4:
                    time.sleep(1)
                else:
                    try:
                        shutil.copy2(old_path, new_path)
                        try:
                            os.remove(old_path)
                        except Exception:
                            pass
                        return True
                    except Exception:
                        return False
            except Exception:
                return False
        return False

    # ----------------------------------------------------------
    # 배치 다운로드
    # ----------------------------------------------------------
    def start_and_extract_banks(self) -> List[Dict[str, Any]]:
        """
        브라우저 시작, 페이지 접속, 은행 목록 추출

        Returns:
            은행 데이터 리스트
        """
        self.driver = self.create_driver()

        for attempt in range(MAX_RETRY_ATTEMPTS):
            try:
                self.log("웹사이트 접속 중...")
                self.driver.get(TARGET_URL)
                time.sleep(2)

                WebDriverWait(self.driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, TABLE_SELECTOR))
                )

                bank_list = self.extract_bank_list()
                if bank_list:
                    return bank_list

                self.log(f"은행 목록 비어있음, 재시도... ({attempt + 1})")
                self.driver.refresh()
                time.sleep(3)

            except TimeoutException:
                self.log(f"타임아웃 (시도 {attempt + 1}/{MAX_RETRY_ATTEMPTS})")
                if attempt < MAX_RETRY_ATTEMPTS - 1:
                    self.driver.refresh()
                    time.sleep(3)

        return []

    def download_all(
        self,
        bank_list: List[Dict[str, Any]],
        progress_callback: Optional[Callable[[int, int, str], None]] = None
    ) -> int:
        """
        모든 은행 파일 다운로드 (브라우저 재시작 없음)

        Args:
            bank_list: 은행 데이터 리스트
            progress_callback: 진행률 콜백 (current_index, total, bank_name)

        Returns:
            총 다운로드된 파일 수
        """
        total_downloaded = 0
        consecutive_failures = 0

        for i, bank in enumerate(bank_list):
            if not self.is_running:
                self.log("사용자가 다운로드를 중단했습니다.")
                break

            if progress_callback:
                progress_callback(i, len(bank_list), bank.get("name", ""))

            # 시스템 상태 체크
            if not check_system_health():
                self.log(f"시스템 리소스 부족, 메모리 정리 중... ({get_memory_usage():.1f}%)", 1)
                gc.collect()

                # Chrome 메모리 정리
                try:
                    self.driver.execute_script("window.gc();")
                except Exception:
                    pass

                # DOM 정리 (불필요한 요소 제거)
                try:
                    self.driver.execute_script("""
                        document.querySelectorAll('img').forEach(img => img.remove());
                        document.querySelectorAll('script').forEach(script => script.remove());
                        document.querySelectorAll('style').forEach(style => style.remove());
                    """)
                except Exception:
                    pass

                time.sleep(3)

            # 5개마다 메모리 정리
            if i > 0 and i % 5 == 0:
                gc.collect()
                self.log(f"메모리 정리 완료 (현재: {get_memory_usage():.1f}%)", 2)

            # 주기적 페이지 새로고침 (DOM 초기화)
            if i > 0 and i % REFRESH_INTERVAL == 0:
                self.log(f"{REFRESH_INTERVAL}개 은행 처리 후 페이지 새로고침", 1)
                self._refresh_and_reextract(bank_list, i)

            # 연속 실패 시 새로고침
            if consecutive_failures >= 5:
                self.log("연속 5회 실패, 페이지 새로고침", 1)
                self._refresh_and_reextract(bank_list, i)
                consecutive_failures = 0

            # 드라이버 상태 확인
            if not self.verify_driver_alive():
                self.log("드라이버 세션 종료됨, 재생성 중...", 1)
                self._recover_driver()
                self._refresh_and_reextract(bank_list, i)

            # 은행 다운로드
            downloaded = self.download_bank(bank)

            if downloaded == 0:
                consecutive_failures += 1
            else:
                consecutive_failures = 0
                total_downloaded += downloaded

            # 진행 상태 저장
            self._save_progress(i + 1, total_downloaded, bank["name"])

        self.log(f"모든 은행 처리 완료! 총 {total_downloaded}개 파일 다운로드")
        return total_downloaded

    def _refresh_and_reextract(self, bank_list: List[Dict], current_index: int):
        """페이지 새로고침 후 은행 데이터 재매핑"""
        try:
            self.driver.execute_script("window.location.reload();")
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, TABLE_SELECTOR))
            )
            time.sleep(1)

            extracted = self.extract_bank_list()
            name_to_data = {b['name']: b for b in extracted}
            for bank in bank_list[current_index:]:
                if bank['name'] in name_to_data:
                    bank.update(name_to_data[bank['name']])
        except Exception:
            self.log("페이지 새로고침 후 재매핑 실패, 계속 진행", 1)

    # ----------------------------------------------------------
    # 진행 상태 저장/복원
    # ----------------------------------------------------------
    def _save_progress(self, current_index: int, total_downloaded: int, bank_name: str):
        """진행 상태 JSON 저장"""
        try:
            progress = self.load_progress()
            if bank_name not in progress.get("completed", []):
                progress.setdefault("completed", []).append(bank_name)
            progress["current_index"] = current_index
            progress["downloaded_files"] = total_downloaded
            with open(self.progress_file, 'w', encoding='utf-8') as f:
                json.dump(progress, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    def load_progress(self) -> Dict[str, Any]:
        """저장된 진행 상태 불러오기"""
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except (json.JSONDecodeError, Exception):
                pass
        return {"completed": [], "current_index": 0, "downloaded_files": 0, "failed": []}

    def reset_progress(self):
        """진행 상태 초기화"""
        data = {"completed": [], "current_index": 0, "downloaded_files": 0, "failed": []}
        try:
            with open(self.progress_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    # ----------------------------------------------------------
    # 보고서 생성
    # ----------------------------------------------------------
    def create_report(self) -> Optional[str]:
        """다운로드 결과 엑셀 보고서 생성"""
        if not self.results:
            self.log("저장할 다운로드 결과가 없습니다.")
            return None

        try:
            today = datetime.now().strftime("%Y%m%d")
            excel_path = os.path.join(self.download_path, f"다운로드_요약보고서_{today}.xlsx")
            df = pd.DataFrame(self.results)

            with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
                # 요약 시트
                total = len(self.results)
                success = len([r for r in self.results if r['상태'] == '완료'])
                partial = len([r for r in self.results if r['상태'] == '부분완료'])
                failed = len([r for r in self.results if r['상태'] == '실패'])
                rate = round((success + partial) / total * 100, 1) if total else 0

                summary_df = pd.DataFrame({
                    '항목': ['전체 은행 수', '성공', '부분 성공', '실패', '성공률(%)'],
                    '건수': [total, success, partial, failed, rate]
                })
                summary_df.to_excel(writer, sheet_name='요약', index=False)
                df.to_excel(writer, sheet_name='상세결과', index=False)

            self.log(f"엑셀 요약보고서 생성 완료: {excel_path}")
            return excel_path

        except Exception as e:
            self.log(f"보고서 생성 오류: {str(e)}", 1)
            return None

    # ----------------------------------------------------------
    # 정리
    # ----------------------------------------------------------
    def stop(self):
        """다운로드 중단 요청"""
        self.is_running = False
        self.log("다운로드 중단 요청됨...")

    def cleanup(self):
        """리소스 정리"""
        if self.driver:
            try:
                self.driver.quit()
            except Exception:
                pass
            self.driver = None
        gc.collect()
        self.log("리소스 정리 완료")
