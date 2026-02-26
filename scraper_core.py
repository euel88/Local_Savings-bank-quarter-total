"""
저축은행 중앙회 통일경영공시 데이터 스크래핑 핵심 로직
Streamlit 웹 앱용 모듈 v4.0

개선사항 (v4.0):
- select_bank: 다중 폴백 방식 (JS 정확 매칭 -> XPath -> 광범위 검색)
- select_category: 다중 폴백 방식 (XPath -> JS 다전략 -> CSS 선택자)
- extract_tables_from_page: BeautifulSoup 폴백, 향상된 중복 제거
- extract_date_information: JavaScript 폴백 추가
- create_driver: 이미지 로딩 활성화, page_load_strategy 기본값 복원
- scrape_bank: 내장 재시도 로직 추가
- 대기 시간 조정으로 페이지 전환 안정성 향상
- 은행명 매핑에 애큐온, 상상인/상상인플러스 구분 강화
"""

import os
import sys
import time
import random
import json
import re
import zipfile
from datetime import datetime
from io import StringIO
import io
from contextlib import contextmanager
from pathlib import Path
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup
import pandas as pd
import warnings

warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=UserWarning)


_stderr_lock = threading.Lock()

# Chrome 드라이버 동시 생성 방지 (webdriver-manager 파일 잠금 충돌 방지)
_chrome_init_lock = threading.Lock()

@contextmanager
def suppress_stderr():
    """표준 에러 출력을 임시로 억제합니다 (스레드 안전)."""
    if not _stderr_lock.acquire(blocking=False):
        yield
        return
    original_stderr = sys.stderr
    sys.stderr = io.StringIO()
    try:
        yield
    finally:
        sys.stderr = original_stderr
        _stderr_lock.release()


class Config:
    """프로그램 설정을 관리하는 클래스"""
    VERSION = "4.0-streamlit"
    QUARTERLY_URL = "https://www.fsb.or.kr/busmagequar_0100.act"
    SETTLEMENT_URL = "https://www.fsb.or.kr/busmagesett_0100.act"
    MAX_RETRIES = 2
    PAGE_LOAD_TIMEOUT = 15
    WAIT_TIMEOUT = 8
    MAX_WORKERS = 4

    BANKS = [
        "다올", "대신", "더케이", "민국", "바로", "스카이", "신한", "애큐온", "예가람", "웰컴",
        "유안타", "조은", "키움YES", "푸른", "하나", "DB", "HB", "JT", "JT친애", "KB",
        "NH", "OK", "OSB", "SBI", "금화", "남양", "모아", "부림", "삼정", "상상인",
        "세람", "안국", "안양", "영진", "융창", "인성", "인천", "키움", "페퍼", "평택",
        "한국투자", "한화", "고려", "국제", "동원제일", "솔브레인", "에스앤티", "우리", "조흥", "진주",
        "흥국", "BNK", "DH", "IBK", "대백", "대아", "대원", "드림", "라온", "머스트삼일",
        "엠에스", "오성", "유니온", "참", "CK", "대한", "더블", "동양", "삼호",
        "센트럴", "스마트", "스타", "대명", "상상인플러스", "아산", "오투", "우리금융", "청주", "한성"
    ]

    CATEGORIES = ["영업개황", "재무현황", "손익현황", "기타"]

    def __init__(self, scrape_type="quarterly", output_dir=None):
        self.today = datetime.now().strftime("%Y%m%d")
        self.scrape_type = scrape_type
        self.BASE_URL = self.QUARTERLY_URL if scrape_type == "quarterly" else self.SETTLEMENT_URL
        if output_dir:
            self.output_dir = os.path.abspath(output_dir)
            os.makedirs(self.output_dir, exist_ok=True)
        else:
            self.output_dir = tempfile.mkdtemp(prefix=f"저축은행_{scrape_type}_")


class WaitUtils:
    """대기 유틸리티 클래스"""

    @staticmethod
    def wait_for_element(driver, locator, timeout):
        """요소가 나타날 때까지 명시적으로 대기합니다."""
        try:
            element = WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located(locator)
            )
            return element
        except TimeoutException:
            return None

    @staticmethod
    def wait_for_clickable(driver, locator, timeout):
        """요소가 클릭 가능할 때까지 명시적으로 대기합니다."""
        try:
            element = WebDriverWait(driver, timeout).until(
                EC.element_to_be_clickable(locator)
            )
            return element
        except TimeoutException:
            return None

    @staticmethod
    def wait_for_page_load(driver, timeout=10):
        """페이지 로딩 완료 대기"""
        try:
            WebDriverWait(driver, timeout).until(
                lambda d: d.execute_script('return document.readyState') == 'complete'
            )
            return True
        except Exception:
            return False

    @staticmethod
    def wait_with_random(min_sec=0.5, max_sec=1.5):
        """랜덤 대기"""
        time.sleep(random.uniform(min_sec, max_sec))


class StreamlitLogger:
    """Streamlit용 로거 클래스 (스레드 안전)"""

    def __init__(self, streamlit_container=None):
        self.messages = []
        self._lock = threading.Lock()
        self.container = streamlit_container

    def log_message(self, message, verbose=True):
        if verbose:
            timestamp = datetime.now().strftime("%H:%M:%S")
            log_entry = f"[{timestamp}] {message}"
            with self._lock:
                self.messages.append(log_entry)
            if self.container:
                self.container.text(log_entry)

    def get_logs(self):
        with self._lock:
            return "\n".join(self.messages)

    def get_messages_snapshot(self):
        """스레드 안전한 메시지 스냅샷 반환"""
        with self._lock:
            return self.messages.copy()


def create_driver(timeout=60):
    """Streamlit Cloud 환경에 맞는 Chrome 드라이버 생성 (고유 프로필 사용)

    동시 생성 방지를 위해 글로벌 락 사용.
    timeout: 락 대기 최대 시간(초). 기본 60초.
    """
    acquired = _chrome_init_lock.acquire(timeout=timeout)
    if not acquired:
        raise RuntimeError(f"Chrome 드라이버 생성 락 획득 실패 ({timeout}초 타임아웃)")
    try:
        return _create_driver_internal()
    finally:
        _chrome_init_lock.release()


def _create_driver_internal():
    """실제 Chrome 드라이버 생성 로직 (락 내부에서 호출)"""
    with suppress_stderr():
        options = webdriver.ChromeOptions()
        options.add_argument('--headless=new')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1280,800')
        options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')

        # 고유한 user-data-dir로 Chrome 프로필 잠금 충돌 방지
        user_data_dir = tempfile.mkdtemp(prefix="chrome_scraper_")
        options.add_argument(f'--user-data-dir={user_data_dir}')
        options.add_argument('--log-level=3')
        options.add_argument('--silent')
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-browser-side-navigation')
        options.add_argument('--disable-infobars')
        options.add_argument('--disable-notifications')
        options.add_argument('--disable-popup-blocking')

        prefs = {
            'profile.default_content_setting_values': {
                'images': 1,      # 이미지 로딩 활성화 (페이지 렌더링에 필요)
                'plugins': 2,
                'javascript': 1,
                'notifications': 2
            },
            'disk-cache-size': 4096,
        }
        options.add_experimental_option('prefs', prefs)
        options.add_experimental_option('excludeSwitches', ['enable-logging'])

        # Streamlit Cloud의 Chromium 경로
        chromium_paths = [
            '/usr/bin/chromium',
            '/usr/bin/chromium-browser',
            '/usr/bin/google-chrome',
            '/usr/bin/google-chrome-stable'
        ]

        for path in chromium_paths:
            if os.path.exists(path):
                options.binary_location = path
                break

        # ChromeDriver 경로
        chromedriver_paths = [
            '/usr/bin/chromedriver',
            '/usr/lib/chromium/chromedriver',
            '/usr/lib/chromium-browser/chromedriver'
        ]

        service = None
        for path in chromedriver_paths:
            if os.path.exists(path):
                service = Service(executable_path=path)
                break

        if service:
            driver = webdriver.Chrome(service=service, options=options)
        else:
            try:
                from webdriver_manager.chrome import ChromeDriverManager
                from webdriver_manager.core.os_manager import ChromeType
                service = Service(ChromeDriverManager(chrome_type=ChromeType.CHROMIUM).install())
                driver = webdriver.Chrome(service=service, options=options)
            except Exception:
                driver = webdriver.Chrome(options=options)

        driver.set_page_load_timeout(15)
        driver.set_script_timeout(15)
        return driver


class BankScraper:
    """은행 데이터 스크래퍼 클래스"""

    def __init__(self, config, logger):
        self.config = config
        self.logger = logger

    @staticmethod
    def _date_sort_key(date_str):
        """날짜 문자열에서 (연도, 월) 튜플을 추출하여 정렬 키로 사용"""
        year_match = re.search(r'(\d{4})년', date_str)
        month_match = re.search(r'(\d{1,2})월', date_str)
        year = int(year_match.group(1)) if year_match else 0
        month = int(month_match.group(1)) if month_match else 0
        return (year, month)

    @staticmethod
    def normalize_date(date_str):
        """날짜를 'YYYY년 MM월말' 형식으로 통일"""
        if not date_str or date_str in ("날짜 정보 없음", "날짜 추출 실패"):
            return date_str
        match = re.search(r'(\d{4})년\s*(\d{1,2})월', date_str)
        if match:
            year = match.group(1)
            month = match.group(2).zfill(2)
            return f"{year}년 {month}월말"
        return date_str

    def extract_date_information(self, driver):
        """웹페이지에서 공시 날짜 정보를 추출합니다. (다중 폴백)"""
        try:
            date_pattern = re.compile(r'\d{4}년\s*\d{1,2}월\s*말?')

            # 방법 1: 당기 데이터 우선 찾기
            current_period_elements = driver.find_elements(
                By.XPATH,
                "//*[contains(text(), '당기') and contains(text(), '년') and contains(text(), '월')]"
            )

            if current_period_elements:
                for element in current_period_elements:
                    try:
                        text = element.text
                        matches = date_pattern.findall(text)
                        if matches:
                            latest_date = max(matches, key=self._date_sort_key)
                            return self.normalize_date(latest_date)
                    except StaleElementReferenceException:
                        continue

            # 방법 2: 모든 날짜를 찾아서 가장 최근 것 선택
            all_date_elements = driver.find_elements(
                By.XPATH,
                "//*[contains(text(), '년') and contains(text(), '월')]"
            )

            all_dates = []
            for element in all_date_elements:
                try:
                    text = element.text
                    matches = date_pattern.findall(text)
                    all_dates.extend(matches)
                except StaleElementReferenceException:
                    continue

            if all_dates:
                unique_dates = list(set(all_dates))
                sorted_dates = sorted(unique_dates, key=self._date_sort_key, reverse=True)
                return self.normalize_date(sorted_dates[0])

            # 방법 3: JavaScript로 직접 추출
            js_script = """
            var allText = document.body.innerText;

            var currentPeriodMatch = allText.match(/당기[^\\n]*?(\\d{4}년\\s*\\d{1,2}월\\s*말?)/);
            if (currentPeriodMatch) {
                return currentPeriodMatch[1];
            }

            var allMatches = allText.match(/\\d{4}년\\s*\\d{1,2}월\\s*말?/g);
            if (allMatches) {
                allMatches.sort(function(a, b) {
                    return parseInt(b.substr(0, 4)) - parseInt(a.substr(0, 4));
                });
                return allMatches[0];
            }

            return '';
            """

            date_text = driver.execute_script(js_script)
            if date_text:
                return self.normalize_date(date_text)

            return "날짜 정보 없음"

        except Exception:
            return "날짜 추출 실패"

    def select_bank(self, driver, bank_name):
        """다양한 방법으로 은행을 선택합니다. (정확한 매칭 우선, 다중 폴백)"""
        try:
            # 메인 페이지로 접속
            driver.get(self.config.BASE_URL)

            # 페이지 로딩 완료 대기
            WaitUtils.wait_for_page_load(driver, self.config.PAGE_LOAD_TIMEOUT)
            WaitUtils.wait_with_random(0.5, 1.0)

            # 특수 케이스 처리를 위한 정확한 은행명 목록
            exact_bank_names = {
                "키움": ["키움", "키움저축은행"],
                "키움YES": ["키움YES", "키움YES저축은행"],
                "JT": ["JT", "JT저축은행"],
                "JT친애": ["JT친애", "JT친애저축은행", "친애", "친애저축은행"],
                "상상인": ["상상인", "상상인저축은행"],
                "상상인플러스": ["상상인플러스", "상상인플러스저축은행"],
                "머스트삼일": ["머스트삼일", "머스트삼일저축은행"],
                "애큐온": ["애큐온", "애큐온저축은행"]
            }

            search_names = exact_bank_names.get(bank_name, [bank_name, f"{bank_name}저축은행"])

            # 방법 1: JavaScript로 정확한 은행명 매칭
            js_script = """
            var targetBankNames = arguments[0];
            var bankNameKey = arguments[1];
            var found = false;

            var allElements = document.querySelectorAll('td, a');

            for(var i = 0; i < allElements.length; i++) {
                var element = allElements[i];
                var elementText = element.textContent.trim();

                for(var j = 0; j < targetBankNames.length; j++) {
                    if(elementText === targetBankNames[j]) {
                        if(bankNameKey === '키움' && elementText.includes('YES')) continue;
                        if(bankNameKey === 'JT' && elementText.includes('친애')) continue;
                        if(bankNameKey === '상상인' && elementText.includes('플러스')) continue;

                        element.scrollIntoView({block: 'center'});

                        if(element.tagName === 'A') {
                            element.click();
                            found = true;
                            break;
                        } else {
                            var link = element.querySelector('a');
                            if(link) {
                                link.click();
                                found = true;
                                break;
                            } else {
                                element.click();
                                found = true;
                                break;
                            }
                        }
                    }
                }
                if(found) break;
            }

            return found ? "exact_js_match" : false;
            """

            result = driver.execute_script(js_script, search_names, bank_name)
            if result:
                self.logger.log_message(f"{bank_name}: {result}", verbose=False)
                WaitUtils.wait_with_random(1.0, 1.5)
                if driver.current_url != self.config.BASE_URL:
                    return True

            # 방법 2: XPath로 정확한 텍스트 매칭
            for search_name in search_names:
                if bank_name == "키움":
                    xpath = f"//td[normalize-space(text())='{search_name}' and not(contains(text(), 'YES'))]"
                elif bank_name == "JT":
                    xpath = f"//td[normalize-space(text())='{search_name}' and not(contains(text(), '친애'))]"
                elif bank_name == "상상인":
                    xpath = f"//td[normalize-space(text())='{search_name}' and not(contains(text(), '플러스'))]"
                else:
                    xpath = f"//td[normalize-space(text())='{search_name}']"

                bank_elements = driver.find_elements(By.XPATH, xpath)

                if bank_elements:
                    for element in bank_elements:
                        try:
                            if element.is_displayed():
                                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
                                WaitUtils.wait_with_random(0.3, 0.5)
                                driver.execute_script("arguments[0].click();", element)
                                WaitUtils.wait_with_random(1.0, 1.5)

                                if driver.current_url != self.config.BASE_URL:
                                    return True
                        except (StaleElementReferenceException, NoSuchElementException):
                            continue

            # 방법 3: XPath로 링크(a) 태그에서 검색
            for search_name in search_names:
                xpath_a = f"//a[normalize-space(text())='{search_name}']"
                a_elements = driver.find_elements(By.XPATH, xpath_a)

                for element in a_elements:
                    try:
                        if element.is_displayed():
                            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
                            WaitUtils.wait_with_random(0.3, 0.5)
                            driver.execute_script("arguments[0].click();", element)
                            WaitUtils.wait_with_random(1.0, 1.5)

                            if driver.current_url != self.config.BASE_URL:
                                return True
                    except (StaleElementReferenceException, NoSuchElementException):
                        continue

            self.logger.log_message(f"{bank_name} 은행을 찾을 수 없습니다.")
            return False

        except Exception as e:
            self.logger.log_message(f"{bank_name} 선택 실패: {str(e)}")
            return False

    def select_category(self, driver, category):
        """특정 카테고리 탭을 클릭합니다. (다중 폴백 방식)"""
        try:
            # 방법 1: 정확한 텍스트 매칭 XPath
            tab_xpaths = [
                f"//a[normalize-space(text())='{category}']",
                f"//a[contains(@class, 'tab') and contains(text(), '{category}')]",
                f"//li[contains(@class, 'tab') and contains(text(), '{category}')]",
                f"//span[contains(text(), '{category}')]",
                f"//button[contains(text(), '{category}')]"
            ]

            for xpath in tab_xpaths:
                elements = driver.find_elements(By.XPATH, xpath)
                for element in elements:
                    try:
                        if element.is_displayed():
                            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
                            WaitUtils.wait_with_random(0.3, 0.5)
                            driver.execute_script("arguments[0].click();", element)
                            WaitUtils.wait_with_random(0.5, 1.0)
                            return True
                    except (StaleElementReferenceException, NoSuchElementException):
                        continue

            # 방법 2: JavaScript로 카테고리 탭 클릭 (다중 전략)
            category_indices = {
                "영업개황": 0,
                "재무현황": 1,
                "손익현황": 2,
                "기타": 3
            }

            if category in category_indices:
                idx = category_indices[category]
                script = """
                var category = arguments[0];
                var idx = arguments[1];

                // 전략 1: 정확한 텍스트 매칭
                var allElements = document.querySelectorAll('a, button, span, li, div');
                for (var k = 0; k < allElements.length; k++) {
                    if (allElements[k].innerText.trim() === category) {
                        allElements[k].scrollIntoView({block: 'center'});
                        allElements[k].click();
                        return "exact_match";
                    }
                }

                // 전략 2: 탭 컨테이너에서 텍스트 매칭
                var tabContainers = document.querySelectorAll('ul.tabs, div.tab-container, nav, .tab-list, ul, div[role="tablist"]');
                for (var i = 0; i < tabContainers.length; i++) {
                    var tabs = tabContainers[i].querySelectorAll('a, li, button, div[role="tab"], span');

                    for (var j = 0; j < tabs.length; j++) {
                        if (tabs[j].innerText.includes(category)) {
                            tabs[j].scrollIntoView({block: 'center'});
                            tabs[j].click();
                            return "text_match_in_container";
                        }
                    }

                    if (tabs.length >= idx + 1) {
                        tabs[idx].scrollIntoView({block: 'center'});
                        tabs[idx].click();
                        return "index_match";
                    }
                }

                // 전략 3: 포함 문자열 검색
                var clickables = document.querySelectorAll('a, button, span, div, li');
                for (var j = 0; j < clickables.length; j++) {
                    if (clickables[j].innerText.includes(category)) {
                        clickables[j].scrollIntoView({block: 'center'});
                        clickables[j].click();
                        return "contains_match";
                    }
                }

                return false;
                """

                result = driver.execute_script(script, category, idx)
                if result:
                    self.logger.log_message(f"{category} 탭: {result}", verbose=False)
                    WaitUtils.wait_with_random(0.5, 1.0)
                    return True

            # 방법 3: 포함 문자열로 넓은 범위 검색
            tab_broad_xpath = f"//*[contains(text(), '{category}')]"
            elements = driver.find_elements(By.XPATH, tab_broad_xpath)

            for element in elements:
                try:
                    if element.is_displayed() and element.tag_name in ['a', 'li', 'span', 'button', 'div']:
                        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
                        WaitUtils.wait_with_random(0.3, 0.5)
                        driver.execute_script("arguments[0].click();", element)
                        WaitUtils.wait_with_random(0.5, 1.0)
                        return True
                except (StaleElementReferenceException, NoSuchElementException):
                    continue

            # 방법 4: CSS 선택자 시도
            tab_css = "[role='tab'], .tab, .tab-item, .tabs li, .tabs a, nav a, ul li a"
            tabs = driver.find_elements(By.CSS_SELECTOR, tab_css)

            for tab in tabs:
                try:
                    if category in tab.text and tab.is_displayed():
                        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", tab)
                        WaitUtils.wait_with_random(0.3, 0.5)
                        driver.execute_script("arguments[0].click();", tab)
                        WaitUtils.wait_with_random(0.5, 1.0)
                        return True
                except (StaleElementReferenceException, NoSuchElementException):
                    continue

            self.logger.log_message(f"{category} 탭을 찾을 수 없습니다.", verbose=False)
            return False

        except Exception as e:
            self.logger.log_message(f"{category} 탭 클릭 실패: {str(e)}", verbose=False)
            return False

    def extract_tables_from_page(self, driver):
        """현재 페이지에서 모든 테이블을 추출합니다. (pandas + BeautifulSoup 폴백)"""
        try:
            WaitUtils.wait_for_page_load(driver, self.config.PAGE_LOAD_TIMEOUT)
            WaitUtils.wait_with_random(0.5, 1.0)

            # 방법 1: pandas로 테이블 추출
            try:
                html_source = driver.page_source
                dfs = pd.read_html(StringIO(html_source))

                if dfs:
                    valid_dfs = []
                    seen_hashes = set()

                    for df in dfs:
                        if not df.empty and df.shape[0] > 0 and df.shape[1] > 0:
                            # MultiIndex 컬럼 처리
                            if isinstance(df.columns, pd.MultiIndex):
                                new_cols = []
                                for col in df.columns:
                                    if isinstance(col, tuple):
                                        clean_col = [str(c).strip() for c in col if str(c).strip() and str(c).lower() != 'nan']
                                        new_cols.append('_'.join(clean_col) if clean_col else f"Column_{len(new_cols)+1}")
                                    else:
                                        new_cols.append(str(col))
                                df.columns = new_cols

                            # 향상된 중복 테이블 제거 (shape + headers + 첫행 데이터)
                            try:
                                shape_hash = f"{df.shape}"
                                headers_hash = f"{list(df.columns)}"
                                data_hash = ""
                                if len(df) > 0:
                                    data_hash = f"{list(df.iloc[0].astype(str))}"

                                table_hash = f"{shape_hash}_{headers_hash}_{data_hash}"

                                if table_hash not in seen_hashes:
                                    valid_dfs.append(df)
                                    seen_hashes.add(table_hash)
                            except Exception:
                                valid_dfs.append(df)

                    return valid_dfs
            except Exception:
                pass

            # 방법 2: BeautifulSoup으로 테이블 추출 (pandas 실패 시 폴백)
            try:
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                tables = soup.find_all('table')

                extracted_dfs = []
                table_hashes = set()

                for table in tables:
                    try:
                        headers = []
                        rows = []

                        # 헤더 추출
                        th_elements = table.select('thead th') or table.select('tr:first-child th')
                        if th_elements:
                            headers = [th.get_text(strip=True) for th in th_elements]

                        # 헤더가 없으면 첫 번째 행의 td를 헤더로 사용
                        if not headers:
                            first_row_tds = table.select('tr:first-child td')
                            if first_row_tds:
                                headers = [td.get_text(strip=True) or f"Column_{i+1}" for i, td in enumerate(first_row_tds)]

                        # 헤더가 없으면 기본 열 이름 생성
                        if not headers:
                            all_rows = table.select('tr')
                            max_cols = max([len(row.select('td')) for row in all_rows], default=0)
                            headers = [f'Column_{j+1}' for j in range(max_cols)]

                        # 데이터 행 추출
                        header_rows = table.select('thead tr')
                        data_rows = table.select('tbody tr') or table.select('tr')[1:]
                        for tr in data_rows:
                            if tr in header_rows:
                                continue

                            cells = tr.select('td')
                            if cells:
                                row_data = [td.get_text(strip=True) for td in cells]
                                if row_data and len(row_data) > 0:
                                    rows.append(row_data)

                        if rows and headers:
                            # 열 개수 맞추기
                            for i, row in enumerate(rows):
                                if len(row) < len(headers):
                                    rows[i] = row + [''] * (len(headers) - len(row))
                                elif len(row) > len(headers):
                                    rows[i] = row[:len(headers)]

                            df = pd.DataFrame(rows, columns=headers)

                            if not df.empty:
                                try:
                                    first_row_str = str(df.iloc[0].values) if len(df) > 0 else ''
                                    table_hash = f"{df.shape}_{hash(first_row_str)}"
                                    if table_hash not in table_hashes:
                                        extracted_dfs.append(df)
                                        table_hashes.add(table_hash)
                                except Exception:
                                    extracted_dfs.append(df)
                    except Exception:
                        continue

                if extracted_dfs:
                    return extracted_dfs
            except Exception:
                pass

            return []

        except Exception:
            return []

    def scrape_bank(self, bank_name, progress_callback=None):
        """단일 은행 데이터 스크래핑 - 내장 재시도 로직 포함"""
        date_info = "날짜 정보 없음"

        for attempt in range(self.config.MAX_RETRIES + 1):
            driver = None
            try:
                try:
                    driver = create_driver()
                except RuntimeError as e:
                    self.logger.log_message(f"{bank_name} Chrome 생성 실패: {str(e)}")
                    return None, False, date_info
                except Exception as e:
                    self.logger.log_message(f"{bank_name} Chrome 생성 오류: {str(e)[:80]}")
                    return None, False, date_info

                if attempt == 0:
                    self.logger.log_message(f"[시작] {bank_name} 은행 스크래핑")
                else:
                    self.logger.log_message(f"[재시도 {attempt}/{self.config.MAX_RETRIES}] {bank_name}")

                if not self.select_bank(driver, bank_name):
                    self.logger.log_message(f"{bank_name} 선택 실패")
                    if attempt < self.config.MAX_RETRIES:
                        WaitUtils.wait_with_random(1.0, 2.0)
                        continue
                    return None, False, date_info

                # 은행 페이지 URL 확인
                try:
                    _ = driver.current_url
                except Exception:
                    self.logger.log_message(f"{bank_name} 페이지 URL 획득 실패")
                    if attempt < self.config.MAX_RETRIES:
                        continue
                    return None, False, date_info

                date_info = self.extract_date_information(driver)
                self.logger.log_message(f"{bank_name} 공시일: {date_info}")

                result_data = {'날짜정보': date_info}
                all_table_hashes = set()

                for category in self.config.CATEGORIES:
                    if progress_callback:
                        progress_callback(bank_name, f"{category} 처리 중")

                    try:
                        if not self.select_category(driver, category):
                            self.logger.log_message(f"{bank_name} {category} 탭 클릭 실패", verbose=False)
                            continue

                        tables = self.extract_tables_from_page(driver)
                        if not tables:
                            continue

                        # 전체 은행 범위 중복 제거
                        valid_tables = []
                        for df in tables:
                            try:
                                shape_hash = f"{df.shape}"
                                headers_hash = f"{list(df.columns)}"
                                data_hash = ""
                                if len(df) > 0:
                                    data_hash = f"{list(df.iloc[0].astype(str))}"

                                table_hash = f"{shape_hash}_{headers_hash}_{data_hash}"

                                if table_hash not in all_table_hashes:
                                    valid_tables.append(df)
                                    all_table_hashes.add(table_hash)
                            except Exception:
                                valid_tables.append(df)

                        if valid_tables:
                            result_data[category] = valid_tables
                            self.logger.log_message(f"{bank_name} - {category}: {len(valid_tables)}개 테이블")

                    except Exception as e:
                        self.logger.log_message(f"{bank_name} {category} 처리 실패: {str(e)}", verbose=False)

                # 데이터 수집 여부 확인
                has_data = any(
                    isinstance(data, list) and data
                    for key, data in result_data.items()
                    if key != '날짜정보'
                )

                if not has_data:
                    self.logger.log_message(f"{bank_name} 데이터 추출 실패")
                    if attempt < self.config.MAX_RETRIES:
                        WaitUtils.wait_with_random(1.0, 2.0)
                        continue
                    return None, False, date_info

                # Excel 파일 저장
                scrape_type_name = "분기공시" if self.config.scrape_type == "quarterly" else "결산공시"
                safe_date = date_info.replace('/', '_').replace(' ', '')
                filename = f"{bank_name}_{scrape_type_name}_{safe_date}.xlsx"
                filepath = os.path.join(self.config.output_dir, filename)

                with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                    # 공시정보 시트
                    pd.DataFrame({
                        '은행명': [bank_name],
                        '공시 날짜': [date_info],
                        '추출 일시': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
                        '스크래핑 시스템': [f'통일경영공시 자동 스크래퍼 v{self.config.VERSION}']
                    }).to_excel(writer, sheet_name='공시정보', index=False)

                    for category, tables in result_data.items():
                        if category == '날짜정보':
                            continue
                        for idx, df in enumerate(tables):
                            if len(tables) > 1:
                                sheet_name = f"{category}_{idx+1}"
                            else:
                                sheet_name = category
                            sheet_name = sheet_name[:31]

                            # MultiIndex 확인 및 처리
                            if isinstance(df.columns, pd.MultiIndex):
                                new_cols = []
                                for col in df.columns:
                                    if isinstance(col, tuple):
                                        col_parts = [str(c).strip() for c in col if str(c).strip() and str(c).lower() != 'nan']
                                        new_cols.append('_'.join(col_parts) if col_parts else f"Column_{len(new_cols)+1}")
                                    else:
                                        new_cols.append(str(col))
                                df.columns = new_cols

                            df.to_excel(writer, sheet_name=sheet_name, index=False)

                self.logger.log_message(f"[완료] {bank_name} 저장완료")
                return filepath, True, date_info

            except Exception as e:
                self.logger.log_message(f"{bank_name} 스크래핑 오류: {str(e)}")
                if attempt < self.config.MAX_RETRIES:
                    WaitUtils.wait_with_random(1.0, 2.0)
                    continue
                return None, False, date_info
            finally:
                if driver:
                    user_data_dir = None
                    for arg in driver.options.arguments:
                        if arg.startswith('--user-data-dir='):
                            user_data_dir = arg.split('=', 1)[1]
                            break
                    try:
                        driver.quit()
                    except Exception:
                        pass
                    if user_data_dir and os.path.exists(user_data_dir):
                        import shutil
                        try:
                            shutil.rmtree(user_data_dir, ignore_errors=True)
                        except Exception:
                            pass

        return None, False, date_info

    def scrape_multiple_banks(self, banks, progress_callback=None):
        """여러 은행 병렬 스크래핑 (ThreadPoolExecutor)"""
        total = len(banks)
        results = [None] * total
        completed_count = 0
        lock = threading.Lock()

        def _scrape_one(idx, bank):
            nonlocal completed_count
            if progress_callback:
                progress_callback(bank, f"처리 중 ({idx+1}/{total})")

            try:
                filepath, success, date_info = self.scrape_bank(bank, progress_callback)
            except Exception:
                filepath, success, date_info = None, False, "오류"

            result = {
                'bank': bank,
                'success': success,
                'filepath': filepath,
                'date_info': date_info
            }

            with lock:
                results[idx] = result
                completed_count += 1

            if progress_callback:
                status = "완료" if success else "실패"
                progress_callback(bank, status)

            return result

        max_workers = min(self.config.MAX_WORKERS, total)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_scrape_one, idx, bank): bank
                for idx, bank in enumerate(banks)
            }
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception:
                    pass

        return results

    def create_zip_archive(self, results, custom_filename=None):
        """결과 파일들을 ZIP으로 압축"""
        successful_files = [r['filepath'] for r in results if r['success'] and r['filepath']]

        if not successful_files:
            return None

        if custom_filename:
            zip_filename = f"{custom_filename}.zip"
        else:
            zip_filename = f"저축은행_{self.config.scrape_type}_{self.config.today}.zip"

        zip_path = os.path.join(self.config.output_dir, zip_filename)

        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for filepath in successful_files:
                if os.path.exists(filepath):
                    zipf.write(filepath, os.path.basename(filepath))

            summary_df = create_summary_dataframe(results)
            summary_path = os.path.join(self.config.output_dir, "스크래핑_요약.xlsx")
            summary_df.to_excel(summary_path, index=False)
            zipf.write(summary_path, "스크래핑_요약.xlsx")

        return zip_path


def create_summary_dataframe(results, bank_dates=None):
    """스크래핑 결과 요약 DataFrame 생성 - 공시날짜 포함"""
    summary_data = []
    for r in results:
        date_info = r.get('date_info', '')
        if not date_info and bank_dates:
            date_info = bank_dates.get(r['bank'], '')

        summary_data.append({
            '은행명': r['bank'],
            '공시날짜': date_info if date_info else '-',
            '상태': '✅ 성공' if r['success'] else '❌ 실패',
            '파일명': os.path.basename(r['filepath']) if r['filepath'] else '-'
        })

    return pd.DataFrame(summary_data)
