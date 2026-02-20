"""
저축은행 중앙회 통일경영공시 데이터 스크래핑 핵심 로직
Streamlit 웹 앱용 모듈 v3.1
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


@contextmanager
def suppress_stderr():
    """표준 에러 출력을 임시로 억제합니다."""
    original_stderr = sys.stderr
    sys.stderr = io.StringIO()
    try:
        yield
    finally:
        sys.stderr = original_stderr


class Config:
    """프로그램 설정을 관리하는 클래스"""
    VERSION = "3.1-streamlit"
    QUARTERLY_URL = "https://www.fsb.or.kr/busmagequar_0100.act"  # 분기공시 URL
    SETTLEMENT_URL = "https://www.fsb.or.kr/busmagesett_0100.act"  # 결산공시 URL
    MAX_RETRIES = 2
    PAGE_LOAD_TIMEOUT = 10
    WAIT_TIMEOUT = 5
    MAX_WORKERS = 3

    # 전체 79개 저축은행 목록
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
    def wait_for_page_load(driver, timeout=10):
        """페이지 로딩 완료 대기"""
        try:
            WebDriverWait(driver, timeout).until(
                lambda d: d.execute_script('return document.readyState') == 'complete'
            )
            return True
        except:
            return False

    @staticmethod
    def wait_with_random(min_sec=0.5, max_sec=1.5):
        """랜덤 대기"""
        time.sleep(random.uniform(min_sec, max_sec))


class StreamlitLogger:
    """Streamlit용 로거 클래스"""

    def __init__(self, streamlit_container=None):
        self.messages = []
        self.container = streamlit_container

    def log_message(self, message, verbose=True):
        if verbose:
            timestamp = datetime.now().strftime("%H:%M:%S")
            log_entry = f"[{timestamp}] {message}"
            self.messages.append(log_entry)
            if self.container:
                self.container.text(log_entry)

    def get_logs(self):
        return "\n".join(self.messages)


def create_driver():
    """Streamlit Cloud 환경에 맞는 Chrome 드라이버 생성"""
    with suppress_stderr():
        options = webdriver.ChromeOptions()
        options.add_argument('--headless=new')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1280,800')
        options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
        options.add_argument('--log-level=3')
        options.add_argument('--silent')
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-browser-side-navigation')
        options.add_argument('--disable-infobars')
        options.add_argument('--disable-notifications')
        options.add_argument('--disable-popup-blocking')

        prefs = {
            'profile.default_content_setting_values': {
                'images': 2,  # 이미지 로딩 비활성화 (속도 향상)
                'plugins': 2,
                'javascript': 1,
                'notifications': 2
            }
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
            # webdriver-manager 사용 시도
            try:
                from webdriver_manager.chrome import ChromeDriverManager
                from webdriver_manager.core.os_manager import ChromeType
                service = Service(ChromeDriverManager(chrome_type=ChromeType.CHROMIUM).install())
                driver = webdriver.Chrome(service=service, options=options)
            except:
                driver = webdriver.Chrome(options=options)

        driver.set_page_load_timeout(10)
        return driver


class BankScraper:
    """은행 데이터 스크래퍼 클래스"""

    def __init__(self, config, logger):
        self.config = config
        self.logger = logger

    def extract_date_information(self, driver):
        """웹페이지에서 공시 날짜 정보를 추출합니다."""
        try:
            # 당기 데이터 우선 찾기
            current_period_elements = driver.find_elements(
                By.XPATH,
                "//*[contains(text(), '당기') and contains(text(), '년') and contains(text(), '월')]"
            )

            if current_period_elements:
                for element in current_period_elements:
                    text = element.text
                    date_pattern = re.compile(r'\d{4}년\s*\d{1,2}월\s*말?')
                    matches = date_pattern.findall(text)

                    if matches:
                        latest_date = max(matches, key=lambda x: int(re.search(r'\d{4}', x).group()))
                        # 날짜 형식 정리 (예: "2025년 9월말")
                        cleaned_date = re.sub(r'\s+', '', latest_date)
                        if not cleaned_date.endswith('말'):
                            cleaned_date += '말'
                        return cleaned_date

            # 모든 날짜 찾기
            all_date_elements = driver.find_elements(
                By.XPATH,
                "//*[contains(text(), '년') and contains(text(), '월')]"
            )

            all_dates = []
            for element in all_date_elements:
                text = element.text
                date_pattern = re.compile(r'\d{4}년\s*\d{1,2}월\s*말?')
                matches = date_pattern.findall(text)
                all_dates.extend(matches)

            if all_dates:
                unique_dates = list(set(all_dates))
                sorted_dates = sorted(unique_dates, key=lambda x: int(re.search(r'\d{4}', x).group()), reverse=True)
                cleaned_date = re.sub(r'\s+', '', sorted_dates[0])
                if not cleaned_date.endswith('말'):
                    cleaned_date += '말'
                return cleaned_date

            return "날짜 정보 없음"

        except Exception as e:
            return "날짜 추출 실패"

    def select_bank(self, driver, bank_name):
        """은행을 선택합니다."""
        try:
            driver.get(self.config.BASE_URL)
            WaitUtils.wait_for_page_load(driver, self.config.PAGE_LOAD_TIMEOUT)
            WaitUtils.wait_with_random(0.2, 0.5)

            # 은행명 매핑
            exact_bank_names = {
                "키움": ["키움", "키움저축은행"],
                "키움YES": ["키움YES", "키움YES저축은행"],
                "JT": ["JT", "JT저축은행"],
                "JT친애": ["JT친애", "JT친애저축은행", "친애", "친애저축은행"],
                "상상인": ["상상인", "상상인저축은행"],
                "상상인플러스": ["상상인플러스", "상상인플러스저축은행"],
                "머스트삼일": ["머스트삼일", "머스트삼일저축은행"]
            }

            search_names = exact_bank_names.get(bank_name, [bank_name, f"{bank_name}저축은행"])

            # JavaScript로 은행 선택
            js_script = f"""
            var targetBankNames = {json.dumps(search_names)};
            var found = false;

            var allElements = document.querySelectorAll('td, a');

            for(var i = 0; i < allElements.length; i++) {{
                var element = allElements[i];
                var elementText = element.textContent.trim();

                for(var j = 0; j < targetBankNames.length; j++) {{
                    if(elementText === targetBankNames[j]) {{
                        if('{bank_name}' === '키움' && elementText.includes('YES')) continue;
                        if('{bank_name}' === 'JT' && elementText.includes('친애')) continue;

                        element.scrollIntoView({{block: 'center'}});

                        if(element.tagName === 'A') {{
                            element.click();
                            found = true;
                            break;
                        }} else {{
                            var link = element.querySelector('a');
                            if(link) {{
                                link.click();
                                found = true;
                                break;
                            }} else {{
                                element.click();
                                found = true;
                                break;
                            }}
                        }}
                    }}
                }}
                if(found) break;
            }}

            return found;
            """

            result = driver.execute_script(js_script)
            if result:
                WaitUtils.wait_with_random(0.3, 0.7)
                if driver.current_url != self.config.BASE_URL:
                    return True

            return False

        except Exception as e:
            self.logger.log_message(f"{bank_name} 선택 실패: {str(e)}")
            return False

    def select_category(self, driver, category):
        """카테고리 탭을 선택합니다."""
        try:
            category_indices = {
                "영업개황": 0,
                "재무현황": 1,
                "손익현황": 2,
                "기타": 3
            }

            if category in category_indices:
                script = f"""
                var allElements = document.querySelectorAll('a, button, span, li, div');
                for (var k = 0; k < allElements.length; k++) {{
                    if (allElements[k].innerText.trim() === '{category}') {{
                        allElements[k].scrollIntoView({{block: 'center'}});
                        allElements[k].click();
                        return true;
                    }}
                }}
                return false;
                """

                result = driver.execute_script(script)
                if result:
                    WaitUtils.wait_with_random(0.2, 0.5)
                    return True

            return False

        except Exception as e:
            return False

    def extract_tables_from_page(self, driver):
        """페이지에서 테이블을 추출합니다."""
        try:
            WaitUtils.wait_for_page_load(driver, self.config.PAGE_LOAD_TIMEOUT)
            WaitUtils.wait_with_random(0.2, 0.4)

            html_source = driver.page_source
            dfs = pd.read_html(StringIO(html_source))

            if dfs:
                valid_dfs = []
                seen_shapes = set()

                for df in dfs:
                    if not df.empty and df.shape[0] > 0 and df.shape[1] > 0:
                        if isinstance(df.columns, pd.MultiIndex):
                            new_cols = []
                            for col in df.columns:
                                if isinstance(col, tuple):
                                    clean_col = [str(c).strip() for c in col if str(c).strip() and str(c).lower() != 'nan']
                                    new_cols.append('_'.join(clean_col) if clean_col else f"Column_{len(new_cols)+1}")
                                else:
                                    new_cols.append(str(col))
                            df.columns = new_cols

                        shape_hash = f"{df.shape}_{list(df.columns)}"
                        if shape_hash not in seen_shapes:
                            valid_dfs.append(df)
                            seen_shapes.add(shape_hash)

                return valid_dfs

            return []

        except Exception as e:
            return []

    def scrape_bank(self, bank_name, progress_callback=None, shared_driver=None):
        """단일 은행 데이터 스크래핑 - 날짜 정보도 반환

        Args:
            shared_driver: 외부에서 전달받은 드라이버 (재사용). None이면 자체 생성/소멸.
        """
        own_driver = shared_driver is None
        driver = shared_driver
        date_info = "날짜 정보 없음"

        try:
            if own_driver:
                driver = create_driver()
            self.logger.log_message(f"[시작] {bank_name} 은행 스크래핑")

            if not self.select_bank(driver, bank_name):
                self.logger.log_message(f"{bank_name} 선택 실패")
                return None, False, date_info

            date_info = self.extract_date_information(driver)
            self.logger.log_message(f"{bank_name} 공시일: {date_info}")

            result_data = {'날짜정보': date_info}

            for category in self.config.CATEGORIES:
                if progress_callback:
                    progress_callback(bank_name, f"{category} 처리 중")

                if self.select_category(driver, category):
                    tables = self.extract_tables_from_page(driver)
                    if tables:
                        result_data[category] = tables
                        self.logger.log_message(f"{bank_name} - {category}: {len(tables)}개 테이블")

            # Excel 파일 저장
            if len(result_data) > 1:
                scrape_type_name = "분기공시" if self.config.scrape_type == "quarterly" else "결산공시"
                # 파일명에 날짜 정보 포함
                safe_date = date_info.replace('/', '_').replace(' ', '')
                filename = f"{bank_name}_{scrape_type_name}_{safe_date}.xlsx"
                filepath = os.path.join(self.config.output_dir, filename)

                with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                    # 날짜 정보 시트
                    pd.DataFrame({
                        '은행명': [bank_name],
                        '공시일': [date_info],
                        '스크래핑일시': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
                    }).to_excel(writer, sheet_name='정보', index=False)

                    for category, tables in result_data.items():
                        if category == '날짜정보':
                            continue
                        for idx, df in enumerate(tables):
                            sheet_name = f"{category}_{idx+1}" if len(tables) > 1 else category
                            sheet_name = sheet_name[:31]  # Excel 시트명 길이 제한
                            df.to_excel(writer, sheet_name=sheet_name, index=False)

                self.logger.log_message(f"[완료] {bank_name} 저장완료")
                return filepath, True, date_info

            return None, False, date_info

        except Exception as e:
            self.logger.log_message(f"{bank_name} 스크래핑 오류: {str(e)}")
            return None, False, date_info
        finally:
            if own_driver and driver:
                try:
                    driver.quit()
                except:
                    pass

    def scrape_multiple_banks(self, banks, progress_callback=None):
        """여러 은행 스크래핑 - 드라이버 1회 생성으로 재사용"""
        results = []
        total = len(banks)
        driver = None

        try:
            driver = create_driver()

            for idx, bank in enumerate(banks):
                if progress_callback:
                    progress_callback(bank, f"처리 중 ({idx+1}/{total})")

                filepath, success, date_info = self.scrape_bank(
                    bank, progress_callback, shared_driver=driver
                )
                results.append({
                    'bank': bank,
                    'success': success,
                    'filepath': filepath,
                    'date_info': date_info
                })

                if progress_callback:
                    status = "완료" if success else "실패"
                    progress_callback(bank, status)

                # 은행 간 딜레이 (축소)
                WaitUtils.wait_with_random(0.3, 0.7)

        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass

        return results

    def create_zip_archive(self, results, custom_filename=None):
        """결과 파일들을 ZIP으로 압축"""
        successful_files = [r['filepath'] for r in results if r['success'] and r['filepath']]

        if not successful_files:
            return None

        # 파일명 설정
        if custom_filename:
            zip_filename = f"{custom_filename}.zip"
        else:
            zip_filename = f"저축은행_{self.config.scrape_type}_{self.config.today}.zip"

        zip_path = os.path.join(self.config.output_dir, zip_filename)

        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for filepath in successful_files:
                if os.path.exists(filepath):
                    zipf.write(filepath, os.path.basename(filepath))

            # 통합 요약 파일 생성
            summary_df = create_summary_dataframe(results)
            summary_path = os.path.join(self.config.output_dir, "스크래핑_요약.xlsx")
            summary_df.to_excel(summary_path, index=False)
            zipf.write(summary_path, "스크래핑_요약.xlsx")

        return zip_path


def create_summary_dataframe(results, bank_dates=None):
    """스크래핑 결과 요약 DataFrame 생성 - 공시날짜 포함"""
    summary_data = []
    for r in results:
        # 날짜 정보 가져오기
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
