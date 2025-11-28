"""
ChatGPT API를 활용한 저축은행 데이터 엑셀 생성 모듈
버전: 1.0
"""

import os
import json
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any, Optional
import tempfile

try:
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False


class ExcelGeneratorConfig:
    """엑셀 생성기 설정"""

    # 엑셀 컬럼 정의 (첨부 이미지 형식)
    EXCEL_COLUMNS = [
        "No",           # 순번
        "은행명",        # 은행명
        "자산(최근분기)", # 자산
        "이익(최근분기)", # 이익
        "순이익",        # 순이익
        "누자본(최근분기신)", # 누적자본
        "최근분기",      # 최근분기
        "신(최근분기)",   # 신규
        "기자본비",      # 기본자본비율
        "위하여신비"     # 위험가중자산비율
    ]

    # ChatGPT 모델 설정
    MODEL = "gpt-4o-mini"
    MAX_TOKENS = 4000
    TEMPERATURE = 0.1


class ChatGPTExcelGenerator:
    """ChatGPT API를 활용한 엑셀 생성기"""

    def __init__(self, api_key: Optional[str] = None):
        """
        초기화

        Args:
            api_key: OpenAI API 키 (None이면 환경변수에서 가져옴)
        """
        if not OPENAI_AVAILABLE:
            raise ImportError("openai 패키지가 설치되어 있지 않습니다. pip install openai 실행하세요.")

        self.api_key = api_key or os.environ.get("OPENAI_API_KEY")
        if not self.api_key:
            raise ValueError("OpenAI API 키가 필요합니다. 환경변수 OPENAI_API_KEY를 설정하거나 api_key 매개변수를 전달하세요.")

        self.client = OpenAI(api_key=self.api_key)
        self.config = ExcelGeneratorConfig()

    def extract_financial_data(self, bank_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        ChatGPT를 사용하여 은행 데이터에서 재무 정보 추출

        Args:
            bank_data: 스크래핑된 은행 데이터 (DataFrame 또는 dict)

        Returns:
            추출된 재무 데이터 딕셔너리
        """
        # 데이터를 문자열로 변환
        if isinstance(bank_data, pd.DataFrame):
            data_str = bank_data.to_string()
        elif isinstance(bank_data, dict):
            data_str = json.dumps(bank_data, ensure_ascii=False, indent=2)
        else:
            data_str = str(bank_data)

        prompt = f"""
다음 저축은행 재무 데이터에서 아래 항목들을 추출해주세요.
반드시 JSON 형식으로만 응답하세요.

추출할 항목:
1. 자산(최근분기) - 총자산 금액 (단위: 억원)
2. 이익(최근분기) - 당기순이익 또는 영업이익 (단위: 억원)
3. 순이익 - 순이익 금액 (단위: 억원)
4. 누자본(최근분기신) - 자기자본 또는 납입자본 (단위: 억원)
5. 최근분기 - 최근 분기 기준 수치
6. 신(최근분기) - 신규 여신 금액 (단위: 억원)
7. 기자본비 - BIS자기자본비율 또는 자기자본비율 (%)
8. 위하여신비 - 위험가중자산 대비 여신비율 또는 고정이하여신비율 (%)

데이터:
{data_str}

JSON 형식 예시:
{{
    "자산": 12345,
    "이익": 100,
    "순이익": 80,
    "누자본": 5000,
    "최근분기": 10000,
    "신": 8000,
    "기자본비": 15.5,
    "위하여신비": 2.3
}}

숫자만 반환하고, 찾을 수 없는 항목은 null로 표시하세요.
"""

        try:
            response = self.client.chat.completions.create(
                model=self.config.MODEL,
                messages=[
                    {"role": "system", "content": "당신은 금융 데이터 분석 전문가입니다. 정확하게 데이터를 추출하고 JSON 형식으로만 응답합니다."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=self.config.MAX_TOKENS,
                temperature=self.config.TEMPERATURE
            )

            result_text = response.choices[0].message.content.strip()

            # JSON 추출 (코드 블록 제거)
            if "```json" in result_text:
                result_text = result_text.split("```json")[1].split("```")[0].strip()
            elif "```" in result_text:
                result_text = result_text.split("```")[1].split("```")[0].strip()

            return json.loads(result_text)

        except Exception as e:
            print(f"ChatGPT API 호출 오류: {e}")
            return {}

    def analyze_and_format_data(self, scraped_results: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        스크래핑된 결과를 분석하고 엑셀 형식으로 정리

        Args:
            scraped_results: 스크래핑 결과 리스트

        Returns:
            정리된 DataFrame
        """
        formatted_data = []

        for idx, result in enumerate(scraped_results, start=1):
            if not result.get('success'):
                continue

            bank_name = result.get('bank', '알 수 없음')
            filepath = result.get('filepath')

            # 엑셀 파일에서 데이터 읽기
            bank_data = self._read_excel_data(filepath) if filepath else {}

            # ChatGPT로 데이터 추출
            if bank_data:
                extracted = self.extract_financial_data(bank_data)
            else:
                extracted = {}

            # 데이터 행 구성
            row = {
                "No": idx,
                "은행명": bank_name,
                "자산(최근분기)": extracted.get("자산", ""),
                "이익(최근분기)": extracted.get("이익", ""),
                "순이익": extracted.get("순이익", ""),
                "누자본(최근분기신)": extracted.get("누자본", ""),
                "최근분기": extracted.get("최근분기", ""),
                "신(최근분기)": extracted.get("신", ""),
                "기자본비": extracted.get("기자본비", ""),
                "위하여신비": extracted.get("위하여신비", "")
            }
            formatted_data.append(row)

        return pd.DataFrame(formatted_data, columns=self.config.EXCEL_COLUMNS)

    def _read_excel_data(self, filepath: str) -> Dict[str, Any]:
        """
        엑셀 파일에서 데이터 읽기

        Args:
            filepath: 엑셀 파일 경로

        Returns:
            데이터 딕셔너리
        """
        if not filepath or not os.path.exists(filepath):
            return {}

        try:
            data = {}
            xl = pd.ExcelFile(filepath)

            for sheet_name in xl.sheet_names:
                df = xl.parse(sheet_name)
                data[sheet_name] = df.to_dict()

            return data
        except Exception as e:
            print(f"엑셀 파일 읽기 오류: {e}")
            return {}

    def generate_summary_excel(
        self,
        scraped_results: List[Dict[str, Any]],
        output_path: Optional[str] = None
    ) -> str:
        """
        스크래핑 결과를 요약 엑셀로 생성

        Args:
            scraped_results: 스크래핑 결과 리스트
            output_path: 출력 파일 경로 (None이면 임시 파일)

        Returns:
            생성된 엑셀 파일 경로
        """
        # 데이터 분석 및 포맷팅
        df = self.analyze_and_format_data(scraped_results)

        # 출력 경로 설정
        if output_path is None:
            output_path = os.path.join(
                tempfile.gettempdir(),
                f"저축은행_분기총괄_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            )

        # 엑셀 파일 생성
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='분기총괄', index=False)

            # 워크시트 스타일링
            worksheet = writer.sheets['분기총괄']

            # 컬럼 너비 자동 조정
            for idx, col in enumerate(df.columns):
                max_length = max(
                    df[col].astype(str).map(len).max(),
                    len(col)
                ) + 2
                worksheet.column_dimensions[chr(65 + idx)].width = min(max_length, 20)

        return output_path

    def process_with_ai_instructions(
        self,
        data: pd.DataFrame,
        instructions: str
    ) -> pd.DataFrame:
        """
        AI 지시에 따라 데이터 처리

        Args:
            data: 처리할 DataFrame
            instructions: 처리 지시사항

        Returns:
            처리된 DataFrame
        """
        data_str = data.to_string()

        prompt = f"""
다음 저축은행 데이터를 아래 지시사항에 따라 처리해주세요.

지시사항:
{instructions}

데이터:
{data_str}

처리된 결과를 JSON 배열 형식으로 반환해주세요.
각 행은 다음 컬럼을 포함해야 합니다: {', '.join(self.config.EXCEL_COLUMNS)}
"""

        try:
            response = self.client.chat.completions.create(
                model=self.config.MODEL,
                messages=[
                    {"role": "system", "content": "당신은 금융 데이터 분석 전문가입니다. 지시에 따라 데이터를 정확하게 처리합니다."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=self.config.MAX_TOKENS,
                temperature=self.config.TEMPERATURE
            )

            result_text = response.choices[0].message.content.strip()

            # JSON 추출
            if "```json" in result_text:
                result_text = result_text.split("```json")[1].split("```")[0].strip()
            elif "```" in result_text:
                result_text = result_text.split("```")[1].split("```")[0].strip()

            result_data = json.loads(result_text)
            return pd.DataFrame(result_data)

        except Exception as e:
            print(f"AI 처리 오류: {e}")
            return data


class DirectExcelGenerator:
    """직접 데이터 입력을 통한 엑셀 생성기 (ChatGPT 없이 사용 가능)"""

    def __init__(self):
        self.config = ExcelGeneratorConfig()

    def create_from_scraped_data(
        self,
        scraped_results: List[Dict[str, Any]],
        output_path: Optional[str] = None
    ) -> str:
        """
        스크래핑 데이터로부터 직접 엑셀 생성

        Args:
            scraped_results: 스크래핑 결과 리스트
            output_path: 출력 파일 경로

        Returns:
            생성된 파일 경로
        """
        formatted_data = []

        for idx, result in enumerate(scraped_results, start=1):
            if not result.get('success'):
                continue

            bank_name = result.get('bank', '')
            filepath = result.get('filepath')

            # 파일에서 데이터 추출
            financial_data = self._extract_from_file(filepath) if filepath else {}

            row = {
                "No": idx,
                "은행명": bank_name,
                "자산(최근분기)": financial_data.get("자산", ""),
                "이익(최근분기)": financial_data.get("이익", ""),
                "순이익": financial_data.get("순이익", ""),
                "누자본(최근분기신)": financial_data.get("누자본", ""),
                "최근분기": financial_data.get("최근분기", ""),
                "신(최근분기)": financial_data.get("신", ""),
                "기자본비": financial_data.get("기자본비", ""),
                "위하여신비": financial_data.get("위하여신비", "")
            }
            formatted_data.append(row)

        df = pd.DataFrame(formatted_data, columns=self.config.EXCEL_COLUMNS)

        # 출력 경로 설정
        if output_path is None:
            output_path = os.path.join(
                tempfile.gettempdir(),
                f"저축은행_분기총괄_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            )

        # 엑셀 저장
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='분기총괄', index=False)

            worksheet = writer.sheets['분기총괄']
            for idx, col in enumerate(df.columns):
                max_length = max(
                    df[col].astype(str).map(len).max(),
                    len(col)
                ) + 2
                worksheet.column_dimensions[chr(65 + idx)].width = min(max_length, 20)

        return output_path

    def _extract_from_file(self, filepath: str) -> Dict[str, Any]:
        """엑셀 파일에서 재무 데이터 추출"""
        if not filepath or not os.path.exists(filepath):
            return {}

        try:
            financial_data = {}
            xl = pd.ExcelFile(filepath)

            for sheet_name in xl.sheet_names:
                df = xl.parse(sheet_name)

                # 재무현황 시트에서 데이터 추출
                if '재무' in sheet_name:
                    financial_data.update(self._parse_financial_sheet(df))

                # 손익현황 시트에서 데이터 추출
                if '손익' in sheet_name:
                    financial_data.update(self._parse_income_sheet(df))

                # 영업개황 시트에서 데이터 추출
                if '영업' in sheet_name:
                    financial_data.update(self._parse_business_sheet(df))

            return financial_data

        except Exception as e:
            print(f"파일 추출 오류: {e}")
            return {}

    def _parse_financial_sheet(self, df: pd.DataFrame) -> Dict[str, Any]:
        """재무현황 시트 파싱"""
        result = {}

        try:
            df_str = df.to_string()

            # 총자산 찾기
            if '총자산' in df_str or '자산총계' in df_str:
                for _, row in df.iterrows():
                    row_str = str(row.values)
                    if '총자산' in row_str or '자산총계' in row_str:
                        for val in row.values:
                            if isinstance(val, (int, float)) and val > 0:
                                result['자산'] = val
                                break

            # 자기자본 찾기
            if '자기자본' in df_str or '자본총계' in df_str:
                for _, row in df.iterrows():
                    row_str = str(row.values)
                    if '자기자본' in row_str or '자본총계' in row_str:
                        for val in row.values:
                            if isinstance(val, (int, float)) and val > 0:
                                result['누자본'] = val
                                break

            # BIS비율 찾기
            if 'BIS' in df_str or '자본비율' in df_str:
                for _, row in df.iterrows():
                    row_str = str(row.values)
                    if 'BIS' in row_str or '자본비율' in row_str:
                        for val in row.values:
                            if isinstance(val, (int, float)) and 0 < val < 100:
                                result['기자본비'] = val
                                break
        except:
            pass

        return result

    def _parse_income_sheet(self, df: pd.DataFrame) -> Dict[str, Any]:
        """손익현황 시트 파싱"""
        result = {}

        try:
            df_str = df.to_string()

            # 당기순이익 찾기
            if '당기순이익' in df_str or '순이익' in df_str:
                for _, row in df.iterrows():
                    row_str = str(row.values)
                    if '당기순이익' in row_str or '순이익' in row_str:
                        for val in row.values:
                            if isinstance(val, (int, float)):
                                result['순이익'] = val
                                result['이익'] = val
                                break
        except:
            pass

        return result

    def _parse_business_sheet(self, df: pd.DataFrame) -> Dict[str, Any]:
        """영업개황 시트 파싱"""
        result = {}

        try:
            df_str = df.to_string()

            # 여신 찾기
            if '여신' in df_str:
                for _, row in df.iterrows():
                    row_str = str(row.values)
                    if '여신' in row_str and '고정이하' not in row_str:
                        for val in row.values:
                            if isinstance(val, (int, float)) and val > 0:
                                result['신'] = val
                                result['최근분기'] = val
                                break

            # 고정이하여신비율 찾기
            if '고정이하' in df_str or '연체' in df_str:
                for _, row in df.iterrows():
                    row_str = str(row.values)
                    if '고정이하' in row_str or '연체율' in row_str:
                        for val in row.values:
                            if isinstance(val, (int, float)) and 0 <= val < 100:
                                result['위하여신비'] = val
                                break
        except:
            pass

        return result


def generate_excel_with_chatgpt(
    scraped_results: List[Dict[str, Any]],
    api_key: Optional[str] = None,
    output_path: Optional[str] = None,
    use_ai: bool = True
) -> str:
    """
    편의 함수: 스크래핑 결과로 엑셀 생성

    Args:
        scraped_results: 스크래핑 결과 리스트
        api_key: OpenAI API 키
        output_path: 출력 파일 경로
        use_ai: ChatGPT 사용 여부

    Returns:
        생성된 엑셀 파일 경로
    """
    if use_ai and OPENAI_AVAILABLE and api_key:
        generator = ChatGPTExcelGenerator(api_key=api_key)
        return generator.generate_summary_excel(scraped_results, output_path)
    else:
        generator = DirectExcelGenerator()
        return generator.create_from_scraped_data(scraped_results, output_path)
