"""
저축은행 데이터 엑셀 생성 모듈
버전: 5.0 - 엑셀 작업은 ChatGPT API, PDF 추출은 Gemini API 분리 구조
"""

import os
import json
import time
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any, Optional
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

# 하위 호환성: Gemini 여부는 pdf_delinquency_extractor에서만 사용
try:
    from google import genai
    from google.genai import types
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False


class ExcelGeneratorConfig:
    """엑셀 생성기 설정"""

    EXCEL_COLUMNS = [
        "No",
        "회사명",
        "금분기(금기)일시",
        "총자산_전년동기(전기)",
        "총자산_금분기(금기)",
        "당기순이익_전년동기(전기)",
        "당기순이익_금분기(금기)",
        "자기자본_전년동기(전기)",
        "자기자본_금분기(금기)",
        "총여신_전년동기(전기)",
        "총여신_금분기(금기)",
        "총수신_전년동기(전기)",
        "총수신_금분기(금기)",
        "BIS비율_전년동기(전기)",
        "BIS비율_금분기(금기)",
        "고정이하여신비율_전년동기(전기)",
        "고정이하여신비율_금분기(금기)",
        "연체율_전년동기(전기)",
        "연체율_금분기(금기)",
    ]

    AMOUNT_COLUMNS = [
        "총자산_전년동기(전기)", "총자산_금분기(금기)",
        "당기순이익_전년동기(전기)", "당기순이익_금분기(금기)",
        "자기자본_전년동기(전기)", "자기자본_금분기(금기)",
        "총여신_전년동기(전기)", "총여신_금분기(금기)",
        "총수신_전년동기(전기)", "총수신_금분기(금기)",
    ]

    RATIO_COLUMNS = [
        "BIS비율_전년동기(전기)", "BIS비율_금분기(금기)",
        "고정이하여신비율_전년동기(전기)", "고정이하여신비율_금분기(금기)",
        "연체율_전년동기(전기)", "연체율_금분기(금기)",
    ]

    # ChatGPT API 설정 (엑셀 작업용)
    OPENAI_MODEL = "gpt-5.2"
    MAX_TOKENS = 4000
    TEMPERATURE = 0.2


def _get_column_letter(idx):
    """0-based 인덱스를 엑셀 컬럼 문자로 변환"""
    result = ""
    while True:
        result = chr(65 + idx % 26) + result
        idx = idx // 26 - 1
        if idx < 0:
            break
    return result


class ChatGPTExcelGenerator:
    """ChatGPT API를 활용한 엑셀 생성기"""

    def __init__(self, api_key=None, log_callback=None):
        if not OPENAI_AVAILABLE:
            raise ImportError("openai 패키지가 설치되어 있지 않습니다. pip install openai")
        self.api_key = api_key or os.environ.get("OPENAI_API_KEY")
        if not self.api_key:
            raise ValueError("OpenAI API 키가 필요합니다.")
        self.client = OpenAI(api_key=self.api_key)
        self.config = ExcelGeneratorConfig()
        self._excel_cache = {}  # filepath → data 캐시
        self._log = log_callback or (lambda msg: None)

    def _chat_completion(self, system_msg, user_msg):
        """ChatGPT API 공통 호출 헬퍼"""
        response = self.client.chat.completions.create(
            model=self.config.OPENAI_MODEL,
            messages=[
                {"role": "system", "content": system_msg},
                {"role": "user", "content": user_msg},
            ],
            temperature=self.config.TEMPERATURE,
            max_completion_tokens=self.config.MAX_TOKENS,
            response_format={"type": "json_object"},
        )
        result_text = response.choices[0].message.content.strip()
        if "```json" in result_text:
            result_text = result_text.split("```json")[1].split("```")[0].strip()
        elif "```" in result_text:
            result_text = result_text.split("```")[1].split("```")[0].strip()
        return json.loads(result_text)

    def extract_financial_data(self, bank_data):
        if isinstance(bank_data, pd.DataFrame):
            data_str = bank_data.to_string()
        elif isinstance(bank_data, dict):
            data_str = json.dumps(bank_data, ensure_ascii=False, indent=2)
        else:
            data_str = str(bank_data)

        prompt = (
            "다음 저축은행 재무 데이터에서 아래 항목들을 추출해주세요.\n"
            "각 항목에 대해 **전년동기(전기)** 값과 **금분기(금기=당기)** 값을 모두 추출해야 합니다.\n"
            "반드시 JSON 형식으로만 응답하세요.\n\n"
            "추출할 항목:\n"
            "1. 총자산 - 전년동기(전기) / 금분기(금기)\n"
            "2. 당기순이익 - 전년동기(전기) / 금분기(금기)\n"
            "3. 자기자본(자본총계) - 전년동기(전기) / 금분기(금기)\n"
            "4. 총여신 - 전년동기(전기) / 금분기(금기)\n"
            "5. 총수신(예금, 수신 합계) - 전년동기(전기) / 금분기(금기)\n"
            "6. BIS비율(위험가중자산에 대한 자기자본비율) - 전년동기(전기) / 금분기(금기) (%)\n"
            "7. 고정이하여신비율 - 전년동기(전기) / 금분기(금기) (%)\n"
            "8. 연체율 - 전년동기(전기) / 금분기(금기) (%)\n\n"
            f"데이터:\n{data_str}\n\n"
            'JSON 형식 예시:\n'
            '{\n'
            '  "총자산_전기": 12345, "총자산_당기": 13000,\n'
            '  "당기순이익_전기": 80, "당기순이익_당기": 100,\n'
            '  "자기자본_전기": 4500, "자기자본_당기": 5000,\n'
            '  "총여신_전기": 7500, "총여신_당기": 8000,\n'
            '  "총수신_전기": 9000, "총수신_당기": 9500,\n'
            '  "BIS비율_전기": 14.5, "BIS비율_당기": 15.5,\n'
            '  "고정이하여신비율_전기": 2.5, "고정이하여신비율_당기": 2.3,\n'
            '  "연체율_전기": 3.1, "연체율_당기": 2.8\n'
            '}\n\n'
            "숫자만 반환하고, 찾을 수 없는 항목은 null로 표시하세요."
        )

        try:
            result = self._chat_completion(
                "당신은 금융 데이터 분석 전문가입니다. 정확하게 데이터를 추출하고 JSON 형식으로만 응답합니다.",
                prompt,
            )
            filled = sum(1 for v in result.values() if v is not None)
            self._log(f"  ✅ ChatGPT 추출 완료 ({filled}/16 항목)")
            return result
        except Exception as e:
            self._log(f"  ❌ ChatGPT API 오류: {e}")
            return {}

    def analyze_and_format_data(self, scraped_results):
        # 1. 성공한 은행만 필터링 & 파일 데이터 사전 읽기 (캐시)
        valid_items = []
        for idx, result in enumerate(scraped_results, start=1):
            if not result.get('success'):
                continue
            bank_name = result.get('bank', '알 수 없음')
            filepath = result.get('filepath')
            date_info = result.get('date_info', '')
            bank_data = self._read_excel_data(filepath) if filepath else {}
            valid_items.append((idx, bank_name, filepath, date_info, bank_data))

        total = len(valid_items)
        self._log(f"[3-1] ChatGPT 재무 데이터 추출 시작 ({total}개 은행)")

        # 2. ChatGPT API 병렬 호출 (은행별 순차 → 최대 5개 동시)
        extracted_map = {}
        completed_count = 0

        def _extract_single(item):
            idx, bank_name, filepath, date_info, bank_data = item
            self._log(f"  [{idx}/{total}] {bank_name} — ChatGPT 호출 중...")
            extracted = self.extract_financial_data(bank_data) if bank_data else {}
            return (idx, bank_name, filepath, date_info, extracted)

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(_extract_single, item): item for item in valid_items}
            for future in as_completed(futures):
                try:
                    idx, bank_name, filepath, date_info, extracted = future.result()
                    extracted_map[idx] = (bank_name, filepath, date_info, extracted)
                    completed_count += 1
                except Exception as e:
                    item = futures[future]
                    extracted_map[item[0]] = (item[1], item[2], item[3], {})
                    completed_count += 1
                    self._log(f"  ❌ {item[1]} 추출 실패: {e}")

        self._log(f"[3-1 완료] ChatGPT 추출 완료 ({completed_count}/{total}개)")

        # 3. DirectExcelGenerator fallback (누락 항목 보완)
        self._log("[3-2] 누락 데이터 보완 (DirectExcelGenerator fallback)")
        formatted_data = []
        fallback_gen = None
        fallback_count = 0
        scrape_keys = [
            "총자산_전기", "총자산_당기", "당기순이익_전기", "당기순이익_당기",
            "자기자본_전기", "자기자본_당기", "총여신_전기", "총여신_당기",
            "총수신_전기", "총수신_당기", "BIS비율_전기", "BIS비율_당기",
            "고정이하여신비율_전기", "고정이하여신비율_당기",
        ]

        for idx in sorted(extracted_map.keys()):
            bank_name, filepath, date_info, extracted = extracted_map[idx]

            key_fields = ["총자산_당기", "당기순이익_당기", "자기자본_당기"]
            has_key_data = any(
                extracted.get(k) is not None and extracted.get(k) != ""
                for k in key_fields
            )
            if filepath:
                has_missing = not has_key_data or any(
                    extracted.get(k) is None or extracted.get(k) == ""
                    for k in scrape_keys
                )
                if has_missing:
                    if fallback_gen is None:
                        fallback_gen = DirectExcelGenerator()
                    fallback_data = fallback_gen._extract_from_file(filepath)
                    if fallback_data:
                        patched = 0
                        for k, v in fallback_data.items():
                            if v is not None and (extracted.get(k) is None or extracted.get(k) == ""):
                                extracted[k] = v
                                patched += 1
                        if patched > 0:
                            self._log(f"  {bank_name}: fallback으로 {patched}개 항목 보완")
                            fallback_count += 1

            row = self._build_row(idx, bank_name, date_info, extracted)
            formatted_data.append(row)

        if fallback_count > 0:
            self._log(f"[3-2 완료] {fallback_count}개 은행 fallback 보완 적용")
        else:
            self._log("[3-2 완료] fallback 불필요 — AI 추출 데이터 완전")
        return pd.DataFrame(formatted_data, columns=self.config.EXCEL_COLUMNS)

    @staticmethod
    def _build_row(idx, bank_name, date_info, extracted):
        return {
            "No": idx,
            "회사명": bank_name,
            "금분기(금기)일시": date_info or "",
            "총자산_전년동기(전기)": extracted.get("총자산_전기", ""),
            "총자산_금분기(금기)": extracted.get("총자산_당기", ""),
            "당기순이익_전년동기(전기)": extracted.get("당기순이익_전기", ""),
            "당기순이익_금분기(금기)": extracted.get("당기순이익_당기", ""),
            "자기자본_전년동기(전기)": extracted.get("자기자본_전기", ""),
            "자기자본_금분기(금기)": extracted.get("자기자본_당기", ""),
            "총여신_전년동기(전기)": extracted.get("총여신_전기", ""),
            "총여신_금분기(금기)": extracted.get("총여신_당기", ""),
            "총수신_전년동기(전기)": extracted.get("총수신_전기", ""),
            "총수신_금분기(금기)": extracted.get("총수신_당기", ""),
            "BIS비율_전년동기(전기)": extracted.get("BIS비율_전기", ""),
            "BIS비율_금분기(금기)": extracted.get("BIS비율_당기", ""),
            "고정이하여신비율_전년동기(전기)": extracted.get("고정이하여신비율_전기", ""),
            "고정이하여신비율_금분기(금기)": extracted.get("고정이하여신비율_당기", ""),
            "연체율_전년동기(전기)": extracted.get("연체율_전기", ""),
            "연체율_금분기(금기)": extracted.get("연체율_당기", ""),
        }

    def _read_excel_data(self, filepath):
        if not filepath or not os.path.exists(filepath):
            return {}
        if filepath in self._excel_cache:
            return self._excel_cache[filepath]
        try:
            data = {}
            xl = pd.ExcelFile(filepath)
            for sheet_name in xl.sheet_names:
                df = xl.parse(sheet_name)
                data[sheet_name] = df.to_dict()
            self._excel_cache[filepath] = data
            return data
        except Exception as e:
            print(f"엑셀 파일 읽기 오류: {e}")
            return {}

    def generate_summary_excel(self, scraped_results, output_path=None, validate=True,
                               early_path_callback=None):
        t0 = time.time()
        df = self.analyze_and_format_data(scraped_results)
        t1 = time.time()
        self._log(f"[3-2 타이밍] AI 데이터 추출: {t1 - t0:.1f}초")

        if output_path is None:
            output_path = os.path.join(
                tempfile.gettempdir(),
                f"저축은행_분기총괄_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            )

        # 먼저 데이터만으로 엑셀 저장 (Merge 등 외부에서 즉시 사용 가능)
        self._log("[3-3] 엑셀 파일 저장 중...")
        _write_styled_excel(df, output_path, None)
        if early_path_callback:
            early_path_callback(output_path)
        self._log("[3-3 완료] 엑셀 파일 저장 완료")

        validation_result = None
        if validate:
            self._log("[3-4] ChatGPT 정합성 검증 시작...")
            t2 = time.time()
            validation_result = self.validate_excel_data(df, scraped_results)
            t3 = time.time()
            self._log(f"[3-4 완료] 정합성 검증 완료 ({t3 - t2:.1f}초)")
            # 검증 결과 시트를 포함하여 재저장
            _write_styled_excel(df, output_path, validation_result)

        self._log(f"[3단계 타이밍] 엑셀 생성 전체: {time.time() - t0:.1f}초")
        return {"filepath": output_path, "validation": validation_result}

    def validate_excel_data(self, df, scraped_results):
        local_result = self._validate_local_rules(df)
        ai_result = self._validate_with_ai(df, scraped_results)
        all_errors = local_result.get("errors", []) + ai_result.get("errors", [])
        all_warnings = local_result.get("warnings", []) + ai_result.get("warnings", [])
        error_penalty = len(all_errors) * 10
        warning_penalty = len(all_warnings) * 3
        score = max(0, min(100, 100 - error_penalty - warning_penalty))
        return {
            "is_valid": len(all_errors) == 0,
            "score": score,
            "errors": all_errors,
            "warnings": all_warnings,
            "details": ai_result.get("details", {}),
            "local_checks": local_result,
            "ai_checks": ai_result,
        }

    def _validate_local_rules(self, df):
        errors = []
        warnings = []
        expected_cols = set(self.config.EXCEL_COLUMNS)
        actual_cols = set(df.columns.tolist())
        missing = expected_cols - actual_cols
        if missing:
            errors.append(f"누락된 컬럼: {', '.join(missing)}")
        if df.empty:
            errors.append("데이터가 비어 있습니다.")
            return {"errors": errors, "warnings": warnings}

        if "No" in df.columns:
            no_values = df["No"].dropna().tolist()
            if no_values != list(range(1, len(no_values) + 1)):
                warnings.append(f"순번(No)이 연속적이지 않습니다: {no_values}")

        if "회사명" in df.columns:
            dups = df["회사명"].dropna()
            dup_names = dups[dups.duplicated()].tolist()
            if dup_names:
                errors.append(f"중복된 회사명: {', '.join(dup_names)}")

        positive_cols = [
            "총자산_전년동기(전기)", "총자산_금분기(금기)",
            "자기자본_전년동기(전기)", "자기자본_금분기(금기)",
        ]
        for col in positive_cols:
            if col not in df.columns:
                continue
            for ri, val in df[col].items():
                bname = df.at[ri, "회사명"] if "회사명" in df.columns else f"행 {ri}"
                if val == "" or val is None or (isinstance(val, float) and pd.isna(val)):
                    warnings.append(f"{bname}: '{col}' 값이 비어 있습니다.")
                elif isinstance(val, (int, float)) and val < 0:
                    errors.append(f"{bname}: '{col}' 값이 음수입니다 ({val}).")

        other_amt = [c for c in self.config.AMOUNT_COLUMNS if c not in positive_cols]
        for col in other_amt:
            if col not in df.columns:
                continue
            for ri, val in df[col].items():
                if val == "" or val is None or (isinstance(val, float) and pd.isna(val)):
                    bname = df.at[ri, "회사명"] if "회사명" in df.columns else f"행 {ri}"
                    warnings.append(f"{bname}: '{col}' 값이 비어 있습니다.")

        for col in self.config.RATIO_COLUMNS:
            if col not in df.columns:
                continue
            for ri, val in df[col].items():
                bname = df.at[ri, "회사명"] if "회사명" in df.columns else f"행 {ri}"
                if val == "" or val is None or (isinstance(val, float) and pd.isna(val)):
                    warnings.append(f"{bname}: '{col}' 값이 비어 있습니다.")
                elif isinstance(val, (int, float)) and not pd.isna(val):
                    if val < 0 or val > 100:
                        errors.append(f"{bname}: '{col}' 비율값이 범위(0~100%)를 벗어났습니다 ({val}%).")

        data_cols = self.config.AMOUNT_COLUMNS + self.config.RATIO_COLUMNS
        existing = [c for c in data_cols if c in df.columns]
        if existing:
            empty_count = sum(
                1 for col in existing for val in df[col]
                if val == "" or val is None or (isinstance(val, float) and pd.isna(val))
            )
            total_count = len(existing) * len(df)
            if total_count > 0:
                ratio = empty_count / total_count * 100
                if ratio > 50:
                    warnings.append(f"전체 데이터의 {ratio:.1f}%가 비어 있습니다. 원본 데이터를 확인하세요.")

        return {"errors": errors, "warnings": warnings}

    def _validate_with_ai(self, df, scraped_results):
        errors = []
        warnings = []
        details = {}
        source_summaries = []
        self._log("  원본 데이터 로드 중...")
        for result in scraped_results:
            if not result.get('success'):
                continue
            bank_name = result.get('bank', '알 수 없음')
            filepath = result.get('filepath')
            if filepath:
                bank_data = self._read_excel_data(filepath)
                if bank_data:
                    source_summaries.append({
                        "bank": bank_name,
                        "source_data": json.dumps(bank_data, ensure_ascii=False, default=str)[:2000]
                    })
        if not source_summaries:
            self._log("  ⚠️ 원본 데이터 없음 — AI 검증 건너뜀")
            warnings.append("원본 데이터를 읽을 수 없어 AI 교차 검증을 건너뜁니다.")
            return {"errors": errors, "warnings": warnings, "details": details}

        self._log(f"  ChatGPT 교차 검증 호출 중... ({len(source_summaries)}개 은행)")

        generated_data_str = df.to_string()
        prompt = (
            "다음은 저축은행 재무 데이터의 정합성 검증 요청입니다.\n\n"
            f"[생성된 엑셀 데이터]\n{generated_data_str}\n\n"
            f"[원본 스크래핑 데이터 (은행별)]\n"
            f"{json.dumps(source_summaries, ensure_ascii=False, default=str)[:6000]}\n\n"
            "아래 항목을 검증하고 결과를 JSON으로 반환하세요:\n"
            "1. 원본 데이터와 생성된 데이터의 수치 일치 확인\n"
            "2. 각 은행 데이터가 올바른 행에 배치되었는지 확인\n"
            "3. 전년동기(전기)/금분기(금기) 값이 올바른 컬럼에 배치되었는지 확인\n"
            "4. 단위 일관성 확인\n"
            "5. 논리적 모순 확인\n"
            "6. 이상치 확인\n\n"
            'JSON 형식:\n'
            '{\n'
            '  "errors": ["심각한 오류 목록"],\n'
            '  "warnings": ["경고 목록"],\n'
            '  "bank_details": { "은행명": { "status": "pass|warn|fail", "issues": [] } },\n'
            '  "summary": "전체 검증 요약"\n'
            '}'
        )
        try:
            validation = self._chat_completion(
                "당신은 금융 데이터 품질 검증 전문가입니다. 반드시 JSON 형식으로만 응답하세요.",
                prompt,
            )
            errors.extend(validation.get("errors", []))
            warnings.extend(validation.get("warnings", []))
            details = validation.get("bank_details", {})
            self._log(f"  ✅ AI 검증 완료 (오류 {len(errors)}건, 경고 {len(warnings)}건)")
            return {"errors": errors, "warnings": warnings, "details": details, "summary": validation.get("summary", "")}
        except Exception as e:
            self._log(f"  ❌ AI 검증 API 호출 실패: {e}")
            warnings.append(f"AI 검증 API 호출 실패: {str(e)}")
            return {"errors": errors, "warnings": warnings, "details": details}

    def process_with_ai_instructions(self, data, instructions):
        data_str = data.to_string()
        prompt = (
            f"다음 저축은행 데이터를 아래 지시사항에 따라 처리해주세요.\n\n"
            f"지시사항:\n{instructions}\n\n"
            f"데이터:\n{data_str}\n\n"
            f"처리된 결과를 JSON 배열 형식으로 반환해주세요.\n"
            f"각 행은 다음 컬럼을 포함해야 합니다: {', '.join(self.config.EXCEL_COLUMNS)}"
        )
        try:
            result = self._chat_completion(
                "당신은 금융 데이터 분석 전문가입니다.",
                prompt,
            )
            # JSON 객체 안에 배열이 있을 수 있음
            if isinstance(result, dict):
                for v in result.values():
                    if isinstance(v, list):
                        return pd.DataFrame(v)
                return pd.DataFrame([result])
            return pd.DataFrame(result)
        except Exception as e:
            print(f"AI 처리 오류: {e}")
            return data


# 하위 호환성 별칭
GeminiExcelGenerator = ChatGPTExcelGenerator


def _write_styled_excel(df, output_path, validation_result=None):
    """엑셀 파일 작성 및 스타일링"""
    config = ExcelGeneratorConfig()
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='분기총괄', index=False)
        ws = writer.sheets['분기총괄']

        col_widths = {"No": 6, "회사명": 14, "금분기(금기)일시": 16}
        for c in config.AMOUNT_COLUMNS:
            col_widths[c] = 18
        for c in config.RATIO_COLUMNS:
            col_widths[c] = 16

        for i, col in enumerate(df.columns):
            letter = _get_column_letter(i)
            ws.column_dimensions[letter].width = col_widths.get(col, 16)

        if validation_result:
            vd = []
            vd.append({"항목": "정합성 점수", "결과": f"{validation_result['score']}점 / 100점"})
            vd.append({"항목": "전체 판정", "결과": "통과" if validation_result['is_valid'] else "오류 있음"})
            vd.append({"항목": "", "결과": ""})
            if validation_result.get("ai_checks", {}).get("summary"):
                vd.append({"항목": "AI 검증 요약", "결과": validation_result["ai_checks"]["summary"]})
                vd.append({"항목": "", "결과": ""})
            if validation_result["errors"]:
                vd.append({"항목": "=== 오류 ===", "결과": ""})
                for err in validation_result["errors"]:
                    vd.append({"항목": "오류", "결과": err})
            if validation_result["warnings"]:
                vd.append({"항목": "=== 경고 ===", "결과": ""})
                for w in validation_result["warnings"]:
                    vd.append({"항목": "경고", "결과": w})
            if validation_result.get("details"):
                vd.append({"항목": "", "결과": ""})
                vd.append({"항목": "=== 은행별 상세 ===", "결과": ""})
                for bank, detail in validation_result["details"].items():
                    status = detail.get("status", "unknown")
                    issues = ", ".join(detail.get("issues", []))
                    vd.append({"항목": f"{bank} [{status}]", "결과": issues if issues else "이상 없음"})
            pd.DataFrame(vd).to_excel(writer, sheet_name='정합성검증', index=False)
            ws_val = writer.sheets['정합성검증']
            ws_val.column_dimensions['A'].width = 25
            ws_val.column_dimensions['B'].width = 60


class DirectExcelGenerator:
    """직접 데이터 입력을 통한 엑셀 생성기 (AI API 없이 사용 가능)"""

    def __init__(self):
        self.config = ExcelGeneratorConfig()

    def create_from_scraped_data(self, scraped_results, output_path=None):
        formatted_data = []
        for idx, result in enumerate(scraped_results, start=1):
            if not result.get('success'):
                continue
            bank_name = result.get('bank', '')
            filepath = result.get('filepath')
            date_info = result.get('date_info', '')
            financial_data = self._extract_from_file(filepath) if filepath else {}
            row = GeminiExcelGenerator._build_row(idx, bank_name, date_info, financial_data)
            formatted_data.append(row)

        df = pd.DataFrame(formatted_data, columns=self.config.EXCEL_COLUMNS)

        if output_path is None:
            output_path = os.path.join(
                tempfile.gettempdir(),
                f"저축은행_분기총괄_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            )
        _write_styled_excel(df, output_path)
        return output_path

    @staticmethod
    def _to_numeric(val):
        if val is None:
            return None
        if isinstance(val, (int, float)):
            if pd.isna(val):
                return None
            return val
        try:
            cleaned = str(val).replace(',', '').replace(' ', '').strip()
            if cleaned in ('', '-', 'nan', 'None', 'NaN'):
                return None
            return pd.to_numeric(cleaned, errors='coerce')
        except Exception:
            return None

    def _identify_period_columns(self, df):
        current_cols = []
        previous_cols = []
        other_cols = []
        for col in df.columns:
            col_str = str(col).strip()
            if any(kw in col_str for kw in ['당기', '현재', '이번', '당분기']):
                current_cols.append(col)
            elif any(kw in col_str for kw in ['전년', '작년', '이전', '전기', '전분기']):
                previous_cols.append(col)
            else:
                other_cols.append(col)
        return current_cols, previous_cols, other_cols

    def _find_both_period_values(self, df, row_idx, label_col, current_cols, previous_cols):
        """행에서 전년동기(전기)와 금분기(당기) 값 모두 추출

        Returns:
            (전기값, 당기값) 튜플
        """
        row = df.iloc[row_idx]
        prev_val = None
        curr_val = None

        if previous_cols:
            for col in previous_cols:
                if col == label_col:
                    continue
                val = self._to_numeric(row[col])
                if val is not None and pd.notna(val):
                    prev_val = val
                    break

        if current_cols:
            for col in current_cols:
                if col == label_col:
                    continue
                val = self._to_numeric(row[col])
                if val is not None and pd.notna(val):
                    curr_val = val
                    break

        if prev_val is None and curr_val is None:
            found_label = False
            numeric_values = []
            for col in df.columns:
                if col == label_col:
                    found_label = True
                    continue
                if not found_label:
                    continue
                val = self._to_numeric(row[col])
                if val is not None and pd.notna(val):
                    numeric_values.append(val)
                    if len(numeric_values) >= 2:
                        break
            if len(numeric_values) >= 2:
                prev_val = numeric_values[0]
                curr_val = numeric_values[1]
            elif len(numeric_values) == 1:
                curr_val = numeric_values[0]

        return prev_val, curr_val

    def _extract_from_file(self, filepath):
        if not filepath or not os.path.exists(filepath):
            return {}
        try:
            financial_data = {}
            xl = pd.ExcelFile(filepath)
            for sheet_name in xl.sheet_names:
                df = xl.parse(sheet_name)
                if df.empty:
                    continue
                current_cols, previous_cols, _ = self._identify_period_columns(df)
                if '재무' in sheet_name:
                    financial_data.update(self._parse_financial_sheet(df, current_cols, previous_cols))
                if '손익' in sheet_name:
                    financial_data.update(self._parse_income_sheet(df, current_cols, previous_cols))
                if '영업' in sheet_name:
                    financial_data.update(self._parse_business_sheet(df, current_cols, previous_cols))
                if '기타' in sheet_name:
                    financial_data.update(self._parse_ratio_sheet(df, current_cols, previous_cols))
            return financial_data
        except Exception as e:
            print(f"파일 추출 오류: {e}")
            return {}

    def _parse_financial_sheet(self, df, current_cols, previous_cols):
        result = {}
        try:
            for row_idx in range(len(df)):
                for col in df.columns:
                    cell_str = str(df.iloc[row_idx][col]).strip()

                    if '총자산_당기' not in result and ('총자산' in cell_str or '자산총계' in cell_str):
                        pv, cv = self._find_both_period_values(df, row_idx, col, current_cols, previous_cols)
                        if cv is not None and cv > 0:
                            result['총자산_당기'] = cv
                        if pv is not None and pv > 0:
                            result['총자산_전기'] = pv

                    if '자기자본_당기' not in result and (
                        '자기자본' in cell_str or '자본총계' in cell_str or '자본합계' in cell_str
                    ) and '자산' not in cell_str:
                        pv, cv = self._find_both_period_values(df, row_idx, col, current_cols, previous_cols)
                        if cv is not None and cv > 0:
                            if result.get('총자산_당기') is None or cv != result['총자산_당기']:
                                result['자기자본_당기'] = cv
                        if pv is not None and pv > 0:
                            if result.get('총자산_전기') is None or pv != result['총자산_전기']:
                                result['자기자본_전기'] = pv

                    if 'BIS비율_당기' not in result and (
                        'BIS' in cell_str or 'bis' in cell_str or
                        '자기자본비율' in cell_str or '위험가중자산에' in cell_str
                    ):
                        pv, cv = self._find_both_period_values(df, row_idx, col, current_cols, previous_cols)
                        if cv is not None and 0 < cv < 100:
                            result['BIS비율_당기'] = cv
                        if pv is not None and 0 < pv < 100:
                            result['BIS비율_전기'] = pv
        except Exception:
            pass
        return result

    def _parse_income_sheet(self, df, current_cols, previous_cols):
        result = {}
        try:
            for row_idx in range(len(df)):
                for col in df.columns:
                    cell_str = str(df.iloc[row_idx][col]).strip()
                    if '당기순이익_당기' not in result and ('당기순이익' in cell_str or '순이익' in cell_str):
                        pv, cv = self._find_both_period_values(df, row_idx, col, current_cols, previous_cols)
                        if cv is not None:
                            result['당기순이익_당기'] = cv
                        if pv is not None:
                            result['당기순이익_전기'] = pv
        except Exception:
            pass
        return result

    def _parse_business_sheet(self, df, current_cols, previous_cols):
        result = {}
        try:
            for row_idx in range(len(df)):
                for col in df.columns:
                    cell_str = str(df.iloc[row_idx][col]).strip()

                    if '총여신_당기' not in result and '여신' in cell_str and '고정이하' not in cell_str and '비율' not in cell_str:
                        pv, cv = self._find_both_period_values(df, row_idx, col, current_cols, previous_cols)
                        if cv is not None and cv > 0:
                            result['총여신_당기'] = cv
                        if pv is not None and pv > 0:
                            result['총여신_전기'] = pv

                    if '총수신_당기' not in result and ('수신' in cell_str or '예금' in cell_str) and '비율' not in cell_str:
                        pv, cv = self._find_both_period_values(df, row_idx, col, current_cols, previous_cols)
                        if cv is not None and cv > 0:
                            result['총수신_당기'] = cv
                        if pv is not None and pv > 0:
                            result['총수신_전기'] = pv

                    if '고정이하여신비율_당기' not in result and ('고정이하여신비율' in cell_str or '고정이하' in cell_str):
                        pv, cv = self._find_both_period_values(df, row_idx, col, current_cols, previous_cols)
                        if cv is not None and 0 <= cv < 100:
                            result['고정이하여신비율_당기'] = cv
                        if pv is not None and 0 <= pv < 100:
                            result['고정이하여신비율_전기'] = pv

                    if '연체율_당기' not in result and '연체' in cell_str and '율' in cell_str:
                        pv, cv = self._find_both_period_values(df, row_idx, col, current_cols, previous_cols)
                        if cv is not None and 0 <= cv < 100:
                            result['연체율_당기'] = cv
                        if pv is not None and 0 <= pv < 100:
                            result['연체율_전기'] = pv
        except Exception:
            pass
        return result

    def _parse_ratio_sheet(self, df, current_cols, previous_cols):
        result = {}
        try:
            for row_idx in range(len(df)):
                for col in df.columns:
                    cell_str = str(df.iloc[row_idx][col]).strip()

                    if 'BIS비율_당기' not in result and (
                        'BIS' in cell_str or 'bis' in cell_str or
                        '자기자본비율' in cell_str or '위험가중자산에' in cell_str
                    ):
                        pv, cv = self._find_both_period_values(df, row_idx, col, current_cols, previous_cols)
                        if cv is not None and 0 < cv < 100:
                            result['BIS비율_당기'] = cv
                        if pv is not None and 0 < pv < 100:
                            result['BIS비율_전기'] = pv

                    if '고정이하여신비율_당기' not in result and ('고정이하여신비율' in cell_str or '고정이하' in cell_str):
                        pv, cv = self._find_both_period_values(df, row_idx, col, current_cols, previous_cols)
                        if cv is not None and 0 <= cv < 100:
                            result['고정이하여신비율_당기'] = cv
                        if pv is not None and 0 <= pv < 100:
                            result['고정이하여신비율_전기'] = pv

                    if '연체율_당기' not in result and '연체' in cell_str and '율' in cell_str:
                        pv, cv = self._find_both_period_values(df, row_idx, col, current_cols, previous_cols)
                        if cv is not None and 0 <= cv < 100:
                            result['연체율_당기'] = cv
                        if pv is not None and 0 <= pv < 100:
                            result['연체율_전기'] = pv
        except Exception:
            pass
        return result


def generate_excel_with_chatgpt(
    scraped_results,
    api_key=None,
    output_path=None,
    use_ai=True,
    validate=True,
    early_path_callback=None,
    log_callback=None,
):
    """편의 함수: 스크래핑 결과로 엑셀 생성 및 정합성 검증 (ChatGPT API)"""
    if use_ai and OPENAI_AVAILABLE and api_key:
        generator = ChatGPTExcelGenerator(api_key=api_key, log_callback=log_callback)
        return generator.generate_summary_excel(
            scraped_results, output_path, validate=validate,
            early_path_callback=early_path_callback,
        )
    else:
        generator = DirectExcelGenerator()
        filepath = generator.create_from_scraped_data(scraped_results, output_path)
        return {"filepath": filepath, "validation": None}

# 하위 호환성 별칭
generate_excel_with_gemini = generate_excel_with_chatgpt
