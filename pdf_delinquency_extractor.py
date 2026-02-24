"""
통일경영공시 PDF에서 은행별 연체율을 추출하여 엑셀로 정리하는 모듈.
- pdfplumber 기반 텍스트/테이블 추출
- 연체율(%) 값만 추출
- 별도 엑셀 파일로 생성
"""

import os
import re
import logging
from datetime import datetime
from typing import List, Dict, Optional, Tuple

import pandas as pd

try:
    import pdfplumber
    PDFPLUMBER_AVAILABLE = True
except ImportError:
    PDFPLUMBER_AVAILABLE = False

logger = logging.getLogger(__name__)

# 연체율 관련 키워드 (통일경영공시 PDF에서 사용되는 다양한 표현)
_DELINQUENCY_KEYWORDS = [
    "연체율",
    "연체대출채권비율",
    "연체대출금비율",
    "연체비율",
]


def _is_delinquency_cell(text: str) -> bool:
    """셀 텍스트가 연체율 관련 항목인지 판별한다."""
    if not text:
        return False
    cleaned = text.strip().replace(" ", "").replace("\n", "")
    return any(kw in cleaned for kw in _DELINQUENCY_KEYWORDS)


def extract_delinquency_from_pdf(pdf_path: str, log_callback=None) -> Optional[Dict[str, str]]:
    """
    단일 통일경영공시 PDF에서 연체율 값을 추출한다.

    Returns:
        {"연체율_당기": "2.35", "연체율_전기": "1.98"} 형태 또는 None
    """
    def log(msg):
        if log_callback:
            log_callback(msg)

    if not PDFPLUMBER_AVAILABLE:
        logger.warning("pdfplumber가 설치되지 않았습니다.")
        return None

    if not os.path.exists(pdf_path):
        return None

    try:
        with pdfplumber.open(pdf_path) as pdf:
            # 1차: 모든 페이지에서 테이블 추출 시도
            for page_num, page in enumerate(pdf.pages):
                result = _search_delinquency_in_page(page)
                if result:
                    log(f"    [테이블 추출] 페이지 {page_num + 1}에서 연체율 발견")
                    return result

            # 2차: 텍스트 기반 추출
            for page_num, page in enumerate(pdf.pages):
                result = _search_delinquency_in_text(page)
                if result:
                    log(f"    [텍스트 추출] 페이지 {page_num + 1}에서 연체율 발견")
                    return result

            # 실패 시 디버그 정보 출력
            log(f"    [디버그] 총 {len(pdf.pages)}페이지 검색했으나 연체율 미발견")
            for page_num, page in enumerate(pdf.pages):
                text = page.extract_text() or ""
                for kw in _DELINQUENCY_KEYWORDS:
                    if kw in text.replace(" ", ""):
                        # 키워드는 있는데 추출 실패한 경우 주변 텍스트 출력
                        idx = text.replace(" ", "").index(kw)
                        # 원본 텍스트에서 해당 위치 근처 출력
                        snippet = text[max(0, idx - 20):idx + 80]
                        log(f"    [디버그] 페이지 {page_num + 1}에 '{kw}' 키워드 존재 (주변: ...{snippet}...)")
                        break

    except Exception as e:
        logger.error(f"PDF 파싱 오류 ({pdf_path}): {e}")
        log(f"    PDF 파싱 오류: {e}")

    return None


def _search_delinquency_in_page(page) -> Optional[Dict[str, str]]:
    """페이지의 테이블에서 연체율 행을 찾는다."""
    tables = page.extract_tables()
    if not tables:
        return None

    for table in tables:
        if not table:
            continue

        # 헤더 행 감지: 첫 몇 행 중 기간 키워드가 있는 행을 헤더로 사용
        header_row_idx = 0
        for h_idx in range(min(3, len(table))):
            row = table[h_idx]
            if row and any(
                cell and any(kw in cell.strip().replace(" ", "").replace("\n", "")
                             for kw in ["전기", "전년", "당기", "금기", "공시기준", "전년동기"])
                for cell in row if cell
            ):
                header_row_idx = h_idx
                break

        header_row = table[header_row_idx] if header_row_idx < len(table) else table[0]

        for row_idx, row in enumerate(table):
            if not row or row_idx == header_row_idx:
                continue
            # 셀 텍스트에서 연체율 키워드 탐색
            for col_idx, cell in enumerate(row):
                if not cell:
                    continue
                if _is_delinquency_cell(cell):
                    result = _extract_values_from_row(table, row_idx, row, col_idx, header_row)
                    if result:
                        return result

    return None


def _extract_values_from_row(
    table: list, row_idx: int, row: list, label_col: int,
    header_row: list = None
) -> Optional[Dict[str, str]]:
    """
    연체율이 발견된 행에서 수치 값을 추출한다.
    보통 테이블 구조: [항목명, ..., 전기값, 당기값] 또는 [항목명, 당기값, 전기값]
    """
    # 같은 행에서 숫자 값들 수집
    numbers = []
    for col_idx, cell in enumerate(row):
        if col_idx == label_col:
            continue
        if cell:
            val = _parse_number(cell)
            if val is not None:
                numbers.append((col_idx, val))

    if not numbers:
        # 아래 행에 값이 있는 경우 (병합 셀)
        if row_idx + 1 < len(table):
            next_row = table[row_idx + 1]
            if next_row:
                for col_idx, cell in enumerate(next_row):
                    if cell:
                        val = _parse_number(cell)
                        if val is not None:
                            numbers.append((col_idx, val))

    if not numbers:
        return None

    # 헤더 행에서 전기/당기 판별 시도
    if header_row is None:
        header_row = table[0] if table else []
    prior_cols, current_cols = _identify_period_columns(header_row)

    result = {}
    if prior_cols and current_cols:
        for col_idx, val in numbers:
            if col_idx in current_cols:
                result["연체율_당기"] = str(val)
            elif col_idx in prior_cols:
                result["연체율_전기"] = str(val)
    elif len(numbers) >= 2:
        # 헤더 판별 불가 시: 위치 기반 추정
        # 통일경영공시 PDF는 보통 [공시기준(당기), 전년동기(전기)] 순서
        numbers.sort(key=lambda x: x[0])
        result["연체율_당기"] = str(numbers[0][1])
        result["연체율_전기"] = str(numbers[1][1])
    elif len(numbers) == 1:
        result["연체율_당기"] = str(numbers[0][1])

    return result if result else None


def _identify_period_columns(header_row: list) -> Tuple[set, set]:
    """헤더 행에서 전기/당기 컬럼 인덱스를 판별한다."""
    prior_cols = set()
    current_cols = set()

    if not header_row:
        return prior_cols, current_cols

    for idx, cell in enumerate(header_row):
        if not cell:
            continue
        cell_clean = cell.strip().replace(" ", "").replace("\n", "")
        if any(kw in cell_clean for kw in ["전기", "전년", "전분기", "전년동기"]):
            prior_cols.add(idx)
        elif any(kw in cell_clean for kw in ["당기", "당분기", "금기", "금분기", "공시기준"]):
            current_cols.add(idx)

    return prior_cols, current_cols


def _search_delinquency_in_text(page) -> Optional[Dict[str, str]]:
    """페이지 텍스트에서 연체율 값을 정규식으로 추출한다."""
    text = page.extract_text()
    if not text:
        return None

    text_clean = text.replace(" ", "")

    # 연체율 키워드 존재 확인
    has_keyword = any(kw in text_clean for kw in _DELINQUENCY_KEYWORDS)
    if not has_keyword:
        return None

    # 각 키워드에 대해 패턴 매칭 시도
    for kw in _DELINQUENCY_KEYWORDS:
        if kw not in text_clean:
            continue

        # 패턴 1: 키워드 뒤에 두 개의 숫자 (소수점 있는 경우)
        pattern_two = kw + r"[^\d]*?([\d]+(?:\.[\d]+)?)[%\s]+([\d]+(?:\.[\d]+)?)"
        match = re.search(pattern_two, text_clean)
        if match:
            val1 = match.group(1)
            val2 = match.group(2)
            # 통일경영공시: 보통 공시기준(당기)이 먼저, 전년동기(전기)가 뒤
            return {
                "연체율_당기": val1,
                "연체율_전기": val2,
            }

        # 패턴 2: 키워드 뒤에 한 개의 숫자
        pattern_one = kw + r"[^\d]*?([\d]+(?:\.[\d]+)?)"
        match = re.search(pattern_one, text_clean)
        if match:
            return {"연체율_당기": match.group(1)}

    # 원본 텍스트(공백 포함)에서도 시도
    for kw in _DELINQUENCY_KEYWORDS:
        if kw not in text.replace(" ", ""):
            continue

        pattern_two = r"연체[^\d]*?비율[^\d]*?([\d]+(?:\.[\d]+)?)[%\s]+([\d]+(?:\.[\d]+)?)"
        match = re.search(pattern_two, text)
        if match:
            return {
                "연체율_당기": match.group(1),
                "연체율_전기": match.group(2),
            }

        pattern_one = r"연체[^\d]*?비율[^\d]*?([\d]+(?:\.[\d]+)?)"
        match = re.search(pattern_one, text)
        if match:
            return {"연체율_당기": match.group(1)}

    return None


def _parse_number(text: str) -> Optional[float]:
    """텍스트에서 숫자(연체율 %) 값을 파싱한다."""
    if not text:
        return None
    cleaned = text.strip().replace(",", "").replace("%", "").replace(" ", "").replace("\n", "")
    # '-' 또는 빈 값
    if cleaned in ("-", "–", "—", "", "N/A", "해당없음", "해당\n없음"):
        return None
    try:
        val = float(cleaned)
        # 연체율은 보통 0~100% 범위
        if 0 <= val <= 100:
            return val
        return None
    except (ValueError, TypeError):
        return None


def create_delinquency_excel(
    download_path: str,
    output_path: Optional[str] = None,
    log_callback=None
) -> Optional[str]:
    """
    다운로드 폴더의 통일경영공시 PDF들에서 연체율을 추출하여 엑셀 파일을 생성한다.

    Args:
        download_path: PDF 파일들이 저장된 디렉터리
        output_path: 출력 엑셀 경로 (None이면 download_path 내에 자동 생성)
        log_callback: 로그 콜백 함수

    Returns:
        생성된 엑셀 파일 경로 또는 None
    """
    if not PDFPLUMBER_AVAILABLE:
        if log_callback:
            log_callback("pdfplumber가 설치되지 않아 연체율 추출을 건너뜁니다.")
        return None

    def log(msg):
        if log_callback:
            log_callback(msg)

    # 통일경영공시 PDF 파일 목록
    pdf_files = _find_disclosure_pdfs(download_path)
    if not pdf_files:
        log("연체율 추출 대상 통일경영공시 PDF 파일이 없습니다.")
        return None

    log(f"연체율 추출 시작: {len(pdf_files)}개 통일경영공시 PDF")

    rows = []
    for bank_name, pdf_path in pdf_files:
        data = extract_delinquency_from_pdf(pdf_path, log_callback=log_callback)
        if data:
            rows.append({
                "No": len(rows) + 1,
                "은행명": bank_name,
                "연체율_전기(%)": data.get("연체율_전기", ""),
                "연체율_당기(%)": data.get("연체율_당기", ""),
            })
            log(f"  {bank_name}: 전기 {data.get('연체율_전기', '-')}% / 당기 {data.get('연체율_당기', '-')}%")
        else:
            rows.append({
                "No": len(rows) + 1,
                "은행명": bank_name,
                "연체율_전기(%)": "",
                "연체율_당기(%)": "",
            })
            log(f"  {bank_name}: 연체율 추출 실패")

    if not rows:
        log("추출된 연체율 데이터가 없습니다.")
        return None

    # 엑셀 생성
    df = pd.DataFrame(rows)

    if not output_path:
        output_path = os.path.join(
            download_path,
            f"연체율_요약_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        )

    try:
        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="연체율", index=False)

            ws = writer.sheets["연체율"]
            # 열 너비 조정
            ws.column_dimensions["A"].width = 6
            ws.column_dimensions["B"].width = 20
            ws.column_dimensions["C"].width = 16
            ws.column_dimensions["D"].width = 16

        extracted_count = sum(1 for r in rows if r["연체율_당기(%)"])
        log(f"연체율 엑셀 생성 완료: {extracted_count}/{len(rows)}개 은행 추출 → {os.path.basename(output_path)}")
        return output_path

    except Exception as e:
        log(f"연체율 엑셀 생성 오류: {e}")
        return None


def extract_all_delinquency(download_path: str, log_callback=None) -> Dict[str, Dict[str, str]]:
    """
    다운로드 폴더의 모든 통일경영공시 PDF에서 연체율을 추출한다.

    Returns:
        {"은행명": {"연체율_전기": "1.98", "연체율_당기": "2.35"}, ...}
    """
    if not PDFPLUMBER_AVAILABLE:
        return {}

    def log(msg):
        if log_callback:
            log_callback(msg)

    pdf_files = _find_disclosure_pdfs(download_path)
    if not pdf_files:
        log("연체율 추출 대상 통일경영공시 PDF 파일이 없습니다.")
        return {}

    log(f"연체율 추출 시작: {len(pdf_files)}개 통일경영공시 PDF")
    result = {}
    for bank_name, pdf_path in pdf_files:
        data = extract_delinquency_from_pdf(pdf_path, log_callback=log_callback)
        if data:
            result[bank_name] = data
            log(f"  {bank_name}: 전기 {data.get('연체율_전기', '-')}% / 당기 {data.get('연체율_당기', '-')}%")
        else:
            log(f"  {bank_name}: 연체율 추출 실패")

    log(f"연체율 추출 완료: {len(result)}/{len(pdf_files)}개 성공")
    return result


def patch_excel_with_delinquency(
    excel_path: str,
    delinquency_data: Dict[str, Dict[str, str]],
    log_callback=None
) -> bool:
    """
    기존 분기총괄 엑셀의 연체율 컬럼에 PDF에서 추출한 연체율을 기입한다.

    Args:
        excel_path: 기존 분기총괄 엑셀 파일 경로
        delinquency_data: {"은행명": {"연체율_전기": "1.98", "연체율_당기": "2.35"}}
        log_callback: 로그 콜백

    Returns:
        성공 여부
    """
    if not delinquency_data or not excel_path or not os.path.exists(excel_path):
        return False

    def log(msg):
        if log_callback:
            log_callback(msg)

    try:
        from openpyxl import load_workbook

        wb = load_workbook(excel_path)
        if "분기총괄" not in wb.sheetnames:
            log("분기총괄 시트를 찾을 수 없습니다.")
            wb.close()
            return False

        ws = wb["분기총괄"]

        # 헤더 행에서 컬럼 인덱스 찾기
        header_row = [cell.value for cell in ws[1]]
        company_col = None
        prior_col = None
        current_col = None

        for idx, val in enumerate(header_row):
            if not val:
                continue
            val_str = str(val).strip()
            if val_str == "회사명":
                company_col = idx
            elif "연체율" in val_str and any(kw in val_str for kw in ["전년동기", "전기"]):
                prior_col = idx
            elif "연체율" in val_str and any(kw in val_str for kw in ["금분기", "금기", "당기"]):
                current_col = idx

        if company_col is None:
            log("회사명 컬럼을 찾을 수 없습니다.")
            wb.close()
            return False

        if prior_col is None and current_col is None:
            log("연체율 컬럼을 찾을 수 없습니다.")
            wb.close()
            return False

        patched = 0
        for row_idx in range(2, ws.max_row + 1):
            bank_name = ws.cell(row=row_idx, column=company_col + 1).value
            if not bank_name:
                continue
            bank_name = str(bank_name).strip()

            # 은행명 매칭 (정확 일치 또는 부분 일치)
            matched_data = None
            if bank_name in delinquency_data:
                matched_data = delinquency_data[bank_name]
            else:
                for pdf_bank, data in delinquency_data.items():
                    if pdf_bank in bank_name or bank_name in pdf_bank:
                        matched_data = data
                        break
                    # "저축은행" 제거 후 비교
                    clean_pdf = pdf_bank.replace("저축은행", "").strip()
                    clean_bank = bank_name.replace("저축은행", "").strip()
                    if clean_pdf and clean_bank and (clean_pdf in clean_bank or clean_bank in clean_pdf):
                        matched_data = data
                        break

            if not matched_data:
                continue

            updated = False
            if prior_col is not None and matched_data.get("연체율_전기"):
                cell = ws.cell(row=row_idx, column=prior_col + 1)
                existing = cell.value
                if not existing or str(existing).strip() in ("", "-", "0", "0.0"):
                    try:
                        cell.value = float(matched_data["연체율_전기"])
                    except (ValueError, TypeError):
                        cell.value = matched_data["연체율_전기"]
                    updated = True

            if current_col is not None and matched_data.get("연체율_당기"):
                cell = ws.cell(row=row_idx, column=current_col + 1)
                existing = cell.value
                if not existing or str(existing).strip() in ("", "-", "0", "0.0"):
                    try:
                        cell.value = float(matched_data["연체율_당기"])
                    except (ValueError, TypeError):
                        cell.value = matched_data["연체율_당기"]
                    updated = True

            if updated:
                patched += 1

        wb.save(excel_path)
        wb.close()

        log(f"연체율 기입 완료: {patched}개 은행 엑셀에 반영")
        return patched > 0

    except Exception as e:
        log(f"연체율 엑셀 기입 오류: {e}")
        return False


def _find_disclosure_pdfs(download_path: str) -> List[Tuple[str, str]]:
    """
    다운로드 폴더에서 통일경영공시 PDF 파일을 찾아 (은행명, 경로) 리스트를 반환한다.
    파일명 패턴: {은행명}_통일경영공시.pdf
    """
    results = []
    if not os.path.isdir(download_path):
        return results

    for filename in sorted(os.listdir(download_path)):
        if "통일경영공시" not in filename:
            continue
        if not filename.lower().endswith(".pdf"):
            continue

        filepath = os.path.join(download_path, filename)
        if not os.path.isfile(filepath):
            continue

        # 은행명 추출: "{은행명}_통일경영공시.pdf" → 은행명
        bank_name = filename.split("_통일경영공시")[0]
        if bank_name:
            results.append((bank_name, filepath))

    return results
