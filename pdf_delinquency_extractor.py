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


def extract_delinquency_from_pdf(pdf_path: str) -> Optional[Dict[str, str]]:
    """
    단일 통일경영공시 PDF에서 연체율 값을 추출한다.

    Returns:
        {"연체율_당기": "2.35", "연체율_전기": "1.98"} 형태 또는 None
    """
    if not PDFPLUMBER_AVAILABLE:
        logger.warning("pdfplumber가 설치되지 않았습니다.")
        return None

    if not os.path.exists(pdf_path):
        return None

    try:
        with pdfplumber.open(pdf_path) as pdf:
            # 모든 페이지에서 테이블 추출 시도
            for page in pdf.pages:
                result = _search_delinquency_in_page(page)
                if result:
                    return result

            # 테이블 추출 실패 시 텍스트 기반 추출
            for page in pdf.pages:
                result = _search_delinquency_in_text(page)
                if result:
                    return result

    except Exception as e:
        logger.error(f"PDF 파싱 오류 ({pdf_path}): {e}")

    return None


def _search_delinquency_in_page(page) -> Optional[Dict[str, str]]:
    """페이지의 테이블에서 연체율 행을 찾는다."""
    tables = page.extract_tables()
    if not tables:
        return None

    for table in tables:
        for row_idx, row in enumerate(table):
            if not row:
                continue
            # 셀 텍스트에서 '연체율' 키워드 탐색
            for col_idx, cell in enumerate(row):
                if not cell:
                    continue
                cell_clean = cell.strip().replace(" ", "")
                if "연체율" in cell_clean:
                    return _extract_values_from_row(table, row_idx, row, col_idx)

    return None


def _extract_values_from_row(
    table: list, row_idx: int, row: list, label_col: int
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
        # 헤더 판별 불가 시: 마지막 열 = 당기, 그 앞 = 전기 (일반적 공시 패턴)
        numbers.sort(key=lambda x: x[0])
        result["연체율_전기"] = str(numbers[-2][1])
        result["연체율_당기"] = str(numbers[-1][1])
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
        cell_clean = cell.strip().replace(" ", "")
        if any(kw in cell_clean for kw in ["전기", "전년", "전분기", "전년동기"]):
            prior_cols.add(idx)
        elif any(kw in cell_clean for kw in ["당기", "당분기", "금기", "금분기"]):
            current_cols.add(idx)

    return prior_cols, current_cols


def _search_delinquency_in_text(page) -> Optional[Dict[str, str]]:
    """페이지 텍스트에서 연체율 값을 정규식으로 추출한다."""
    text = page.extract_text()
    if not text or "연체율" not in text:
        return None

    # "연체율" 뒤에 나오는 숫자 패턴 매칭
    # 예: "연체율(%) 1.98 2.35" 또는 "연체율 1.98% 2.35%"
    pattern = r"연체율[^\d]*?([\d]+\.[\d]+)[%\s]+([\d]+\.[\d]+)"
    match = re.search(pattern, text)
    if match:
        return {
            "연체율_전기": match.group(1),
            "연체율_당기": match.group(2),
        }

    # 단일 값만 매칭
    pattern_single = r"연체율[^\d]*?([\d]+\.[\d]+)"
    match = re.search(pattern_single, text)
    if match:
        return {"연체율_당기": match.group(1)}

    return None


def _parse_number(text: str) -> Optional[float]:
    """텍스트에서 숫자(연체율 %) 값을 파싱한다."""
    if not text:
        return None
    cleaned = text.strip().replace(",", "").replace("%", "").replace(" ", "")
    # '-' 또는 빈 값
    if cleaned in ("-", "–", "—", "", "N/A", "해당없음"):
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
        data = extract_delinquency_from_pdf(pdf_path)
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
