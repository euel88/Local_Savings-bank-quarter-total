"""
저축은행 통합 데이터 스크래퍼 (단순화 버전)
결산공시와 분기공시를 하나의 GUI에서 관리
버전: 4.0 (단순화 버전)
작성일: 2025-10-24
"""

import tkinter as tk
from tkinter import ttk, messagebox
import sys
import os
from datetime import datetime

# 현재 디렉토리를 Python 경로에 추가
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# 각 스크래퍼 모듈 import (에러 처리 포함)
try:
    import settlement_scraper
    SETTLEMENT_AVAILABLE = True
    print("✅ 결산공시 스크래퍼 모듈 로드 성공")
except ImportError as e:
    print(f"❌ 결산공시 스크래퍼 모듈 로드 실패: {e}")
    SETTLEMENT_AVAILABLE = False

try:
    import quarterly_scraper
    QUARTERLY_AVAILABLE = True
    print("✅ 분기공시 스크래퍼 모듈 로드 성공")
except ImportError as e:
    print(f"❌ 분기공시 스크래퍼 모듈 로드 실패: {e}")
    QUARTERLY_AVAILABLE = False


class SimpleBankScraperGUI:
    """단순화된 저축은행 스크래퍼 메인 GUI 클래스"""

    def __init__(self, root):
        self.root = root
        self.root.title("🏦 저축은행 데이터 스크래퍼 v4.0")
        self.root.geometry("1000x700")
        self.root.resizable(True, True)

        # 탭 인스턴스 저장
        self.settlement_tab = None
        self.quarterly_tab = None

        # 스타일 설정
        self.setup_styles()

        # 메인 UI 생성
        self.create_main_ui()

        # 종료 이벤트 바인딩
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)

    def setup_styles(self):
        """전체 앱 스타일 설정"""
        style = ttk.Style()

        # 사용 가능한 테마 확인 및 설정
        available_themes = style.theme_names()
        if 'clam' in available_themes:
            style.theme_use('clam')
        elif 'vista' in available_themes:
            style.theme_use('vista')
        elif 'alt' in available_themes:
            style.theme_use('alt')

        # 탭 스타일 커스터마이징
        style.configure('TNotebook', tabposition='n')
        style.configure('TNotebook.Tab', padding=[20, 10], font=('', 10, 'bold'))

    def create_main_ui(self):
        """메인 UI 구성 요소 생성"""
        # 메인 컨테이너
        main_container = ttk.Frame(self.root, padding="5")
        main_container.pack(fill=tk.BOTH, expand=True)

        # 상단 타이틀
        title_frame = ttk.Frame(main_container)
        title_frame.pack(fill=tk.X, pady=(0, 10))

        title_label = ttk.Label(
            title_frame,
            text="🏦 저축은행 공시자료 크롤링 시스템",
            font=("", 14, "bold")
        )
        title_label.pack()

        subtitle_label = ttk.Label(
            title_frame,
            text="79개 저축은행의 결산공시 및 분기공시 데이터 수집",
            font=("", 9)
        )
        subtitle_label.pack()

        # 탭 컨트롤 생성
        self.notebook = ttk.Notebook(main_container)
        self.notebook.pack(fill=tk.BOTH, expand=True, pady=5)

        # 결산공시 탭 추가
        if SETTLEMENT_AVAILABLE:
            try:
                self.settlement_tab = settlement_scraper.SettlementScraperTab(self.notebook, simplified=True)
                self.notebook.add(
                    self.settlement_tab.frame,
                    text="🏦 결산공시 (연말)"
                )
            except Exception as e:
                messagebox.showerror("오류", f"결산공시 탭을 로드할 수 없습니다: {str(e)}")
        else:
            placeholder_frame = ttk.Frame(self.notebook)
            ttk.Label(
                placeholder_frame,
                text="❌ 결산공시 스크래퍼 모듈을 사용할 수 없습니다.\n\nsettlement_scraper.py 파일을 확인해주세요.",
                font=("", 12),
                justify=tk.CENTER
            ).pack(expand=True)
            self.notebook.add(placeholder_frame, text="🏦 결산공시 (사용 불가)")

        # 분기공시 탭 추가
        if QUARTERLY_AVAILABLE:
            try:
                self.quarterly_tab = quarterly_scraper.QuarterlyScraperTab(self.notebook, simplified=True)
                self.notebook.add(
                    self.quarterly_tab.frame,
                    text="📊 분기공시 (3개월)"
                )
            except Exception as e:
                messagebox.showerror("오류", f"분기공시 탭을 로드할 수 없습니다: {str(e)}")
        else:
            placeholder_frame = ttk.Frame(self.notebook)
            ttk.Label(
                placeholder_frame,
                text="❌ 분기공시 스크래퍼 모듈을 사용할 수 없습니다.\n\nquarterly_scraper.py 파일을 확인해주세요.",
                font=("", 12),
                justify=tk.CENTER
            ).pack(expand=True)
            self.notebook.add(placeholder_frame, text="📊 분기공시 (사용 불가)")

        # 하단 상태바
        status_frame = ttk.Frame(main_container)
        status_frame.pack(side=tk.BOTTOM, fill=tk.X, pady=(5, 0))

        self.status_label = ttk.Label(
            status_frame,
            text="준비 완료",
            relief=tk.SUNKEN,
            padding=5
        )
        self.status_label.pack(side=tk.LEFT, fill=tk.X, expand=True)

        self.time_label = ttk.Label(
            status_frame,
            text="",
            relief=tk.SUNKEN,
            padding=5
        )
        self.time_label.pack(side=tk.RIGHT)

        # 시간 업데이트 시작
        self.update_time()

    def update_time(self):
        """시간 업데이트"""
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.time_label.config(text=current_time)
        self.root.after(1000, self.update_time)

    def on_closing(self):
        """프로그램 종료 시 호출"""
        if messagebox.askokcancel("종료", "프로그램을 종료하시겠습니까?"):
            self.root.destroy()


def main():
    """메인 함수"""
    root = tk.Tk()
    app = SimpleBankScraperGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()
