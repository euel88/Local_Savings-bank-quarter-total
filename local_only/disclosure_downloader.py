"""
저축은행 중앙회 통일경영공시 및 감사보고서 자동 다운로드 스크립트
v3.0 - 브라우저 재시작 없는 안정적인 버전

단독 실행: python disclosure_downloader.py
또는 main_local.py에서 탭으로 로드

핵심 다운로드 로직은 downloader_core.py 모듈 사용
"""

import sys
import os
import time
import threading
import tkinter as tk
from tkinter import ttk, filedialog, scrolledtext, messagebox
from datetime import datetime

# 상위 디렉토리를 경로에 추가 (downloader_core 임포트용)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from downloader_core import (
        DisclosureDownloader,
        get_memory_usage,
        PSUTIL_AVAILABLE
    )
    CORE_AVAILABLE = True
except ImportError as e:
    print(f"downloader_core 모듈 로드 실패: {e}")
    CORE_AVAILABLE = False
    PSUTIL_AVAILABLE = False

    def get_memory_usage():
        return 0.0

# 전역 변수
TODAY = datetime.now().strftime("%Y%m%d")
DEFAULT_DOWNLOAD_DIR = os.path.join(os.getcwd(), "downloads")


class DisclosureDownloaderGUI:
    """통일경영공시/감사보고서 다운로더 Tkinter GUI"""

    def __init__(self, parent=None, as_tab=False):
        """
        Args:
            parent: 부모 위젯 (탭 모드에서 사용)
            as_tab: True이면 탭으로 동작 (자체 윈도우 없음)
        """
        self.as_tab = as_tab
        self.downloader = None
        self.is_downloading = False

        if as_tab and parent:
            self.frame = ttk.Frame(parent, padding="5")
            self._build_ui(self.frame)
        else:
            self.app = tk.Tk()
            self.app.title("저축은행 공시자료 다운로더 v3.0 - No Restart Edition")
            self.app.geometry("800x700")
            self.app.grid_rowconfigure(3, weight=1)
            self.app.grid_columnconfigure(0, weight=1)
            self._build_ui(self.app)

    def _build_ui(self, root):
        """GUI 구성 요소 생성"""
        style = ttk.Style()
        try:
            style.theme_use('default')
        except Exception:
            pass

        # ---- 설정 프레임 ----
        settings_frame = ttk.LabelFrame(root, text="설정", padding="10")
        if self.as_tab:
            settings_frame.pack(fill=tk.X, padx=5, pady=5)
        else:
            settings_frame.grid(row=0, column=0, sticky="ew", padx=10, pady=5)
        settings_frame.grid_columnconfigure(1, weight=1)

        ttk.Label(settings_frame, text="다운로드 폴더:").grid(row=0, column=0, sticky="w", pady=2)
        self.download_dir_var = tk.StringVar(value=DEFAULT_DOWNLOAD_DIR)
        dir_entry = ttk.Entry(settings_frame, textvariable=self.download_dir_var, state="readonly")
        dir_entry.grid(row=0, column=1, sticky="ew", padx=5)
        ttk.Button(
            settings_frame, text="폴더 선택",
            command=lambda: self.download_dir_var.set(
                filedialog.askdirectory() or DEFAULT_DOWNLOAD_DIR
            )
        ).grid(row=0, column=2)

        # 수동 드라이버 경로 설정
        self.manual_driver_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            settings_frame,
            text="수동으로 드라이버 경로 지정 (네트워크 오류 시)",
            variable=self.manual_driver_var,
            command=self._toggle_manual_driver
        ).grid(row=1, column=0, columnspan=3, sticky='w', pady=5)

        ttk.Label(settings_frame, text="드라이버 경로:").grid(row=2, column=0, sticky="w", pady=2)
        self.driver_path_var = tk.StringVar()
        self.driver_path_entry = ttk.Entry(
            settings_frame, textvariable=self.driver_path_var, state="readonly"
        )
        self.driver_path_entry.grid(row=2, column=1, sticky="ew", padx=5)
        self.select_driver_button = ttk.Button(
            settings_frame, text="드라이버 선택...",
            command=self._select_driver_path, state="disabled"
        )
        self.select_driver_button.grid(row=2, column=2)

        # ---- 컨트롤 프레임 ----
        control_frame = ttk.Frame(root, padding="10")
        if self.as_tab:
            control_frame.pack(fill=tk.X, padx=5, pady=5)
        else:
            control_frame.grid(row=1, column=0, sticky="ew", padx=10, pady=5)
        control_frame.grid_columnconfigure(0, weight=1)
        control_frame.grid_columnconfigure(1, weight=1)

        self.start_button = ttk.Button(
            control_frame, text="다운로드 시작", command=self._start_download
        )
        self.start_button.grid(row=0, column=0, sticky="ew", padx=(0, 5), ipady=5)

        ttk.Button(
            control_frame, text="다운로드 중단", command=self._stop_download
        ).grid(row=0, column=1, sticky="ew", padx=(5, 0), ipady=5)

        # ---- 진행 상황 프레임 ----
        progress_frame = ttk.LabelFrame(root, text="진행 상황", padding="10")
        if self.as_tab:
            progress_frame.pack(fill=tk.X, padx=5, pady=5)
        else:
            progress_frame.grid(row=2, column=0, sticky="ew", padx=10, pady=5)
        progress_frame.grid_columnconfigure(0, weight=1)

        self.progress_bar = ttk.Progressbar(progress_frame, orient="horizontal", mode="determinate")
        self.progress_bar.grid(row=0, column=0, sticky="ew", pady=5)

        # ---- 로그 프레임 ----
        log_frame = ttk.LabelFrame(root, text="로그", padding="10")
        if self.as_tab:
            log_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        else:
            log_frame.grid(row=3, column=0, sticky="nsew", padx=10, pady=5)
        log_frame.grid_rowconfigure(0, weight=1)
        log_frame.grid_columnconfigure(0, weight=1)

        self.log_text = scrolledtext.ScrolledText(log_frame, wrap=tk.WORD, state='disabled', height=15)
        self.log_text.grid(row=0, column=0, sticky="nsew")

        # ---- 하단 상태바 ----
        bottom_frame = ttk.Frame(root, padding="5")
        if self.as_tab:
            bottom_frame.pack(fill=tk.X, padx=5, pady=5)
        else:
            bottom_frame.grid(row=4, column=0, sticky="ew", padx=10, pady=5)

        self.status_label = ttk.Label(bottom_frame, text="상태: 준비 완료")
        self.status_label.grid(row=0, column=0, sticky="w")

        self.memory_label = ttk.Label(bottom_frame, text="")
        self.memory_label.grid(row=0, column=1, sticky="e", padx=(0, 20))
        self._update_memory()

        # 초기 로그
        self._log("=" * 50)
        self._log("저축은행 공시자료 다운로더 v3.0")
        self._log("브라우저 재시작 없는 안정적인 버전")
        self._log("=" * 50)

    # ---- 로그 관련 ----
    def _log(self, message):
        """로그 메시지 출력"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        formatted = f"[{timestamp}] {message}"

        self.log_text.configure(state='normal')

        # 로그 길이 제한
        line_count = int(self.log_text.index('end-1c').split('.')[0])
        if line_count > 500:
            self.log_text.delete('1.0', '50.0')

        self.log_text.insert(tk.END, formatted + "\n")
        self.log_text.configure(state='disabled')
        self.log_text.see(tk.END)

        root = self.app if not self.as_tab else self.frame.winfo_toplevel()
        try:
            root.update_idletasks()
        except Exception:
            pass

        print(formatted)

    def _update_memory(self):
        """메모리 사용률 업데이트"""
        if PSUTIL_AVAILABLE:
            self.memory_label.config(text=f"메모리: {get_memory_usage():.1f}%")

        root = self.app if not self.as_tab else self.frame.winfo_toplevel()
        try:
            root.after(5000, self._update_memory)
        except Exception:
            pass

    # ---- 설정 관련 ----
    def _toggle_manual_driver(self):
        state = 'normal' if self.manual_driver_var.get() else 'disabled'
        self.driver_path_entry.config(
            state='normal' if self.manual_driver_var.get() else 'readonly'
        )
        self.select_driver_button.config(state=state)

    def _select_driver_path(self):
        filepath = filedialog.askopenfilename(
            title="chromedriver를 선택하세요",
            filetypes=[("ChromeDriver", "chromedriver*"), ("All files", "*.*")]
        )
        if filepath:
            self.driver_path_var.set(filepath)

    # ---- 다운로드 로직 ----
    def _start_download(self):
        """다운로드 시작"""
        if not CORE_AVAILABLE:
            messagebox.showerror("오류", "downloader_core 모듈을 로드할 수 없습니다.")
            return

        if self.is_downloading:
            messagebox.showwarning("경고", "이미 다운로드가 진행 중입니다.")
            return

        if not messagebox.askyesno("시작 확인", "다운로드를 시작하시겠습니까?"):
            return

        self.start_button.config(state=tk.DISABLED)
        self.is_downloading = True

        threading.Thread(target=self._download_thread, daemon=True).start()

    def _download_thread(self):
        """다운로드 실행 스레드"""
        try:
            download_path = self.download_dir_var.get()
            os.makedirs(download_path, exist_ok=True)

            driver_path = self.driver_path_var.get() if self.manual_driver_var.get() else None

            # 다운로더 생성
            self.downloader = DisclosureDownloader(
                download_path=download_path,
                log_callback=self._log,
                headless=False,
                driver_path=driver_path
            )

            # 이전 진행 상태 확인
            progress = self.downloader.load_progress()
            start_index = progress.get("current_index", 0)

            if start_index > 0:
                resume = messagebox.askyesno(
                    "이어서 진행",
                    f"이전 진행 상태가 발견되었습니다.\n"
                    f"{start_index}번째 은행부터 이어서 진행하시겠습니까?"
                )
                if not resume:
                    start_index = 0
                    self.downloader.reset_progress()

            # 은행 목록 추출
            self.status_label.config(text="상태: 웹사이트 접속 중...")
            bank_list = self.downloader.start_and_extract_banks()

            if not bank_list:
                messagebox.showerror("오류", "은행 목록을 추출할 수 없습니다.")
                return

            # 이어서 진행
            if start_index > 0:
                bank_list = bank_list[start_index:]
                self._log(f"{start_index}번째부터 {len(bank_list)}개 은행 다운로드 시작")

            self.status_label.config(text="상태: 다운로드 진행 중...")

            # 다운로드 실행
            def update_progress(current, total, bank_name):
                pct = (start_index + current + 1) / (start_index + total) * 100
                self.progress_bar['value'] = pct
                self.status_label.config(
                    text=f"상태: {start_index + current + 1}/{start_index + total} - {bank_name}"
                )
                root = self.app if not self.as_tab else self.frame.winfo_toplevel()
                try:
                    root.update_idletasks()
                except Exception:
                    pass

            total_downloaded = self.downloader.download_all(bank_list, update_progress)

            # 완료
            self.progress_bar['value'] = 100
            self._log(f"\n모든 작업 완료! 총 {total_downloaded}개 파일 다운로드됨")
            self.status_label.config(text="상태: 완료")

            # 보고서 생성
            self.downloader.create_report()
            self.downloader.reset_progress()

            # 결과 알림
            failed = [r['은행명'] for r in self.downloader.results if r['상태'] == '실패']
            if failed:
                messagebox.showwarning(
                    "부분 완료",
                    f"실패한 은행 ({len(failed)}개):\n"
                    f"{', '.join(failed[:10])}"
                    f"{' 외 ' + str(len(failed) - 10) + '개' if len(failed) > 10 else ''}"
                )
            else:
                messagebox.showinfo("완료", "모든 다운로드가 성공적으로 완료되었습니다!")

        except Exception as e:
            self._log(f"치명적 오류: {str(e)}")
            self.status_label.config(text="상태: 오류 발생")
            messagebox.showerror("오류", f"다운로드 중 오류:\n{str(e)}")

        finally:
            self.is_downloading = False
            if self.downloader:
                self.downloader.cleanup()
                self.downloader = None
            self.start_button.config(state=tk.NORMAL)
            self._log("다운로드 절차 종료")

    def _stop_download(self):
        """다운로드 중단"""
        if self.is_downloading and self.downloader:
            if messagebox.askyesno("중단 확인", "정말로 다운로드를 중단하시겠습니까?"):
                self.downloader.stop()
                self.status_label.config(text="상태: 중단 중...")

    def run(self):
        """독립 실행 모드 (메인 루프 시작)"""
        if not self.as_tab and hasattr(self, 'app'):
            self.app.mainloop()


# main_local.py에서 탭으로 사용할 때의 호환 클래스
class DisclosureDownloaderTab:
    """main_local.py의 탭 패턴과 호환되는 래퍼"""

    def __init__(self, parent, simplified=False):
        self.gui = DisclosureDownloaderGUI(parent=parent, as_tab=True)
        self.frame = self.gui.frame


if __name__ == "__main__":
    gui = DisclosureDownloaderGUI()
    gui.run()
