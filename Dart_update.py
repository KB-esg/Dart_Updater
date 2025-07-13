import os
from datetime import datetime, timedelta
import json
import time
import gspread
from google.oauth2.service_account import Credentials
import OpenDartReader
import pandas as pd
from openpyxl import load_workbook
from playwright.sync_api import sync_playwright
import shutil

class DartExcelDownloader:
    """DART 재무제표 Excel 다운로드 및 Google Sheets 업로드 (Playwright 사용)"""
    
    def __init__(self, company_config):
        """초기화"""
        self.corp_code = company_config['corp_code']
        self.company_name = company_config['company_name']
        self.spreadsheet_var_name = company_config['spreadsheet_var']
        
        # 환경변수 확인
        self._check_environment_variables()
        
        # Google Sheets 설정
        self.credentials = self._get_google_credentials()
        self.gc = gspread.authorize(self.credentials)
        self.workbook = self.gc.open_by_key(os.environ[self.spreadsheet_var_name])
        
        # DART API 설정
        self.dart = OpenDartReader(os.environ['DART_API_KEY'])
        
        # 텔레그램 설정
        self.telegram_bot_token = os.environ.get('TELEGRAM_BOT_TOKEN')
        self.telegram_channel_id = os.environ.get('TELEGRAM_CHANNEL_ID')
        
        # 다운로드 폴더 설정
        self.download_dir = os.path.join(os.getcwd(), 'downloads')
        os.makedirs(self.download_dir, exist_ok=True)
        
        # 처리 결과 추적
        self.results = {
            'total_reports': 0,
            'downloaded_files': [],
            'uploaded_sheets': [],
            'failed_downloads': [],
            'failed_uploads': []
        }

    def _check_environment_variables(self):
        """환경변수 확인"""
        print("🔍 환경변수 확인:")
        required_vars = ['DART_API_KEY', 'GOOGLE_CREDENTIALS', self.spreadsheet_var_name]
        
        for var in required_vars:
            if var in os.environ:
                value = os.environ[var]
                masked_value = f"{value[:6]}...{value[-4:]}" if len(value) > 20 else f"{value[:-2]}**"
                print(f"✅ {var}: {masked_value} (길이: {len(value)})")
            else:
                raise ValueError(f"❌ {var} 환경변수가 설정되지 않았습니다.")

    def _get_google_credentials(self):
        """Google Sheets 인증 설정"""
        creds_json = json.loads(os.environ['GOOGLE_CREDENTIALS'])
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        return Credentials.from_service_account_info(creds_json, scopes=scopes)

    def run(self):
        """메인 실행 함수"""
        print(f"\n🚀 {self.company_name}({self.corp_code}) 재무제표 다운로드 시작")
        
        # 1. 보고서 목록 조회
        reports = self._get_recent_reports()
        if reports.empty:
            print("📭 최근 보고서가 없습니다.")
            return
        
        print(f"📋 발견된 보고서: {len(reports)}개")
        self.results['total_reports'] = len(reports)
        
        # 2. Playwright로 각 보고서 처리
        with sync_playwright() as p:
            # 브라우저 시작 (헤드리스 모드)
            browser = p.chromium.launch(
                headless=True,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage'
                ]
            )
            context = browser.new_context(
                accept_downloads=True,
                locale='ko-KR',
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            )
            
            try:
                # 각 보고서 처리
                for _, report in reports.iterrows():
                    self._process_report_with_browser(context, report)
                    
            finally:
                browser.close()
        
        # 3. Archive 업데이트 (선택적)
        if os.environ.get('ENABLE_ARCHIVE_UPDATE', 'true').lower() == 'true':
            self._update_archive()
        
        # 4. 결과 요약
        self._print_summary()
        
        # 5. 다운로드 폴더 정리
        self._cleanup_downloads()

    def _get_recent_reports(self):
        """최근 보고서 목록 조회"""
        start_date, end_date = self._get_date_range()
        return self.dart.list(self.corp_code, start_date, end_date, kind='A', final='T')

    def _get_date_range(self):
        """날짜 범위 계산"""
        # 수동 설정 확인
        manual_start = os.environ.get('MANUAL_START_DATE')
        manual_end = os.environ.get('MANUAL_END_DATE')
        
        if manual_start and manual_end:
            print(f"📅 수동 설정 날짜: {manual_start} ~ {manual_end}")
            return manual_start, manual_end
        
        # 기본값: 최근 3개월
        end_date = datetime.now()
        start_date = end_date - timedelta(days=90)
        date_range = start_date.strftime('%Y%m%d'), end_date.strftime('%Y%m%d')
        print(f"📅 기본 날짜 범위 (최근 3개월): {date_range[0]} ~ {date_range[1]}")
        return date_range

    def _process_report_with_browser(self, context, report):
        """브라우저로 개별 보고서 처리"""
        print(f"\n📄 보고서 처리: {report['report_nm']} (접수번호: {report['rcept_no']})")
        
        page = context.new_page()
        
        try:
            # XBRL 뷰어 페이지 열기
            viewer_url = f"https://opendart.fss.or.kr/xbrl/viewer/main.do?rcpNo={report['rcept_no']}"
            print(f"🌐 페이지 열기: {viewer_url}")
            
            page.goto(viewer_url, wait_until='networkidle', timeout=60000)
            page.wait_for_timeout(2000)  # 페이지 로딩 대기
            
            # 다운로드 버튼 찾기 및 클릭
            download_button = page.locator('button.btnDown').first
            if not download_button.is_visible():
                print("⚠️ 다운로드 버튼을 찾을 수 없습니다.")
                self.results['failed_downloads'].append(report['rcept_no'])
                return
            
            print("🖱️ 다운로드 버튼 클릭")
            
            # 새 창 대기 - page.expect_popup() 사용
            with page.expect_popup() as popup_info:
                download_button.click()
            
            popup = popup_info.value
            popup.wait_for_load_state('networkidle')
            
            # 다운로드 팝업에서 Excel 파일 다운로드
            self._download_excel_files(popup, report['rcept_no'])
            
            popup.close()
            
        except Exception as e:
            print(f"❌ 브라우저 처리 실패: {str(e)}")
            self.results['failed_downloads'].append(report['rcept_no'])
            
        finally:
            page.close()

    def _download_excel_files(self, popup_page, rcept_no):
        """팝업 페이지에서 Excel 파일 다운로드"""
        try:
            # 페이지 로딩 대기
            popup_page.wait_for_timeout(2000)
            
            # 현재 URL 확인
            print(f"📍 팝업 페이지 URL: {popup_page.url}")
            
            # 다운로드 링크들 찾기
            download_links = popup_page.locator('a.btnFile')
            link_count = download_links.count()
            print(f"📄 다운로드 가능한 파일 수: {link_count}개")
            
            # 모든 링크의 href 확인 (디버깅용)
            for i in range(link_count):
                href = download_links.nth(i).get_attribute('href')
                print(f"  - 링크 {i+1}: {href}")
            
            # 재무제표 다운로드 (첫 번째 xlsx)
            if link_count >= 1:
                print("📥 재무제표 다운로드 중...")
                
                # 다운로드 대기 설정
                with popup_page.expect_download() as download_info:
                    download_links.nth(0).click()  # 첫 번째 버튼 클릭
                
                download = download_info.value
                
                # 원본 파일명 확인
                suggested_filename = download.suggested_filename
                print(f"  원본 파일명: {suggested_filename}")
                
                # 파일 저장
                file_path = os.path.join(self.download_dir, f"재무제표_{rcept_no}.xlsx")
                download.save_as(file_path)
                
                print(f"✅ 재무제표 다운로드 완료: {file_path}")
                self.results['downloaded_files'].append(file_path)
                
                # Google Sheets에 업로드
                self._upload_excel_to_sheets(file_path, "재무제표", rcept_no)
                
                # 다음 다운로드 전 잠시 대기
                popup_page.wait_for_timeout(2000)
            
            # 재무제표주석 다운로드 (두 번째 xlsx)
            if link_count >= 2:
                print("📥 재무제표주석 다운로드 중...")
                
                with popup_page.expect_download() as download_info:
                    download_links.nth(1).click()  # 두 번째 버튼 클릭
                
                download = download_info.value
                
                # 원본 파일명 확인
                suggested_filename = download.suggested_filename
                print(f"  원본 파일명: {suggested_filename}")
                
                # 파일 저장
                file_path = os.path.join(self.download_dir, f"재무제표주석_{rcept_no}.xlsx")
                download.save_as(file_path)
                
                print(f"✅ 재무제표주석 다운로드 완료: {file_path}")
                self.results['downloaded_files'].append(file_path)
                
                # Google Sheets에 업로드
                self._upload_excel_to_sheets(file_path, "재무제표주석", rcept_no)
                
        except Exception as e:
            print(f"❌ Excel 다운로드 실패: {str(e)}")
            import traceback
            traceback.print_exc()
            self.results['failed_downloads'].append(f"Excel_{rcept_no}")

    def _upload_excel_to_sheets(self, file_path, file_type, rcept_no):
        """Excel 파일을 Google Sheets에 업로드"""
        try:
            # Excel 파일 읽기
            wb = load_workbook(file_path, data_only=True)
            print(f"📊 Excel 파일 열기 완료. 시트 목록: {wb.sheetnames}")
            
            # 각 시트를 Google Sheets에 업로드
            for sheet_name in wb.sheetnames:
                self._upload_sheet_to_google(wb[sheet_name], sheet_name, file_type, rcept_no)
                
        except Exception as e:
            print(f"❌ Excel 업로드 실패: {str(e)}")
            self.results['failed_uploads'].append(file_path)

    def _upload_sheet_to_google(self, worksheet, sheet_name, file_type, rcept_no):
        """개별 시트를 Google Sheets에 업로드"""
        try:
            # 데이터 추출
            data = []
            for row in worksheet.iter_rows(values_only=True):
                row_data = [str(cell) if cell is not None else '' for cell in row]
                if any(row_data):  # 빈 행 제외
                    data.append(row_data)
            
            if not data:
                print(f"⚠️ 시트 '{sheet_name}'에 데이터가 없습니다.")
                return
            
            # Google Sheets 시트 이름 생성
            gsheet_name = f"{file_type}_{sheet_name.replace(' ', '_')}"
            if len(gsheet_name) > 100:
                gsheet_name = gsheet_name[:97] + "..."
            
            # Google Sheets에 시트 생성 또는 업데이트
            try:
                gsheet = self.workbook.worksheet(gsheet_name)
                gsheet.clear()  # 기존 데이터 삭제
            except gspread.exceptions.WorksheetNotFound:
                rows = max(1000, len(data) + 100)
                cols = max(26, len(data[0]) + 5) if data else 26
                gsheet = self.workbook.add_worksheet(gsheet_name, rows, cols)
            
            # 헤더 추가
            header = [
                [f"업데이트: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"],
                [f"보고서: {rcept_no}"],
                [f"원본 시트: {sheet_name}"],
                []
            ]
            
            # 데이터 업로드
            all_data = header + data
            
            # 배치로 업로드
            batch_size = 100
            for i in range(0, len(all_data), batch_size):
                batch = all_data[i:i + batch_size]
                gsheet.append_rows(batch)
                time.sleep(1)  # API 제한 회피
            
            print(f"✅ 업로드 완료: {gsheet_name} ({len(data)}행)")
            self.results['uploaded_sheets'].append(gsheet_name)
            
        except Exception as e:
            print(f"❌ 시트 업로드 실패 '{sheet_name}': {str(e)}")
            self.results['failed_uploads'].append(sheet_name)

    def _update_archive(self):
        """Archive 시트 업데이트 (간소화된 버전)"""
        try:
            print("\n📊 Archive 시트 업데이트 확인 중...")
            
            # Archive 시트가 있는지 확인만
            try:
                archive = self.workbook.worksheet('Dart_Archive')
                print("✅ Dart_Archive 시트 존재 확인")
                # 실제 Archive 업데이트 로직은 필요시 구현
            except gspread.exceptions.WorksheetNotFound:
                print("ℹ️ Dart_Archive 시트가 없습니다. 건너뜁니다.")
                
        except Exception as e:
            print(f"⚠️ Archive 시트 확인 중 오류: {str(e)}")

    def _cleanup_downloads(self):
        """다운로드 폴더 정리"""
        try:
            if os.path.exists(self.download_dir):
                shutil.rmtree(self.download_dir)
                print("🧹 다운로드 폴더 정리 완료")
        except Exception as e:
            print(f"⚠️ 다운로드 폴더 정리 실패: {str(e)}")

    def _print_summary(self):
        """처리 결과 요약"""
        print("\n" + "="*50)
        print("📊 처리 결과 요약")
        print("="*50)
        print(f"전체 보고서: {self.results['total_reports']}개")
        print(f"다운로드 성공: {len(self.results['downloaded_files'])}개")
        print(f"업로드된 시트: {len(self.results['uploaded_sheets'])}개")
        print(f"다운로드 실패: {len(self.results['failed_downloads'])}개")
        print(f"업로드 실패: {len(self.results['failed_uploads'])}개")
        
        # 텔레그램 메시지 전송
        if self.telegram_bot_token and self.telegram_channel_id:
            self._send_telegram_summary()

    def _send_telegram_summary(self):
        """텔레그램 요약 메시지 전송"""
        try:
            import requests
            
            message = (
                f"📊 DART 재무제표 다운로드 완료\n\n"
                f"• 종목: {self.company_name} ({self.corp_code})\n"
                f"• 처리 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"• 전체 보고서: {self.results['total_reports']}개\n"
                f"• 다운로드 성공: {len(self.results['downloaded_files'])}개\n"
                f"• 업로드된 시트: {len(self.results['uploaded_sheets'])}개"
            )
            
            url = f"https://api.telegram.org/bot{self.telegram_bot_token}/sendMessage"
            data = {
                "chat_id": self.telegram_channel_id,
                "text": message,
                "parse_mode": "HTML"
            }
            requests.post(url, data=data)
            print("📱 텔레그램 메시지 전송 완료")
            
        except Exception as e:
            print(f"📱 텔레그램 메시지 전송 실패: {str(e)}")


def load_company_config():
    """회사 설정 로드"""
    # 환경변수에서 읽기
    corp_code = os.environ.get('COMPANY_CORP_CODE', '307950')
    company_name = os.environ.get('COMPANY_NAME', '현대오토에버')
    spreadsheet_var = os.environ.get('COMPANY_SPREADSHEET_VAR', 'AUTOEVER_SPREADSHEET_ID')
    
    return {
        'corp_code': corp_code,
        'company_name': company_name,
        'spreadsheet_var': spreadsheet_var
    }


def main():
    """메인 실행 함수"""
    try:
        # Playwright 설치 확인
        print("🔧 Playwright 브라우저 설치 확인...")
        os.system("playwright install chromium")
        
        # 회사 설정 로드
        company_config = load_company_config()
        
        print(f"🤖 DART 재무제표 Excel 다운로드 시스템")
        print(f"🏢 대상 기업: {company_config['company_name']} ({company_config['corp_code']})")
        
        # 다운로더 실행
        downloader = DartExcelDownloader(company_config)
        downloader.run()
        
        print("\n✅ 모든 작업이 완료되었습니다!")
        
    except Exception as e:
        print(f"\n❌ 오류 발생: {str(e)}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()
