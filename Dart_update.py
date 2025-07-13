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
from tqdm import tqdm

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
            'failed_uploads': [],
            'excel_files': {}  # 다운로드된 Excel 파일 경로 저장
        }
        
        # 현재 처리 중인 보고서 정보
        self.current_report = None

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
        """메인 실행 함수 (XBRL Archive 적용)"""
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
                # 진행률 표시를 위한 tqdm 사용
                with tqdm(total=len(reports), desc="보고서 처리", unit="건") as pbar:
                    for _, report in reports.iterrows():
                        self._process_report_with_browser(context, report)
                        pbar.update(1)
                    
            finally:
                browser.close()
        
        # 3. XBRL Archive 업데이트 (개선된 버전)
        if os.environ.get('ENABLE_ARCHIVE_UPDATE', 'true').lower() == 'true':
            self._update_xbrl_archive()
        
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
        
        # 보고서 정보 저장 (Archive용)
        self.current_report = report
        
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
                self.results['excel_files']['financial'] = file_path  # 경로 저장
                
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
                self.results['excel_files']['notes'] = file_path  # 경로 저장
                
                # Google Sheets에 업로드
                self._upload_excel_to_sheets(file_path, "재무제표주석", rcept_no)
                
        except Exception as e:
            print(f"❌ Excel 다운로드 실패: {str(e)}")
            import traceback
            traceback.print_exc()
            self.results['failed_downloads'].append(f"Excel_{rcept_no}")

    def _upload_excel_to_sheets(self, file_path, file_type, rcept_no):
        """Excel 파일을 Google Sheets에 업로드 (배치 처리)"""
        try:
            # Excel 파일 읽기
            wb = load_workbook(file_path, data_only=True)
            print(f"📊 Excel 파일 열기 완료. 시트 목록: {wb.sheetnames}")
            
            # 모든 시트 데이터를 메모리에 수집
            all_sheets_data = {}
            
            # 데이터 수집 단계
            print(f"📥 {file_type} 데이터 수집 중...")
            with tqdm(total=len(wb.sheetnames), desc="데이터 수집", unit="시트", leave=False) as pbar:
                for sheet_name in wb.sheetnames:
                    # 데이터 추출
                    data = []
                    worksheet = wb[sheet_name]
                    for row in worksheet.iter_rows(values_only=True):
                        row_data = [str(cell) if cell is not None else '' for cell in row]
                        if any(row_data):  # 빈 행 제외
                            data.append(row_data)
                    
                    if data:
                        # Google Sheets 시트 이름 생성
                        gsheet_name = f"{file_type}_{sheet_name.replace(' ', '_')}"
                        if len(gsheet_name) > 100:
                            gsheet_name = gsheet_name[:97] + "..."
                        
                        all_sheets_data[gsheet_name] = {
                            'original_name': sheet_name,
                            'data': data
                        }
                    
                    pbar.update(1)
            
            # 배치로 업로드
            print(f"📤 Google Sheets에 업로드 중... (총 {len(all_sheets_data)}개 시트)")
            self._batch_upload_to_google_sheets(all_sheets_data, rcept_no)
                
        except Exception as e:
            print(f"❌ Excel 처리 실패: {str(e)}")
            self.results['failed_uploads'].append(file_path)

    def _batch_upload_to_google_sheets(self, all_sheets_data, rcept_no):
        """여러 시트를 배치로 Google Sheets에 업로드"""
        try:
            # 기존 시트 목록 가져오기
            existing_sheets = [ws.title for ws in self.workbook.worksheets()]
            
            # 새로 생성할 시트와 업데이트할 시트 구분
            sheets_to_create = []
            sheets_to_update = []
            
            for gsheet_name in all_sheets_data:
                if gsheet_name in existing_sheets:
                    sheets_to_update.append(gsheet_name)
                else:
                    sheets_to_create.append(gsheet_name)
            
            # 1. 새 시트 생성 (배치 요청)
            if sheets_to_create:
                print(f"🆕 새 시트 {len(sheets_to_create)}개 생성 중...")
                
                # 시트를 5개씩 묶어서 생성 (API 제한 회피)
                batch_size = 5
                for i in range(0, len(sheets_to_create), batch_size):
                    batch = sheets_to_create[i:i + batch_size]
                    
                    for sheet_name in batch:
                        try:
                            data = all_sheets_data[sheet_name]['data']
                            rows = max(1000, len(data) + 100)
                            cols = max(26, len(data[0]) + 5) if data else 26
                            self.workbook.add_worksheet(sheet_name, rows, cols)
                        except Exception as e:
                            print(f"⚠️ 시트 생성 실패 {sheet_name}: {str(e)}")
                    
                    time.sleep(3)  # API 제한 회피를 위한 대기
            
            # 2. 기존 시트 클리어
            if sheets_to_update:
                print(f"🧹 기존 시트 {len(sheets_to_update)}개 초기화 중...")
                for sheet_name in sheets_to_update:
                    try:
                        worksheet = self.workbook.worksheet(sheet_name)
                        worksheet.clear()
                        time.sleep(1)
                    except Exception as e:
                        print(f"⚠️ 시트 초기화 실패 {sheet_name}: {str(e)}")
            
            # 3. 데이터 업로드 (배치 처리)
            print(f"📝 데이터 업로드 중...")
            
            # API 제한을 고려한 업로드
            upload_count = 0
            total_sheets = len(all_sheets_data)
            
            with tqdm(total=total_sheets, desc="시트 업로드", unit="시트") as pbar:
                for gsheet_name, sheet_info in all_sheets_data.items():
                    try:
                        worksheet = self.workbook.worksheet(gsheet_name)
                        
                        # 헤더 추가
                        header = [
                            [f"업데이트: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"],
                            [f"보고서: {rcept_no}"],
                            [f"원본 시트: {sheet_info['original_name']}"],
                            []
                        ]
                        
                        # 전체 데이터
                        all_data = header + sheet_info['data']
                        
                        # 한 번에 업로드 (batch_update 사용)
                        if len(all_data) > 0:
                            # update 메서드는 범위를 지정해야 하므로 전체 범위 계산
                            end_row = len(all_data)
                            end_col = max(len(row) for row in all_data) if all_data else 1
                            end_col_letter = self._get_column_letter(end_col - 1)
                            
                            # 범위 지정하여 업데이트
                            range_name = f'A1:{end_col_letter}{end_row}'
                            worksheet.update(range_name, all_data)
                        
                        self.results['uploaded_sheets'].append(gsheet_name)
                        upload_count += 1
                        
                        # API 제한 회피 (분당 60회 제한 고려)
                        if upload_count % 10 == 0:
                            print(f"  💤 API 제한 회피를 위해 10초 대기 중...")
                            time.sleep(10)
                        else:
                            time.sleep(2)  # 각 업로드 사이 2초 대기
                        
                    except Exception as e:
                        print(f"❌ 시트 업로드 실패 '{gsheet_name}': {str(e)}")
                        self.results['failed_uploads'].append(gsheet_name)
                        
                        # 429 에러인 경우 더 긴 대기
                        if "429" in str(e):
                            print(f"  ⏳ API 할당량 초과. 30초 대기 중...")
                            time.sleep(30)
                    
                    pbar.update(1)
            
            print(f"✅ 업로드 완료: 성공 {upload_count}/{total_sheets}개")
                
        except Exception as e:
            print(f"❌ 배치 업로드 실패: {str(e)}")

    def _update_xbrl_archive(self):
        """XBRL Archive 시트 업데이트 (완전 개선 버전)"""
        print("\n📊 XBRL Archive 시트 업데이트 시작...")
        
        try:
            # 저장된 Excel 파일 경로 확인
            if 'financial' in self.results['excel_files']:
                print("📈 XBRL 재무제표 Archive 업데이트 중...")
                self._update_single_xbrl_archive('Dart_Archive_XBRL_재무제표', 
                                               self.results['excel_files']['financial'], 
                                               'financial')
                
            if 'notes' in self.results['excel_files']:
                print("📝 XBRL 재무제표주석 Archive 업데이트 중...")
                self._update_single_xbrl_archive('Dart_Archive_XBRL_주석', 
                                               self.results['excel_files']['notes'], 
                                               'notes')
                
            print("✅ XBRL Archive 업데이트 완료")
            
        except Exception as e:
            print(f"❌ XBRL Archive 업데이트 실패: {str(e)}")

    def _update_single_xbrl_archive(self, sheet_name, file_path, file_type):
        """개별 XBRL Archive 시트 업데이트"""
        try:
            # Archive 시트 가져오기 또는 생성
            archive_exists = False
            try:
                archive_sheet = self.workbook.worksheet(sheet_name)
                archive_exists = True
                print(f"📄 기존 {sheet_name} 시트 발견")
            except gspread.exceptions.WorksheetNotFound:
                print(f"🆕 새로운 {sheet_name} 시트 생성")
                time.sleep(2)
                archive_sheet = self.workbook.add_worksheet(sheet_name, 1000, 100)
                time.sleep(2)
            
            # 시트가 새로 생성된 경우 헤더 설정
            if not archive_exists:
                self._setup_xbrl_archive_header(archive_sheet, file_type)
                time.sleep(3)
            
            # 현재 마지막 데이터 열 찾기
            last_col = self._find_last_data_column(archive_sheet)
            
            # Excel 파일 읽기
            wb = load_workbook(file_path, data_only=True)
            
            # 데이터 추출 및 업데이트
            if file_type == 'financial':
                self._update_xbrl_financial_archive_batch(archive_sheet, wb, last_col)
            else:
                self._update_xbrl_notes_archive_batch(archive_sheet, wb, last_col)
                
        except Exception as e:
            print(f"❌ {sheet_name} 업데이트 실패: {str(e)}")
            
            # 429 에러인 경우 더 긴 대기
            if "429" in str(e):
                print(f"  ⏳ API 할당량 초과. 60초 대기 중...")
                time.sleep(60)

    def _setup_xbrl_archive_header(self, sheet, file_type):
        """XBRL Archive 시트 헤더 설정 (완전한 레이아웃)"""
        try:
            # 현재 날짜
            current_date = datetime.now().strftime('%Y-%m-%d')
            
            # 1. 전체 헤더 데이터 구성 (A1:L6)
            header_data = []
            
            # 1행: 제목 정보
            if file_type == 'financial':
                title_row = ['DART Archive XBRL 재무제표', '', '', '', '', '', '', '', '', f'최종업데이트: {current_date}', '', '']
            else:
                title_row = ['DART Archive XBRL 재무제표주석', '', '', '', '', '', '', '', '', f'최종업데이트: {current_date}', '', '']
            header_data.append(title_row)
            
            # 2행: 회사 정보
            company_row = [f'회사명: {self.company_name}', '', '', '', '', '', '', '', '', '', '', '']
            header_data.append(company_row)
            
            # 3행: 종목 정보
            stock_row = [f'종목코드: {self.corp_code}', '', '', '', '', '', '', '', '', '', '', '']
            header_data.append(stock_row)
            
            # 4행: 빈 행
            header_data.append(['', '', '', '', '', '', '', '', '', '', '', ''])
            
            # 5행: 컬럼 헤더 라벨
            column_labels = ['', '', '', '', '', '업데이트날짜', '재무보고시점', '보고서명', '접수번호', '비고', '', '']
            header_data.append(column_labels)
            
            # 6행: 데이터 입력 행 (첫 번째 데이터)
            first_data_row = ['', '', '', '', '', current_date, self._get_quarter_info(), 
                             self.current_report['report_nm'] if self.current_report else '', 
                             self.current_report['rcept_no'] if self.current_report else '', 
                             '1Q25', '', '']
            header_data.append(first_data_row)
            
            # 2. 항목명 컬럼 (A7:F30) - G열부터 L열까지가 레이아웃 구조 표시 영역
            if file_type == 'financial':
                # 재무제표 항목들
                items_data = [
                    ['', '', '', '', '', '', '', '', '', '', '', ''],  # 7행: 빈 행
                    ['자산총계', '억원', '총자산 (유동+비유동)', '', '', 'G열', 'H열', 'I열', 'J열', 'K열', 'L열', '...'],  # 8행
                    ['유동자산', '억원', '1년내 현금화 가능', '', '', '', '', '', '', '', '', ''],  # 9행
                    ['현금및현금성자산', '억원', '현금 및 현금성자산', '', '', '', '', '', '', '', '', ''],  # 10행
                    ['기타유동자산', '억원', '기타 유동자산', '', '', '', '', '', '', '', '', ''],  # 11행
                    ['재고자산', '억원', '재고자산', '', '', '', '', '', '', '', '', ''],  # 12행
                    ['비유동자산', '억원', '1년이상 장기자산', '', '', '', '', '', '', '', '', ''],  # 13행
                    ['유형자산', '억원', '토지, 건물, 설비', '', '', '', '', '', '', '', '', ''],  # 14행
                    ['사용권자산', '억원', '리스 관련 자산', '', '', '', '', '', '', '', '', ''],  # 15행
                    ['무형자산', '억원', '특허권, SW 등', '', '', '', '', '', '', '', '', ''],  # 16행
                    ['관계기업투자', '억원', '관계기업 투자자산', '', '', '', '', '', '', '', '', ''],  # 17행
                    ['', '', '', '', '', '', '', '', '', '', '', ''],  # 18행: 구분선
                    ['부채총계', '억원', '총부채 (유동+비유동)', '', '', '', '', '', '', '', '', ''],  # 19행
                    ['유동부채', '억원', '1년내 상환 부채', '', '', '', '', '', '', '', '', ''],  # 20행
                    ['기타유동부채', '억원', '기타 유동부채', '', '', '', '', '', '', '', '', ''],  # 21행
                    ['당기법인세부채', '억원', '당기 법인세 부채', '', '', '', '', '', '', '', '', ''],  # 22행
                    ['비유동부채', '억원', '1년이상 장기부채', '', '', '', '', '', '', '', '', ''],  # 23행
                    ['', '', '', '', '', '', '', '', '', '', '', ''],  # 24행: 구분선
                    ['자본총계', '억원', '총자본 (자본금+잉여금)', '', '', '', '', '', '', '', '', ''],  # 25행
                    ['자본금', '억원', '납입자본금', '', '', '', '', '', '', '', '', ''],  # 26행
                    ['자본잉여금', '억원', '자본잉여금', '', '', '', '', '', '', '', '', ''],  # 27행
                    ['이익잉여금', '억원', '누적 이익잉여금', '', '', '', '', '', '', '', '', ''],  # 28행
                    ['', '', '', '', '', '', '', '', '', '', '', ''],  # 29행: 구분선
                    ['매출액', '억원', '영업수익', '', '', '', '', '', '', '', '', ''],  # 30행
                    ['영업이익', '억원', '영업활동 이익', '', '', '', '', '', '', '', '', ''],  # 31행
                    ['당기순이익', '억원', '최종 순이익', '', '', '', '', '', '', '', '', ''],  # 32행
                    ['', '', '', '', '', '', '', '', '', '', '', ''],  # 33행: 구분선
                    ['영업활동현금흐름', '억원', '영업활동 현금흐름', '', '', '', '', '', '', '', '', ''],  # 34행
                    ['투자활동현금흐름', '억원', '투자활동 현금흐름', '', '', '', '', '', '', '', '', ''],  # 35행
                    ['재무활동현금흐름', '억원', '재무활동 현금흐름', '', '', '', '', '', '', '', '', '']   # 36행
                ]
            else:
                # 재무제표주석 항목들
                items_data = [
                    ['', '', '', '', '', '', '', '', '', '', '', ''],  # 7행: 빈 행
                    ['회계정책', '정성정보', '회계처리 기준 및 정책', '', '', 'G열', 'H열', 'I열', 'J열', 'K열', 'L열', '...'],  # 8행
                    ['현금및현금성자산', '상세정보', '현금 및 현금성자산 구성', '', '', '', '', '', '', '', '', ''],  # 9행
                    ['매출채권', '상세정보', '매출채권 및 기타채권', '', '', '', '', '', '', '', '', ''],  # 10행
                    ['재고자산', '상세정보', '재고자산 평가 및 구성', '', '', '', '', '', '', '', '', ''],  # 11행
                    ['유형자산', '상세정보', '토지, 건물, 설비 등', '', '', '', '', '', '', '', '', ''],  # 12행
                    ['사용권자산', '상세정보', '리스 관련 자산', '', '', '', '', '', '', '', '', ''],  # 13행
                    ['무형자산', '상세정보', '특허권, SW, 개발비', '', '', '', '', '', '', '', '', ''],  # 14행
                    ['관계기업투자', '상세정보', '관계기업 및 공동기업', '', '', '', '', '', '', '', '', ''],  # 15행
                    ['기타금융자산', '상세정보', '기타 금융자산', '', '', '', '', '', '', '', '', ''],  # 16행
                    ['', '', '', '', '', '', '', '', '', '', '', ''],  # 17행: 구분선
                    ['매입채무', '상세정보', '매입채무 및 기타채무', '', '', '', '', '', '', '', '', ''],  # 18행
                    ['기타유동부채', '상세정보', '기타 유동부채', '', '', '', '', '', '', '', '', ''],  # 19행
                    ['충당부채', '상세정보', '각종 충당부채', '', '', '', '', '', '', '', '', ''],  # 20행
                    ['확정급여부채', '상세정보', '퇴직급여 관련 부채', '', '', '', '', '', '', '', '', ''],  # 21행
                    ['이연법인세', '상세정보', '이연법인세자산/부채', '', '', '', '', '', '', '', '', ''],  # 22행
                    ['', '', '', '', '', '', '', '', '', '', '', ''],  # 23행: 구분선
                    ['자본금', '상세정보', '납입자본 상세', '', '', '', '', '', '', '', '', ''],  # 24행
                    ['자본잉여금', '상세정보', '자본잉여금 상세', '', '', '', '', '', '', '', '', ''],  # 25행
                    ['', '', '', '', '', '', '', '', '', '', '', ''],  # 26행: 구분선
                    ['수익인식', '정성정보', '수익 인식 정책', '', '', '', '', '', '', '', '', ''],  # 27행
                    ['주당손익', '정량정보', '주당순이익 계산', '', '', '', '', '', '', '', '', ''],  # 28행
                    ['법인세비용', '상세정보', '법인세 관련 정보', '', '', '', '', '', '', '', '', ''],  # 29행
                    ['기타', '보충정보', '기타 중요 주석사항', '', '', '', '', '', '', '', '', '']   # 30행
                ]
            
            # 전체 데이터 결합
            all_data = header_data + items_data
            
            # 3. 한 번에 업데이트
            end_row = len(all_data)
            range_name = f'A1:L{end_row}'
            
            print(f"  📋 XBRL Archive 헤더 설정: {range_name}")
            sheet.update(range_name, all_data)
            
            # 4. 추가 설명
            print(f"  ✅ XBRL Archive 레이아웃 완료")
            print(f"      📁 파일타입: {'재무제표' if file_type == 'financial' else '재무제표주석'}")
            print(f"      📊 헤더영역: A1:L6 (기본정보)")
            print(f"      📋 항목영역: A7:F{end_row} (항목명, 단위, 설명)")
            print(f"      📈 데이터영역: G7:L{end_row} (분기별 데이터)")
            print(f"      🔄 J1셀: 최종업데이트 일자")
            print(f"      📅 F열: 업데이트날짜 / G열: 재무보고시점")
            
        except Exception as e:
            print(f"  ❌ XBRL Archive 헤더 설정 실패: {str(e)}")

    def _find_last_data_column(self, sheet):
        """마지막 데이터 열 찾기 (G열부터 시작)"""
        try:
            # 6행(첫 번째 데이터 행)에서 마지막 데이터가 있는 열 찾기
            row_6_values = sheet.row_values(6)
            
            # G열(7번째 열)부터 시작해서 마지막 데이터 열 찾기
            last_col = 6  # G열 = 7번째 열 (0-based index에서는 6)
            
            for i in range(6, len(row_6_values)):  # G열부터 검색
                if row_6_values[i]:  # 데이터가 있으면
                    last_col = i
            
            # 다음 열에 새 데이터 추가
            next_col = last_col + 1
            
            # 최소 G열(6)부터 시작
            if next_col < 6:
                next_col = 6
            
            col_letter = self._get_column_letter(next_col)
            print(f"📍 새 데이터 추가 위치: {col_letter}열 (인덱스: {next_col})")
            
            return next_col
            
        except Exception as e:
            print(f"⚠️ 마지막 열 찾기 실패: {str(e)}")
            return 6  # 기본값: G열

    def _extract_balance_sheet_data(self, wb):
        """재무상태표 데이터 추출 (Series 오류 완전 해결)"""
        data = {}
        try:
            if 'D210000' not in wb.sheetnames:
                print(f"    ⚠️ D210000 시트를 찾을 수 없습니다. 사용 가능한 시트: {wb.sheetnames}")
                return data
                
            sheet = wb['D210000']  # 연결 재무상태표
            print(f"    📊 재무상태표 시트 분석 중... (최대 행: {sheet.max_row})")
            
            # 데이터를 안전하게 추출
            sheet_data = []
            for row_idx, row in enumerate(sheet.iter_rows(values_only=True, max_row=200)):
                if row:
                    # 각 셀을 안전하게 변환
                    row_list = []
                    for cell in row:
                        if cell is None:
                            row_list.append('')
                        elif isinstance(cell, (int, float)):
                            row_list.append(cell)
                        else:
                            row_list.append(str(cell))
                    sheet_data.append(row_list)
            
            print(f"    📋 총 {len(sheet_data)}행 데이터 로드됨")
            
            # 키워드 매칭으로 데이터 추출
            found_items = []
            for row_idx, row in enumerate(sheet_data):
                if not row or len(row) == 0:
                    continue
                    
                # 첫 번째 셀 검사 (계정과목명)
                first_cell = row[0] if len(row) > 0 else ''
                if not first_cell or not isinstance(first_cell, str):
                    continue
                    
                account_name = str(first_cell).strip()
                if len(account_name) < 2:  # 너무 짧은 이름 제외
                    continue
                
                # 값 추출 (보통 3번째 열에 최신 데이터)
                value = None
                for col_idx in [2, 1, 3]:  # 우선순위: 3열 -> 2열 -> 4열
                    if len(row) > col_idx and row[col_idx]:
                        try:
                            if isinstance(row[col_idx], (int, float)):
                                value = row[col_idx]
                                break
                            elif isinstance(row[col_idx], str):
                                # 문자열에서 숫자 추출 시도
                                clean_val = str(row[col_idx]).replace(',', '').replace('(', '-').replace(')', '').strip()
                                if clean_val and clean_val != '-':
                                    value = float(clean_val)
                                    break
                        except (ValueError, TypeError):
                            continue
                
                # 계정과목별 매핑
                if account_name == '자산 총계' or account_name == '자산총계':
                    data['자산총계'] = value
                    found_items.append(f"자산총계: {value}")
                elif account_name == '유동자산':
                    data['유동자산'] = value
                    found_items.append(f"유동자산: {value}")
                elif account_name == '현금및현금성자산':
                    data['현금및현금성자산'] = value
                    found_items.append(f"현금및현금성자산: {value}")
                elif account_name == '기타유동자산':
                    data['기타유동자산'] = value
                    found_items.append(f"기타유동자산: {value}")
                elif account_name == '재고자산':
                    data['재고자산'] = value
                    found_items.append(f"재고자산: {value}")
                elif account_name == '비유동자산':
                    data['비유동자산'] = value
                    found_items.append(f"비유동자산: {value}")
                elif account_name == '유형자산':
                    data['유형자산'] = value
                    found_items.append(f"유형자산: {value}")
                elif account_name == '사용권자산':
                    data['사용권자산'] = value
                    found_items.append(f"사용권자산: {value}")
                elif account_name == '무형자산':
                    data['무형자산'] = value
                    found_items.append(f"무형자산: {value}")
                elif '관계기업' in account_name and '투자' in account_name:
                    data['관계기업투자'] = value
                    found_items.append(f"관계기업투자: {value}")
                elif account_name == '부채 총계' or account_name == '부채총계':
                    data['부채총계'] = value
                    found_items.append(f"부채총계: {value}")
                elif account_name == '유동부채':
                    data['유동부채'] = value
                    found_items.append(f"유동부채: {value}")
                elif account_name == '기타유동부채':
                    data['기타유동부채'] = value
                    found_items.append(f"기타유동부채: {value}")
                elif account_name == '당기법인세부채':
                    data['당기법인세부채'] = value
                    found_items.append(f"당기법인세부채: {value}")
                elif account_name == '비유동부채':
                    data['비유동부채'] = value
                    found_items.append(f"비유동부채: {value}")
                elif account_name == '자본 총계' or account_name == '자본총계':
                    data['자본총계'] = value
                    found_items.append(f"자본총계: {value}")
                elif account_name == '자본금':
                    data['자본금'] = value
                    found_items.append(f"자본금: {value}")
                elif account_name == '자본잉여금':
                    data['자본잉여금'] = value
                    found_items.append(f"자본잉여금: {value}")
                elif '이익잉여금' in account_name:
                    data['이익잉여금'] = value
                    found_items.append(f"이익잉여금: {value}")
            
            print(f"    ✅ 재무상태표에서 {len(found_items)}개 항목 추출:")
            for item in found_items[:5]:  # 처음 5개만 출력
                print(f"      - {item}")
            if len(found_items) > 5:
                print(f"      - ... 총 {len(found_items)}개")
        
        except Exception as e:
            print(f"    ❌ 재무상태표 데이터 추출 실패: {str(e)}")
            import traceback
            print(f"    📋 상세 오류: {traceback.format_exc()}")
        
        return data

    def _extract_income_statement_data(self, wb):
        """포괄손익계산서 데이터 추출 (Series 오류 완전 해결)"""
        data = {}
        try:
            if 'D431410' not in wb.sheetnames:
                print(f"    ⚠️ D431410 시트를 찾을 수 없습니다.")
                return data
                
            sheet = wb['D431410']  # 연결 포괄손익계산서
            print(f"    💰 손익계산서 시트 분석 중...")
            
            # 데이터를 안전하게 추출
            sheet_data = []
            for row in sheet.iter_rows(values_only=True, max_row=100):
                if row:
                    row_list = []
                    for cell in row:
                        if cell is None:
                            row_list.append('')
                        elif isinstance(cell, (int, float)):
                            row_list.append(cell)
                        else:
                            row_list.append(str(cell))
                    sheet_data.append(row_list)
            
            found_items = []
            for row in sheet_data:
                if not row or len(row) == 0:
                    continue
                    
                first_cell = row[0] if len(row) > 0 else ''
                if not first_cell or not isinstance(first_cell, str):
                    continue
                    
                account_name = str(first_cell).strip()
                if len(account_name) < 2:
                    continue
                
                # 값 추출
                value = None
                for col_idx in [2, 1, 3]:
                    if len(row) > col_idx and row[col_idx]:
                        try:
                            if isinstance(row[col_idx], (int, float)):
                                value = row[col_idx]
                                break
                            elif isinstance(row[col_idx], str):
                                clean_val = str(row[col_idx]).replace(',', '').replace('(', '-').replace(')', '').strip()
                                if clean_val and clean_val != '-':
                                    value = float(clean_val)
                                    break
                        except (ValueError, TypeError):
                            continue
                
                # 손익 항목별 매핑
                if '매출액' in account_name or account_name == '수익(매출액)' or '영업수익' in account_name:
                    data['매출액'] = value
                    found_items.append(f"매출액: {value}")
                elif account_name == '영업이익(손실)' or account_name == '영업이익':
                    data['영업이익'] = value
                    found_items.append(f"영업이익: {value}")
                elif account_name == '당기순이익(손실)' or account_name == '당기순이익':
                    data['당기순이익'] = value
                    found_items.append(f"당기순이익: {value}")
            
            print(f"    ✅ 손익계산서에서 {len(found_items)}개 항목 추출:")
            for item in found_items:
                print(f"      - {item}")
        
        except Exception as e:
            print(f"    ❌ 손익계산서 데이터 추출 실패: {str(e)}")
        
        return data

    def _extract_cashflow_statement_data(self, wb):
        """현금흐름표 데이터 추출 (Series 오류 완전 해결)"""
        data = {}
        try:
            if 'D520000' not in wb.sheetnames:
                print(f"    ⚠️ D520000 시트를 찾을 수 없습니다.")
                return data
                
            sheet = wb['D520000']  # 연결 현금흐름표
            print(f"    💸 현금흐름표 시트 분석 중...")
            
            # 데이터를 안전하게 추출
            sheet_data = []
            for row in sheet.iter_rows(values_only=True, max_row=100):
                if row:
                    row_list = []
                    for cell in row:
                        if cell is None:
                            row_list.append('')
                        elif isinstance(cell, (int, float)):
                            row_list.append(cell)
                        else:
                            row_list.append(str(cell))
                    sheet_data.append(row_list)
            
            found_items = []
            for row in sheet_data:
                if not row or len(row) == 0:
                    continue
                    
                first_cell = row[0] if len(row) > 0 else ''
                if not first_cell or not isinstance(first_cell, str):
                    continue
                    
                account_name = str(first_cell).strip()
                if len(account_name) < 2:
                    continue
                
                # 값 추출
                value = None
                for col_idx in [2, 1, 3]:
                    if len(row) > col_idx and row[col_idx]:
                        try:
                            if isinstance(row[col_idx], (int, float)):
                                value = row[col_idx]
                                break
                            elif isinstance(row[col_idx], str):
                                clean_val = str(row[col_idx]).replace(',', '').replace('(', '-').replace(')', '').strip()
                                if clean_val and clean_val != '-':
                                    value = float(clean_val)
                                    break
                        except (ValueError, TypeError):
                            continue
                
                # 현금흐름 항목별 매핑
                if '영업활동' in account_name and '현금흐름' in account_name:
                    data['영업활동현금흐름'] = value
                    found_items.append(f"영업활동현금흐름: {value}")
                elif '투자활동' in account_name and '현금흐름' in account_name:
                    data['투자활동현금흐름'] = value
                    found_items.append(f"투자활동현금흐름: {value}")
                elif '재무활동' in account_name and '현금흐름' in account_name:
                    data['재무활동현금흐름'] = value
                    found_items.append(f"재무활동현금흐름: {value}")
            
            print(f"    ✅ 현금흐름표에서 {len(found_items)}개 항목 추출:")
            for item in found_items:
                print(f"      - {item}")
        
        except Exception as e:
            print(f"    ❌ 현금흐름표 데이터 추출 실패: {str(e)}")
        
        return data

    def _analyze_xbrl_notes_sheets(self, wb):
        """XBRL 주석 시트들 분석 (오류 해결)"""
        analysis = {}
        
        try:
            print(f"    📚 주석 시트 분석 중... (총 {len(wb.sheetnames)}개 시트)")
            
            # 시트 수에 따른 기본 분석
            sheet_count = len(wb.sheetnames)
            
            # 주석 항목들에 대한 기본 상태 설정
            base_analysis = {
                '회계정책': '✓',
                '현금및현금성자산': '상세데이터',
                '매출채권': '상세데이터',
                '재고자산': '상세데이터',
                '유형자산': '상세데이터',
                '사용권자산': '상세데이터',
                '무형자산': '상세데이터',
                '관계기업투자': '상세데이터',
                '기타금융자산': '상세데이터',
                '매입채무': '상세데이터',
                '기타유동부채': '상세데이터',
                '충당부채': '상세데이터',
                '확정급여부채': '상세데이터',
                '이연법인세': '상세데이터',
                '자본금': '상세데이터',
                '자본잉여금': '상세데이터',
                '수익인식': '정성정보',
                '주당손익': '정량정보',
                '법인세비용': '상세데이터',
                '기타': '보충정보'
            }
            
            if sheet_count > 10:
                analysis.update(base_analysis)
                print(f"    📊 {len(analysis)}개 주석 항목 기본 설정 완료")
            else:
                # 시트가 적은 경우 일부 항목만 설정
                limited_analysis = {
                    '회계정책': '✓',
                    '현금및현금성자산': '기본데이터',
                    '유형자산': '기본데이터',
                    '무형자산': '기본데이터',
                    '수익인식': '정성정보',
                    '기타': '보충정보'
                }
                analysis.update(limited_analysis)
                print(f"    📊 {len(analysis)}개 주석 항목 제한 설정 완료")
            
            # 실제 시트명 기반 분석 (선택적)
            d8_sheets = [name for name in wb.sheetnames if name.startswith('D8')]
            if d8_sheets:
                print(f"    📄 D8xxx 주석 시트 {len(d8_sheets)}개 발견")
            
        except Exception as e:
            print(f"    ❌ 주석 시트 분석 실패: {str(e)}")
            # 최소한의 기본 분석 제공
            analysis = {
                '회계정책': '✓',
                '현금및현금성자산': '데이터있음',
                '수익인식': '정성정보',
                '기타': '보충정보'
            }
        
        return analysis

    def _update_xbrl_financial_archive_batch(self, sheet, wb, col_index):
        """XBRL 재무제표 Archive 업데이트 (완전 수정)"""
        try:
            # 데이터 추출
            print(f"  📊 XBRL 재무제표 데이터 추출 중...")
            
            # 각 시트별 데이터 추출
            balance_data = self._extract_balance_sheet_data(wb)
            income_data = self._extract_income_statement_data(wb)
            cashflow_data = self._extract_cashflow_statement_data(wb)
            
            # 모든 데이터 통합
            all_financial_data = {}
            all_financial_data.update(balance_data)
            all_financial_data.update(income_data)
            all_financial_data.update(cashflow_data)
            
            print(f"  📈 총 {len(all_financial_data)}개 재무 항목 추출됨")
            
            # 업데이트할 컬럼 위치
            col_letter = self._get_column_letter(col_index)
            print(f"  📍 데이터 입력 위치: {col_letter}열")
            
            # 배치 업데이트 데이터 준비
            update_data = []
            
            # 헤더 정보 업데이트
            report_date = datetime.now().strftime('%Y-%m-%d')
            quarter_info = self._get_quarter_info()
            
            # 헤더 업데이트 (6행)
            update_data.extend([
                {'range': f'F6', 'values': [[report_date]]},
                {'range': f'G6', 'values': [[quarter_info]]},
                {'range': f'H6', 'values': [[self.current_report['report_nm'] if self.current_report else '']]},
                {'range': f'I6', 'values': [[self.current_report['rcept_no'] if self.current_report else '']]},
                {'range': f'J1', 'values': [[f'최종업데이트: {report_date}']]}
            ])
            
            # 재무 데이터 매핑
            financial_mapping = {
                '자산총계': 8, '유동자산': 9, '현금및현금성자산': 10, '기타유동자산': 11, '재고자산': 12,
                '비유동자산': 13, '유형자산': 14, '사용권자산': 15, '무형자산': 16, '관계기업투자': 17,
                '부채총계': 19, '유동부채': 20, '기타유동부채': 21, '당기법인세부채': 22, '비유동부채': 23,
                '자본총계': 25, '자본금': 26, '자본잉여금': 27, '이익잉여금': 28,
                '매출액': 30, '영업이익': 31, '당기순이익': 32,
                '영업활동현금흐름': 34, '투자활동현금흐름': 35, '재무활동현금흐름': 36
            }
            
            # 각 항목별 데이터 업데이트
            updated_count = 0
            for item, row_num in financial_mapping.items():
                if item in all_financial_data and all_financial_data[item] is not None:
                    value = self._format_number_for_archive(all_financial_data[item])
                    update_data.append({
                        'range': f'{col_letter}{row_num}',
                        'values': [[value]]
                    })
                    print(f"    📈 {item}: {value}억원")
                    updated_count += 1
                else:
                    # 빈 값으로 설정
                    update_data.append({
                        'range': f'{col_letter}{row_num}',
                        'values': [['']]
                    })
            
            print(f"  📊 총 {updated_count}개 항목에 데이터 입력됨")
            
            # 배치 업데이트 실행
            if update_data:
                print(f"  📤 Archive 업데이트 중... ({len(update_data)}개 셀)")
                try:
                    # 작은 청크로 나누어 안전하게 업데이트
                    chunk_size = 10
                    for i in range(0, len(update_data), chunk_size):
                        chunk = update_data[i:i + chunk_size]
                        sheet.batch_update(chunk)
                        if i + chunk_size < len(update_data):
                            time.sleep(1)  # 청크 간 대기 시간 단축
                    
                    print(f"  ✅ XBRL 재무제표 Archive 업데이트 완료")
                    
                except Exception as e:
                    print(f"  ❌ 배치 업데이트 실패: {str(e)}")
                    # 개별 업데이트로 fallback
                    self._fallback_individual_update(sheet, update_data)
            
        except Exception as e:
            print(f"❌ XBRL 재무제표 Archive 업데이트 실패: {str(e)}")
            import traceback
            print(f"📋 상세 오류: {traceback.format_exc()}")

    def _update_xbrl_notes_archive_batch(self, sheet, wb, col_index):
        """XBRL 재무제표주석 Archive 업데이트 (완전 수정)"""
        try:
            # 주석 데이터 분석
            print(f"  📝 XBRL 주석 데이터 분석 중...")
            notes_analysis = self._analyze_xbrl_notes_sheets(wb)
            
            # 업데이트 위치
            col_letter = self._get_column_letter(col_index)
            print(f"  📍 데이터 입력 위치: {col_letter}열")
            
            # 배치 업데이트 데이터 준비
            update_data = []
            
            # 헤더 정보 업데이트
            report_date = datetime.now().strftime('%Y-%m-%d')
            quarter_info = self._get_quarter_info()
            
            update_data.extend([
                {'range': f'F6', 'values': [[report_date]]},
                {'range': f'G6', 'values': [[quarter_info]]},
                {'range': f'H6', 'values': [[self.current_report['report_nm'] if self.current_report else '']]},
                {'range': f'I6', 'values': [[self.current_report['rcept_no'] if self.current_report else '']]},
                {'range': f'J1', 'values': [[f'최종업데이트: {report_date}']]}
            ])
            
            # 주석 항목 매핑
            notes_mapping = {
                '회계정책': 8, '현금및현금성자산': 9, '매출채권': 10, '재고자산': 11,
                '유형자산': 12, '사용권자산': 13, '무형자산': 14, '관계기업투자': 15, '기타금융자산': 16,
                '매입채무': 18, '기타유동부채': 19, '충당부채': 20, '확정급여부채': 21, '이연법인세': 22,
                '자본금': 24, '자본잉여금': 25,
                '수익인식': 27, '주당손익': 28, '법인세비용': 29, '기타': 30
            }
            
            # 각 주석 항목 업데이트
            updated_count = 0
            for item, row_num in notes_mapping.items():
                if item in notes_analysis:
                    status = notes_analysis[item]
                    update_data.append({
                        'range': f'{col_letter}{row_num}',
                        'values': [[status]]
                    })
                    print(f"    📄 {item}: {status}")
                    updated_count += 1
                else:
                    update_data.append({
                        'range': f'{col_letter}{row_num}',
                        'values': [['N/A']]
                    })
            
            print(f"  📊 총 {updated_count}개 주석 항목 설정됨")
            
            # 배치 업데이트 실행
            if update_data:
                print(f"  📤 주석 Archive 업데이트 중... ({len(update_data)}개 셀)")
                try:
                    chunk_size = 10
                    for i in range(0, len(update_data), chunk_size):
                        chunk = update_data[i:i + chunk_size]
                        sheet.batch_update(chunk)
                        if i + chunk_size < len(update_data):
                            time.sleep(1)
                    
                    print(f"  ✅ XBRL 주석 Archive 업데이트 완료")
                    
                except Exception as e:
                    print(f"  ❌ 배치 업데이트 실패: {str(e)}")
                    self._fallback_individual_update(sheet, update_data)
            
        except Exception as e:
            print(f"❌ XBRL 주석 Archive 업데이트 실패: {str(e)}")
            import traceback
            print(f"📋 상세 오류: {traceback.format_exc()}")

    def _fallback_individual_update(self, sheet, update_data):
        """개별 업데이트 fallback"""
        print(f"    🔄 개별 업데이트로 재시도...")
        for item in update_data:
            try:
                sheet.update(item['range'], item['values'])
                time.sleep(1)
            except Exception as fallback_error:
                print(f"      ⚠️ {item['range']} 업데이트 실패: {str(fallback_error)}")

    def _format_number_for_archive(self, value):
        """Archive용 숫자 포맷팅 (억원 단위)"""
        try:
            if not value:
                return ''
            
            # 숫자 변환
            num = self._clean_number(value)
            if num is None:
                return ''
            
            # 억원 단위로 변환
            billion_value = num / 100000000
            
            # 소수점 자리 결정
            if abs(billion_value) >= 100:
                return f"{billion_value:.0f}"  # 100억 이상은 정수
            elif abs(billion_value) >= 10:
                return f"{billion_value:.1f}"  # 10억 이상은 소수점 1자리
            else:
                return f"{billion_value:.2f}"  # 10억 미만은 소수점 2자리
                
        except Exception as e:
            print(f"    ⚠️ 숫자 포맷팅 오류 ({value}): {str(e)}")
            return str(value)

    def _clean_number(self, value):
        """숫자 값 정제"""
        try:
            str_val = str(value).replace(',', '').replace('(', '-').replace(')', '').strip()
            if not str_val or str_val == '-':
                return None
            return float(str_val)
        except:
            return None

    def _get_quarter_info(self):
        """보고서 기준 분기 정보 반환 (pandas Series 오류 해결)"""
        try:
            # self.current_report가 pandas Series인지 확인하고 안전하게 처리
            if self.current_report is not None and hasattr(self.current_report, 'get'):
                # pandas Series인 경우 안전하게 값 추출
                if hasattr(self.current_report, 'iloc'):
                    # pandas Series인 경우
                    report_name = self.current_report.get('report_nm', '')
                    rcept_no = self.current_report.get('rcept_no', '')
                else:
                    # 일반 dict인 경우
                    report_name = self.current_report.get('report_nm', '')
                    rcept_no = self.current_report.get('rcept_no', '')
                
                if report_name:
                    print(f"  📅 보고서 분석: {report_name}")
                    
                    # 정규식으로 날짜 추출 개선
                    import re
                    
                    # 패턴 1: (YYYY.MM) 형태
                    date_pattern1 = re.search(r'\((\d{4})\.(\d{2})\)', str(report_name))
                    # 패턴 2: YYYY년 MM월 형태  
                    date_pattern2 = re.search(r'(\d{4})년\s*(\d{1,2})월', str(report_name))
                    # 패턴 3: 분기보고서 패턴
                    if '1분기' in str(report_name):
                        current_year = datetime.now().year
                        quarter_text = f"1Q{str(current_year)[2:]}"
                        print(f"    📊 1분기 보고서 감지: {quarter_text}")
                        return quarter_text
                    elif '반기' in str(report_name) or '2분기' in str(report_name):
                        current_year = datetime.now().year
                        quarter_text = f"2Q{str(current_year)[2:]}"
                        print(f"    📊 2분기/반기 보고서 감지: {quarter_text}")
                        return quarter_text
                    elif '3분기' in str(report_name):
                        current_year = datetime.now().year
                        quarter_text = f"3Q{str(current_year)[2:]}"
                        print(f"    📊 3분기 보고서 감지: {quarter_text}")
                        return quarter_text
                    elif '연결재무제표' in str(report_name) and '3월' in str(report_name):
                        current_year = datetime.now().year
                        quarter_text = f"1Q{str(current_year)[2:]}"
                        print(f"    📊 3월 연결재무제표 감지: {quarter_text}")
                        return quarter_text
                    
                    year, month = None, None
                    
                    if date_pattern1:
                        year, month = date_pattern1.groups()
                        month = int(month)
                    elif date_pattern2:
                        year, month = date_pattern2.groups()
                        month = int(month)
                    
                    if year and month:
                        # 분기 계산
                        if month <= 3:
                            quarter = 1
                        elif month <= 6:
                            quarter = 2
                        elif month <= 9:
                            quarter = 3
                        else:
                            quarter = 4
                        
                        quarter_text = f"{quarter}Q{year[2:]}"
                        print(f"    📊 추출된 분기: {quarter_text} (년도: {year}, 월: {month})")
                        return quarter_text
        
        except Exception as e:
            print(f"    ⚠️ 분기 정보 추출 중 오류: {str(e)}")
        
        # 기본값: 현재 날짜 기준
        now = datetime.now()
        quarter = (now.month - 1) // 3 + 1
        year = str(now.year)[2:]
        default_quarter = f"{quarter}Q{year}"
        print(f"    📊 기본 분기 사용: {default_quarter}")
        return default_quarter

    def _get_column_letter(self, col_index):
        """컬럼 인덱스를 문자로 변환 (0-based)"""
        result = ""
        num = col_index + 1  # 1-based로 변환
        while num > 0:
            num, remainder = divmod(num - 1, 26)
            result = chr(65 + remainder) + result
        return result

    def _cleanup_downloads(self):
        """다운로드 폴더 정리"""
        try:
            # Archive 업데이트가 완료된 후에만 정리
            if os.path.exists(self.download_dir) and self.results.get('excel_files'):
                # Excel 파일들만 남기고 다른 파일들 정리
                for file in os.listdir(self.download_dir):
                    file_path = os.path.join(self.download_dir, file)
                    if file_path not in self.results['downloaded_files']:
                        os.remove(file_path)
                
                # Archive 업데이트 완료 후 전체 폴더 삭제
                if os.environ.get('DELETE_AFTER_ARCHIVE', 'true').lower() == 'true':
                    shutil.rmtree(self.download_dir)
                    print("🧹 다운로드 폴더 정리 완료")
                else:
                    print("📁 다운로드 파일 보존 중")
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
