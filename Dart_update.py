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

        # Archive 시트 행 영역 매핑 설정
        self._setup_archive_row_mapping()

    def _setup_archive_row_mapping(self):
        """Archive 시트의 행 영역 매핑 설정"""
        # 재무제표 Archive 시트 행 매핑
        self.financial_row_mapping = {
            # 연결 재무제표
            'connected': {
                'D210000': {'start': 7, 'end': 80, 'name': '연결 재무상태표'},
                'D431410': {'start': 81, 'end': 140, 'name': '연결 손익계산서'},
                'D520000': {'start': 141, 'end': 200, 'name': '연결 현금흐름표'},
                'D610000': {'start': 201, 'end': 250, 'name': '연결 자본변동표'}
            },
            # 별도 재무제표  
            'separate': {
                'D210005': {'start': 257, 'end': 330, 'name': '별도 재무상태표'},
                'D431415': {'start': 331, 'end': 390, 'name': '별도 손익계산서'},
                'D520005': {'start': 391, 'end': 450, 'name': '별도 현금흐름표'},
                'D610005': {'start': 451, 'end': 500, 'name': '별도 자본변동표'}
            }
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
        """XBRL Archive 시트 업데이트 (연결/별도 구분, M열부터 시작)"""
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
                
                # 주석은 연결/별도로 분리
                self._update_single_xbrl_archive('Dart_Archive_XBRL_주석_연결', 
                                               self.results['excel_files']['notes'], 
                                               'notes_connected')
                
                self._update_single_xbrl_archive('Dart_Archive_XBRL_주석_별도', 
                                               self.results['excel_files']['notes'], 
                                               'notes_separate')
                
            print("✅ XBRL Archive 업데이트 완료")
            
        except Exception as e:
            print(f"❌ XBRL Archive 업데이트 실패: {str(e)}")

    def _update_single_xbrl_archive(self, sheet_name, file_path, file_type):
        """개별 XBRL Archive 시트 업데이트 (연결/별도 구분)"""
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
                # 주석 Archive는 더 많은 행이 필요함
                max_rows = 2000 if 'notes' in file_type else 1000
                archive_sheet = self.workbook.add_worksheet(sheet_name, max_rows, 20)
                time.sleep(2)
            
            # 시트가 새로 생성된 경우 헤더 설정
            if not archive_exists:
                header_type = file_type
                if file_type.startswith('notes_'):
                    header_type = 'notes'
                self._setup_xbrl_archive_header(archive_sheet, header_type)
                time.sleep(3)
            
            # 현재 마지막 데이터 열 찾기 (M열부터)
            last_col = self._find_last_data_column(archive_sheet)
            
            # Excel 파일 읽기
            wb = load_workbook(file_path, data_only=True)
            
            # 데이터 추출 및 업데이트
            if file_type == 'financial':
                self._update_xbrl_financial_archive_batch(archive_sheet, wb, last_col)
            elif file_type == 'notes_connected':
                self._update_xbrl_notes_archive_batch(archive_sheet, wb, last_col, 'connected')
            elif file_type == 'notes_separate':
                self._update_xbrl_notes_archive_batch(archive_sheet, wb, last_col, 'separate')
            else:
                # 기본 주석 처리 (하위 호환성)
                self._update_xbrl_notes_archive_batch(archive_sheet, wb, last_col, 'connected')
                
        except Exception as e:
            print(f"❌ {sheet_name} 업데이트 실패: {str(e)}")
            
            # 429 에러인 경우 더 긴 대기
            if "429" in str(e):
                print(f"  ⏳ API 할당량 초과. 60초 대기 중...")
                time.sleep(60)

    def _setup_xbrl_archive_header(self, sheet, file_type):
        """XBRL Archive 시트 헤더 설정 (M열부터 데이터 시작, 수정됨)"""
        try:
            # 현재 날짜
            current_date = datetime.now().strftime('%Y-%m-%d')
            
            # 1. 기본 헤더만 설정 (A1:L6)
            header_data = []
            
            # 1행: 제목 정보
            if file_type == 'financial':
                title_row = ['DART Archive XBRL 재무제표', '', '', '', '', '', '', '', '', f'최종업데이트: {current_date}', '', '계정과목']
            else:
                title_row = ['DART Archive XBRL 재무제표주석', '', '', '', '', '', '', '', '', f'최종업데이트: {current_date}', '', '계정과목']
            header_data.append(title_row)
            
            # 2행: 회사 정보
            company_row = [f'회사명: {self.company_name}', '', '', '', '', '', '', '', '', '', '', '항목명↓']
            header_data.append(company_row)
            
            # 3행: 종목 정보
            stock_row = [f'종목코드: {self.corp_code}', '', '', '', '', '', '', '', '', '', '', '']
            header_data.append(stock_row)
            
            # 4-6행: 빈 행들
            for _ in range(3):
                header_data.append(['', '', '', '', '', '', '', '', '', '', '', ''])
            
            # 한 번에 업데이트 (L열까지만)
            end_row = len(header_data)
            range_name = f'A1:L{end_row}'
            
            print(f"  📋 XBRL Archive 기본 헤더 설정: {range_name}")
            sheet.update(range_name, header_data)
            
            print(f"  ✅ XBRL Archive 기본 레이아웃 완료")
            print(f"      📁 파일타입: {'재무제표' if file_type == 'financial' else '재무제표주석'}")
            print(f"      📊 헤더영역: A1:L6 (기본정보)")
            print(f"      📋 계정명영역: L열 (계정과목명)")
            print(f"      📈 데이터영역: M열부터 시작 (분기별 데이터)")
            
        except Exception as e:
            print(f"  ❌ XBRL Archive 헤더 설정 실패: {str(e)}")

    def _find_last_data_column(self, sheet):
        """마지막 데이터 열 찾기 (M열부터 시작)"""
        try:
            # 2행에서 마지막 데이터가 있는 열 찾기 (헤더 행)
            row_2_values = sheet.row_values(2)
            
            # M열(13번째 열)부터 시작해서 마지막 데이터 열 찾기
            last_col = 11  # M열 = 12번째 열 (0-based index에서는 11)
            
            for i in range(11, len(row_2_values)):  # M열부터 검색
                if row_2_values[i]:  # 데이터가 있으면
                    last_col = i
            
            # 다음 열에 새 데이터 추가
            next_col = last_col + 1
            
            # 최소 M열(11)부터 시작
            if next_col < 11:
                next_col = 11
            
            col_letter = self._get_column_letter(next_col)
            print(f"📍 새 데이터 추가 위치: {col_letter}열 (인덱스: {next_col})")
            
            return next_col
            
        except Exception as e:
            print(f"⚠️ 마지막 열 찾기 실패: {str(e)}")
            return 11  # 기본값: M열

    def _update_xbrl_financial_archive_batch(self, sheet, wb, col_index):
        """XBRL 재무제표 Archive 업데이트 (대용량 배치 업데이트 최적화)"""
        try:
            print(f"  📊 XBRL 재무제표 데이터 추출 중...")
            
            # 업데이트할 컬럼 위치 (M열부터 시작)
            col_letter = self._get_column_letter(col_index)
            print(f"  📍 데이터 입력 위치: {col_letter}열")
            
            # 헤더 정보 업데이트
            report_date = datetime.now().strftime('%Y-%m-%d')
            quarter_info = self._get_quarter_info()
            
            # STEP 1: 모든 재무 데이터를 메모리에서 준비
            all_account_data, all_value_data = self._prepare_financial_data_for_batch_update(wb)
            
            # STEP 2: 대용량 배치 업데이트
            print(f"  🚀 대용량 배치 업데이트 시작...")
            
            # 배치 1: 헤더 정보 (분기정보와 날짜만)
            header_range = f'{col_letter}1:{col_letter}2'
            header_data = [[quarter_info], [report_date]]
            sheet.update(header_range, header_data)
            print(f"    ✅ 헤더 정보 업데이트 완료")
            
            # 배치 2: L열 계정명 대량 업데이트 (한 번에)
            if all_account_data:
                account_range = f'L7:L{6 + len(all_account_data)}'
                sheet.update(account_range, all_account_data)
                print(f"    ✅ L열 계정명 {len([row for row in all_account_data if row[0]])}개 업데이트 완료")
            
            time.sleep(2)  # API 제한 회피
            
            # 배치 3: M열 값 대량 업데이트 (한 번에)
            if all_value_data:
                value_range = f'{col_letter}7:{col_letter}{6 + len(all_value_data)}'
                sheet.update(value_range, all_value_data)
                print(f"    ✅ {col_letter}열 값 {len([row for row in all_value_data if row[0]])}개 업데이트 완료")
            
            print(f"  ✅ XBRL 재무제표 Archive 배치 업데이트 완료")
            
        except Exception as e:
            print(f"❌ XBRL 재무제표 Archive 업데이트 실패: {str(e)}")
            import traceback
            print(f"📋 상세 오류: {traceback.format_exc()}")

    def _prepare_financial_data_for_batch_update(self, wb):
        """재무 데이터를 배치 업데이트용으로 준비 (메모리에서 처리)"""
        try:
            print(f"  🔄 배치 업데이트용 데이터 준비 중...")
            
            # 연결 재무제표 데이터 추출 및 배치
            connected_data = self._extract_all_connected_financial_data(wb)
            
            # 별도 재무제표 데이터 추출 및 배치
            separate_data = self._extract_all_separate_financial_data(wb)
            
            # 전체 데이터를 하나의 배열로 통합
            all_account_data = []
            all_value_data = []
            
            # 연결 데이터 추가
            for sheet_code, sheet_info in connected_data.items():
                # 시트명 헤더 추가
                all_account_data.append([f"[연결] {sheet_info['name']}"])
                all_value_data.append([''])
                
                # 계정 데이터 추가
                for item in sheet_info['data']:
                    all_account_data.append([item['account']])
                    all_value_data.append([item['formatted_value']])
            
            # 구분선 추가
            all_account_data.append(['=== 구분선 ==='])
            all_value_data.append([''])
            
            # 별도 데이터 추가
            for sheet_code, sheet_info in separate_data.items():
                # 시트명 헤더 추가
                all_account_data.append([f"[별도] {sheet_info['name']}"])
                all_value_data.append([''])
                
                # 계정 데이터 추가
                for item in sheet_info['data']:
                    all_account_data.append([item['account']])
                    all_value_data.append([item['formatted_value']])
            
            # 통계 출력
            account_count = len([row for row in all_account_data if row[0]])
            value_count = len([row for row in all_value_data if row[0]])
            print(f"    📋 준비 완료: 계정명 {account_count}개, 값 {value_count}개")
            
            return all_account_data, all_value_data
            
        except Exception as e:
            print(f"  ❌ 배치 데이터 준비 실패: {str(e)}")
            return None, None

    def _extract_all_connected_financial_data(self, wb):
        """연결 재무제표 모든 데이터 추출"""
        connected_data = {}
        
        try:
            # 연결 시트들 처리
            for sheet_code, info in self.financial_row_mapping['connected'].items():
                if sheet_code in wb.sheetnames:
                    sheet_data = self._extract_financial_sheet_data(wb[sheet_code], info['name'])
                    connected_data[sheet_code] = {
                        'name': info['name'],
                        'data': sheet_data
                    }
                    print(f"    📄 [연결] {sheet_code}: {len(sheet_data)}개 계정")
            
        except Exception as e:
            print(f"    ⚠️ 연결 데이터 추출 실패: {str(e)}")
        
        return connected_data

    def _extract_all_separate_financial_data(self, wb):
        """별도 재무제표 모든 데이터 추출"""
        separate_data = {}
        
        try:
            # 별도 시트들 처리
            for sheet_code, info in self.financial_row_mapping['separate'].items():
                if sheet_code in wb.sheetnames:
                    sheet_data = self._extract_financial_sheet_data(wb[sheet_code], info['name'])
                    separate_data[sheet_code] = {
                        'name': info['name'],
                        'data': sheet_data
                    }
                    print(f"    📄 [별도] {sheet_code}: {len(sheet_data)}개 계정")
            
        except Exception as e:
            print(f"    ⚠️ 별도 데이터 추출 실패: {str(e)}")
        
        return separate_data

    def _extract_financial_sheet_data(self, worksheet, sheet_name):
        """개별 재무제표 시트에서 데이터 추출 (B열만 확인)"""
        data = []
        
        try:
            # 데이터 시작 행 동적으로 찾기
            data_start_row = self._find_data_start_row(worksheet)
            print(f"      📍 {sheet_name} 데이터 시작 행: {data_start_row}")
            
            # 데이터 행들 추출
            for row_idx, row in enumerate(worksheet.iter_rows(values_only=True, min_row=data_start_row, max_row=200), start=data_start_row):
                if not row or len(row) < 2:
                    continue
                    
                # A열: 계정명
                account_name = row[0]
                if not account_name:
                    continue
                    
                account_name = str(account_name).strip()
                
                # 유효한 계정명 필터링 (조건 완화)
                if (len(account_name) > 1 and 
                    not account_name.startswith(('[', '주석', 'Index', '(단위')) and
                    not account_name.endswith(('영역]', '항목', '코드')) and
                    '개요' not in account_name and
                    account_name not in ['', '-', '해당없음', 'None']):
                    
                    # B열 값만 추출 (가장 최신 데이터)
                    value = None
                    
                    if len(row) > 1 and row[1] is not None and row[1] != '' and row[1] != 'None':
                        cell_value = row[1]
                        # 숫자 확인
                        if isinstance(cell_value, (int, float)):
                            value = cell_value
                        # 문자열인 경우 숫자 변환 시도
                        elif isinstance(cell_value, str):
                            try:
                                clean_str = str(cell_value).replace(',', '').replace('(', '-').replace(')', '').strip()
                                if clean_str and clean_str != '-':
                                    value = float(clean_str)
                            except:
                                pass
                    
                    # 값이 없더라도 계정명은 저장 (나중에 값이 업데이트될 수 있음)
                    data.append({
                        'account': account_name,
                        'value': value,
                        'formatted_value': self._format_number_for_archive(value) if value else '',
                        'row': row_idx
                    })
            
            # 데이터 검증 로그
            if data:
                valid_count = len([d for d in data if d['value'] is not None])
                print(f"      ✅ 유효 데이터: {valid_count}/{len(data)}개")
        
        except Exception as e:
            print(f"      ⚠️ 시트 데이터 추출 실패: {str(e)}")
            import traceback
            traceback.print_exc()
        
        return data

    def _find_data_start_row(self, worksheet):
        """데이터 시작 행 동적으로 찾기 (B열 기준)"""
        try:
            # 첫 50행 내에서 B열에 숫자 데이터가 있는 첫 행 찾기
            for row_idx in range(1, min(51, worksheet.max_row + 1)):
                row = worksheet[row_idx]
                
                # B열(2번째 열)만 확인
                if len(row) >= 2:
                    cell = row[1]  # B열 (0-based index에서 1)
                    if cell and cell.value is not None:
                        if isinstance(cell.value, (int, float)):
                            # 이전 행부터 시작 (헤더 포함을 위해)
                            return max(1, row_idx - 1)
                        elif isinstance(cell.value, str):
                            try:
                                clean_str = str(cell.value).replace(',', '').replace('(', '-').replace(')', '').strip()
                                if clean_str and clean_str != '-':
                                    float(clean_str)
                                    # 이전 행부터 시작 (헤더 포함을 위해)
                                    return max(1, row_idx - 1)
                            except:
                                pass
            
            # 기본값
            return 6
            
        except Exception as e:
            print(f"        ⚠️ 시작 행 찾기 실패: {str(e)}")
            return 6

    def _update_xbrl_notes_archive_batch(self, sheet, wb, col_index, notes_type='connected'):
        """XBRL 재무제표주석 Archive 업데이트 (실제 주석 시트 내용 배치 업데이트, 수정됨)"""
        try:
            print(f"  📝 XBRL 주석 데이터 분석 중... ({notes_type})")
            
            # 업데이트할 컬럼 위치
            col_letter = self._get_column_letter(col_index)
            print(f"  📍 데이터 입력 위치: {col_letter}열")
            
            # 헤더 정보 업데이트
            report_date = datetime.now().strftime('%Y-%m-%d')
            quarter_info = self._get_quarter_info()
            
            # STEP 1: 모든 주석 데이터를 메모리에서 준비 (수정된 버전)
            all_notes_account_data, all_notes_value_data = self._prepare_notes_data_for_batch_update(wb, notes_type)
            
            # STEP 2: 배치 업데이트
            print(f"  🚀 주석 배치 업데이트 시작...")
            
            # 배치 1: 헤더 정보 (분기정보와 날짜만)
            header_range = f'{col_letter}1:{col_letter}2'
            header_data = [[quarter_info], [report_date]]
            sheet.update(header_range, header_data)
            print(f"    ✅ 헤더 정보 업데이트 완료")
            
            # 배치 2: L열 주석 항목명 대량 업데이트
            if all_notes_account_data:
                account_range = f'L7:L{6 + len(all_notes_account_data)}'
                sheet.update(account_range, all_notes_account_data)
                print(f"    ✅ L열 주석 항목 {len([row for row in all_notes_account_data if row[0]])}개 업데이트 완료")
            
            time.sleep(2)  # API 제한 회피
            
            # 배치 3: M열 주석 값 대량 업데이트
            if all_notes_value_data:
                value_range = f'{col_letter}7:{col_letter}{6 + len(all_notes_value_data)}'
                sheet.update(value_range, all_notes_value_data)
                print(f"    ✅ {col_letter}열 주석 값 {len([row for row in all_notes_value_data if row[0]])}개 업데이트 완료")
            
            print(f"  ✅ XBRL 주석 Archive 배치 업데이트 완료")
            
        except Exception as e:
            print(f"❌ XBRL 주석 Archive 업데이트 실패: {str(e)}")
            import traceback
            print(f"📋 상세 오류: {traceback.format_exc()}")

    def _prepare_notes_data_for_batch_update(self, wb, notes_type):
        """주석 데이터를 배치 업데이트용으로 준비 (텍스트 지원)"""
        try:
            print(f"  🔄 주석 배치 업데이트용 데이터 준비 중...")
            
            # D8xxxxx 주석 시트들 필터링 (연결/별도 구분)
            if notes_type == 'connected':
                target_sheets = [name for name in wb.sheetnames if name.startswith('D8') and name.endswith('0')]
            else:  # separate
                target_sheets = [name for name in wb.sheetnames if name.startswith('D8') and name.endswith('5')]
            
            print(f"    📄 {notes_type} 주석 시트 {len(target_sheets)}개 발견")
            
            # 전체 데이터를 하나의 배열로 통합
            all_notes_account_data = []
            all_notes_value_data = []
            
            # 각 주석 시트의 데이터 추출 및 배치
            for sheet_name in target_sheets:
                sheet_data = self._extract_notes_sheet_data(wb[sheet_name], sheet_name)
                if sheet_data:
                    # 시트 제목 추가
                    all_notes_account_data.append([sheet_data['title']])
                    all_notes_value_data.append([''])
                    
                    # 각 항목들 배치 (중분류/세분류 구분하여)
                    for item in sheet_data['items']:
                        # 중분류인 경우 구분 표시
                        if item.get('is_category', False):
                            display_name = f"● {item['original_name']}"
                        else:
                            # 세분류인 경우 들여쓰기
                            original_name = item.get('original_name', item['name'])
                            display_name = f"  └ {original_name}"
                        
                        all_notes_account_data.append([display_name])
                        all_notes_value_data.append([item['formatted_value']])
                    
                    # 구분을 위한 빈 행 추가
                    all_notes_account_data.append([''])
                    all_notes_value_data.append([''])
                    
                    # 통계 출력
                    text_items = len([item for item in sheet_data['items'] if item.get('value_type') == 'text'])
                    number_items = len([item for item in sheet_data['items'] if item.get('value_type') == 'number'])
                    print(f"      ✅ {sheet_name}: {len(sheet_data['items'])}개 항목 (숫자: {number_items}, 텍스트: {text_items})")
            
            # 통계 출력
            account_count = len([row for row in all_notes_account_data if row[0]])
            value_count = len([row for row in all_notes_value_data if row[0]])
            print(f"    📋 주석 준비 완료: 항목명 {account_count}개, 값 {value_count}개")
            
            return all_notes_account_data, all_notes_value_data
            
        except Exception as e:
            print(f"  ❌ 주석 배치 데이터 준비 실패: {str(e)}")
            return None, None

    def _extract_notes_sheet_data(self, worksheet, sheet_name):
        """개별 주석 시트에서 데이터 추출 (텍스트 데이터 포함)"""
        try:
            sheet_data = {
                'title': '',
                'items': []
            }
            
            # 시트 정보 수집
            max_col = worksheet.max_column
            max_row = min(worksheet.max_row, 500)  # 최대 500행까지만
            print(f"      📊 {sheet_name} - 열: {max_col}, 행: {max_row}")
            
            # 제목 추출 (보통 1-5행에 있음)
            for row in worksheet.iter_rows(min_row=1, max_row=5, min_col=1, max_col=1, values_only=True):
                if row[0] and isinstance(row[0], str):
                    title_candidate = str(row[0]).strip()
                    if len(title_candidate) > 5 and (sheet_name in title_candidate or '주석' in title_candidate):
                        sheet_data['title'] = title_candidate
                        break
            
            if not sheet_data['title']:
                sheet_data['title'] = f"[{sheet_name}] 주석"
            
            # 데이터 시작 행 찾기
            data_start_row = 3
            
            # 중분류 컨텍스트 추적
            current_category = ""
            
            # 데이터 추출
            for row_idx, row in enumerate(worksheet.iter_rows(min_row=data_start_row, max_row=max_row, values_only=True), start=data_start_row):
                if not row or not row[0]:
                    continue
                    
                # A열 항목명
                item_name = str(row[0]).strip()
                
                # 빈 값이거나 무의미한 항목 제외
                if (len(item_name) <= 1 or 
                    item_name.startswith(('[', 'Index', '(단위')) or
                    item_name.endswith(('영역]')) or
                    item_name in ['', '-', '해당없음', 'None']):
                    continue
                
                # 중분류 감지
                is_category = self._is_category_header(item_name, row_idx, worksheet)
                
                if is_category:
                    current_category = item_name
                    sheet_data['items'].append({
                        'name': f"[중분류] {current_category}",
                        'value': None,
                        'formatted_value': '',
                        'category': current_category,
                        'is_category': True,
                        'original_name': current_category
                    })
                    continue
                
                # B열 값 추출 (숫자 또는 텍스트)
                value = None
                value_type = None
                
                if len(row) > 1 and row[1] is not None and row[1] != '' and row[1] != 'None':
                    cell_value = row[1]
                    
                    # 숫자 값 확인
                    if isinstance(cell_value, (int, float)):
                        value = cell_value
                        value_type = 'number'
                    # 문자열인 경우
                    elif isinstance(cell_value, str):
                        str_value = str(cell_value).strip()
                        if str_value and str_value != '-':
                            # 먼저 숫자 변환 시도
                            try:
                                clean_num = str_value.replace(',', '').replace('(', '-').replace(')', '').strip()
                                if clean_num and clean_num != '-' and clean_num.replace('-', '').replace('.', '').isdigit():
                                    value = float(clean_num)
                                    value_type = 'number'
                                else:
                                    # 숫자가 아닌 텍스트로 처리
                                    value = str_value
                                    value_type = 'text'
                            except:
                                # 텍스트로 처리
                                value = str_value
                                value_type = 'text'
                
                # 항목 추가
                unique_name = f"{current_category}_{item_name}" if current_category else item_name
                
                sheet_data['items'].append({
                    'name': unique_name,
                    'original_name': item_name,
                    'value': value,
                    'formatted_value': self._format_notes_value(value, value_type) if value is not None else '',
                    'category': current_category,
                    'is_category': False,
                    'row_number': row_idx,
                    'value_type': value_type
                })
            
            # 결과 요약
            if sheet_data['items']:
                value_count = len([item for item in sheet_data['items'] if item.get('value') is not None])
                text_count = len([item for item in sheet_data['items'] if item.get('value_type') == 'text'])
                number_count = len([item for item in sheet_data['items'] if item.get('value_type') == 'number'])
                print(f"      📊 추출 완료: 총 {len(sheet_data['items'])}개 항목 (숫자: {number_count}개, 텍스트: {text_count}개)")
            
            return sheet_data if sheet_data['items'] else None
            
        except Exception as e:
            print(f"      ⚠️ 주석 시트 {sheet_name} 데이터 추출 실패: {str(e)}")
            import traceback
            traceback.print_exc()
            return None

    def _is_category_header(self, item_name, row_idx, worksheet):
        """항목이 중분류 헤더인지 판단"""
        try:
            # 방법 1: 패턴 기반 판단
            category_patterns = [
                '비용의 성격별',
                '비용의 성격',
                '성격별',
                '매출채권',
                '재고자산',
                '유형자산',
                '무형자산',
                '투자자산',
                '부채',
                '자본',
                '수익',
                '비용',
                '현금흐름',
                '분류',
                '구성내역',
                '내역',
                '내용',
                '현황'
            ]
            
            # 특정 키워드가 포함된 경우 중분류로 판단
            for pattern in category_patterns:
                if pattern in item_name:
                    return True
            
            # 방법 2: 셀 스타일 확인 (가능한 경우)
            try:
                cell = worksheet.cell(row=row_idx, column=1)
                if hasattr(cell, 'font') and cell.font and cell.font.bold:
                    return True
            except:
                pass
            
            # 방법 3: 들여쓰기 확인
            if not item_name.startswith((' ', '\t')):
                # 다음 행들이 들여쓰기되어 있는지 확인
                next_rows_indented = 0
                for next_row_idx in range(row_idx + 1, min(row_idx + 6, worksheet.max_row + 1)):
                    try:
                        next_cell = worksheet.cell(row=next_row_idx, column=1).value
                        if next_cell and isinstance(next_cell, str) and next_cell.startswith((' ', '\t')):
                            next_rows_indented += 1
                    except:
                        continue
                
                # 다음 행들이 들여쓰기되어 있으면 현재 행은 중분류
                if next_rows_indented >= 2:
                    return True
            
            return False
            
        except Exception as e:
            print(f"        ⚠️ 중분류 판단 실패: {str(e)}")
            return False

    def _format_notes_value(self, value, value_type=None):
        """주석 값 포맷팅 (숫자 및 텍스트 처리)"""
        try:
            if value is None:
                return ''
            
            # 텍스트인 경우
            if value_type == 'text' or isinstance(value, str):
                # 긴 텍스트는 적절히 잘라서 표시
                text_value = str(value).strip()
                if len(text_value) > 100:
                    return text_value[:97] + "..."
                else:
                    return text_value
            
            # 숫자인 경우 억원 단위로 변환
            elif isinstance(value, (int, float)):
                if abs(value) >= 100000000:  # 1억 이상
                    billion_value = value / 100000000
                    return f"{billion_value:.2f}억원"
                elif abs(value) >= 1000000:  # 100만 이상
                    million_value = value / 1000000
                    return f"{million_value:.1f}백만원"
                elif abs(value) >= 1000:  # 1000 이상
                    return f"{value:,.0f}"
                else:
                    return str(value)
            
            else:
                return str(value)
                
        except Exception as e:
            print(f"    ⚠️ 주석 값 포맷팅 오류 ({value}): {str(e)}")
            return str(value) if value else ''

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
            if isinstance(value, (int, float)):
                return float(value)
            
            str_val = str(value).replace(',', '').replace('(', '-').replace(')', '').strip()
            if not str_val or str_val == '-':
                return None
            return float(str_val)
        except:
            return None

    def _get_quarter_info(self):
        """보고서 기준 분기 정보 반환"""
        try:
            if self.current_report is not None and hasattr(self.current_report, 'get'):
                if hasattr(self.current_report, 'iloc'):
                    report_name = self.current_report.get('report_nm', '')
                else:
                    report_name = self.current_report.get('report_nm', '')
                
                if report_name:
                    print(f"  📅 보고서 분석: {report_name}")
                    
                    import re
                    
                    # 패턴 매칭으로 분기 정보 추출
                    if '1분기' in str(report_name):
                        current_year = datetime.now().year
                        quarter_text = f"1Q{str(current_year)[2:]}"
                        return quarter_text
                    elif '반기' in str(report_name) or '2분기' in str(report_name):
                        current_year = datetime.now().year
                        quarter_text = f"2Q{str(current_year)[2:]}"
                        return quarter_text
                    elif '3분기' in str(report_name):
                        current_year = datetime.now().year
                        quarter_text = f"3Q{str(current_year)[2:]}"
                        return quarter_text
                    
                    # 날짜 패턴 매칭
                    date_pattern1 = re.search(r'\((\d{4})\.(\d{2})\)', str(report_name))
                    date_pattern2 = re.search(r'(\d{4})년\s*(\d{1,2})월', str(report_name))
                    
                    year, month = None, None
                    
                    if date_pattern1:
                        year, month = date_pattern1.groups()
                        month = int(month)
                    elif date_pattern2:
                        year, month = date_pattern2.groups()
                        month = int(month)
                    
                    if year and month:
                        if month <= 3:
                            quarter = 1
                        elif month <= 6:
                            quarter = 2
                        elif month <= 9:
                            quarter = 3
                        else:
                            quarter = 4
                        
                        quarter_text = f"{quarter}Q{year[2:]}"
                        return quarter_text
        
        except Exception as e:
            print(f"    ⚠️ 분기 정보 추출 중 오류: {str(e)}")
        
        # 기본값: 현재 날짜 기준
        now = datetime.now()
        quarter = (now.month - 1) // 3 + 1
        year = str(now.year)[2:]
        default_quarter = f"{quarter}Q{year}"
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
