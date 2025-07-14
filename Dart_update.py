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
        
        # 단위 정보 출력
        number_unit = os.environ.get('NUMBER_UNIT', 'million')
        unit_text = {
            'million': '백만원',
            'hundred_million': '억원',
            'billion': '십억원'
        }.get(number_unit, '백만원')
        print(f"💰 숫자 표시 단위: {unit_text}")
        
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
                            
                            # 범위 지정하여 업데이트 (새로운 매개변수 순서: values, range_name)
                            range_name = f'A1:{end_col_letter}{end_row}'
                            worksheet.update(values=all_data, range_name=range_name)
                        
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
                
                # 디버깅: Excel 파일 구조 분석
                self._debug_excel_structure(self.results['excel_files']['financial'], 'financial')
                
                self._update_single_xbrl_archive('Dart_Archive_XBRL_재무제표', 
                                               self.results['excel_files']['financial'], 
                                               'financial')
                
            if 'notes' in self.results['excel_files']:
                print("📝 XBRL 재무제표주석 Archive 업데이트 중...")
                
                # 디버깅: Excel 파일 구조 분석
                self._debug_excel_structure(self.results['excel_files']['notes'], 'notes')
                
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

    def _debug_excel_structure(self, file_path, file_type):
        """Excel 파일 구조 디버깅"""
        try:
            print(f"\n  🔍 Excel 파일 구조 분석: {file_type}")
            wb = load_workbook(file_path, data_only=True)
            
            print(f"  📋 시트 목록: {wb.sheetnames}")
            print(f"  📊 총 시트 수: {len(wb.sheetnames)}")
            
            # 각 시트의 구조 분석
            for sheet_name in wb.sheetnames[:5]:  # 처음 5개 시트만
                if sheet_name.startswith('D'):
                    worksheet = wb[sheet_name]
                    print(f"\n  📄 시트 분석: {sheet_name}")
                    print(f"     크기: {worksheet.max_row}행 x {worksheet.max_column}열")
                    
                    # 처음 10행의 데이터 출력
                    print(f"     데이터 샘플 (처음 10행):")
                    for row_idx in range(1, min(11, worksheet.max_row + 1)):
                        row_data = []
                        for col_idx in range(1, min(6, worksheet.max_column + 1)):
                            cell = worksheet.cell(row=row_idx, column=col_idx)
                            if cell.value is not None:
                                value_str = str(cell.value)[:30]
                                if len(str(cell.value)) > 30:
                                    value_str += "..."
                                row_data.append(f"{self._get_column_letter(col_idx-1)}:{value_str}")
                        
                        if row_data:
                            print(f"       행{row_idx}: {' | '.join(row_data)}")
                    
                    # 데이터가 있는 열 분석
                    print(f"     열 분석:")
                    data_cols = []
                    for col_idx in range(1, min(11, worksheet.max_column + 1)):
                        has_data = False
                        numeric_count = 0
                        text_count = 0
                        
                        for row_idx in range(1, min(51, worksheet.max_row + 1)):
                            cell = worksheet.cell(row=row_idx, column=col_idx)
                            if cell.value is not None:
                                has_data = True
                                if isinstance(cell.value, (int, float)):
                                    numeric_count += 1
                                elif isinstance(cell.value, str) and cell.value.strip():
                                    text_count += 1
                        
                        if has_data:
                            col_letter = self._get_column_letter(col_idx-1)
                            data_cols.append(f"{col_letter}열(숫자:{numeric_count}, 텍스트:{text_count})")
                    
                    print(f"       데이터 있는 열: {', '.join(data_cols)}")
            
            wb.close()
            
        except Exception as e:
            print(f"  ❌ Excel 구조 분석 실패: {str(e)}")

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
        """XBRL Archive 시트 헤더 설정 (M열부터 데이터 시작, 단위 표시 추가)"""
        try:
            # 현재 날짜
            current_date = datetime.now().strftime('%Y-%m-%d')
            
            # 단위 정보 가져오기
            number_unit = os.environ.get('NUMBER_UNIT', 'million')
            unit_text = {
                'million': '백만원',
                'hundred_million': '억원',
                'billion': '십억원'
            }.get(number_unit, '백만원')
            
            # 1. 기본 헤더만 설정 (A1:L6)
            header_data = []
            
            # 1행: 제목 정보
            if file_type == 'financial':
                title_row = ['DART Archive XBRL 재무제표', '', '', '', '', '', '', '', '', f'최종업데이트: {current_date}', '', '계정과목']
            else:
                title_row = ['DART Archive XBRL 재무제표주석', '', '', '', '', '', '', '', '', f'최종업데이트: {current_date}', '', '계정과목']
            header_data.append(title_row)
            
            # 2행: 회사 정보
            company_row = [f'회사명: {self.company_name}', '', '', '', '', '', '', '', '', f'단위: {unit_text}', '', '항목명↓']
            header_data.append(company_row)
            
            # 3행: 종목 정보
            stock_row = [f'종목코드: {self.corp_code}', '', '', '', '', '', '', '', '', '', '', '']
            header_data.append(stock_row)
            
            # 4-6행: 빈 행들
            for _ in range(3):
                header_data.append(['', '', '', '', '', '', '', '', '', '', '', ''])
            
            # 한 번에 업데이트 (L열까지만) - 새로운 매개변수 순서
            end_row = len(header_data)
            range_name = f'A1:L{end_row}'
            
            print(f"  📋 XBRL Archive 기본 헤더 설정: {range_name}")
            sheet.update(values=header_data, range_name=range_name)
            
            print(f"  ✅ XBRL Archive 기본 레이아웃 완료")
            print(f"      📁 파일타입: {'재무제표' if file_type == 'financial' else '재무제표주석'}")
            print(f"      💰 단위: {unit_text}")
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
        """XBRL 재무제표 Archive 업데이트 (신규 표시 제거)"""
        try:
            print(f"  📊 XBRL 재무제표 데이터 추출 중...")
            
            # 업데이트할 컬럼 위치 (M열부터 시작)
            col_letter = self._get_column_letter(col_index)
            print(f"  📍 데이터 입력 위치: {col_letter}열")
            
            # STEP 1: 기존 L열의 계정명 읽어오기
            existing_accounts = set()
            try:
                # L열 데이터 읽기 (7행부터)
                l_column_values = sheet.col_values(12)  # L열은 12번째 열
                for idx, account in enumerate(l_column_values[6:], start=7):  # 7행부터
                    if account and account.strip():
                        existing_accounts.add(account.strip())
                
                print(f"  📋 기존 계정명 {len(existing_accounts)}개 발견")
            except Exception as e:
                print(f"  ⚠️ 기존 계정명 읽기 실패: {str(e)}")
            
            # 헤더 정보 업데이트
            report_date = datetime.now().strftime('%Y-%m-%d')
            quarter_info = self._get_quarter_info()
            
            # STEP 2: 모든 재무 데이터를 메모리에서 준비
            all_account_data, all_value_data = self._prepare_financial_data_for_batch_update(wb)
            
            # STEP 3: 신규 계정명 추적 (표시는 하지 않음)
            new_accounts = []
            for idx, account_row in enumerate(all_account_data):
                if account_row and account_row[0]:
                    account_name = account_row[0]
                    # 중분류나 구분선이 아닌 일반 계정명인 경우만 체크
                    if (not account_name.startswith('[') and 
                        not account_name.startswith('===') and
                        account_name not in existing_accounts):
                        new_accounts.append((idx, account_name))
            
            if new_accounts:
                print(f"  🆕 신규 계정명 {len(new_accounts)}개 발견:")
                for idx, name in new_accounts[:5]:  # 처음 5개만 출력
                    print(f"     - {name}")
                if len(new_accounts) > 5:
                    print(f"     ... 외 {len(new_accounts) - 5}개")
            
            # STEP 4: 대용량 배치 업데이트
            print(f"  🚀 대용량 배치 업데이트 시작...")
            
            # 배치 1: 헤더 정보 (분기정보와 날짜만) - 새로운 매개변수 순서
            header_range = f'{col_letter}1:{col_letter}2'
            header_data = [[quarter_info], [report_date]]
            sheet.update(values=header_data, range_name=header_range)
            print(f"    ✅ 헤더 정보 업데이트 완료")
            
            # 배치 2: L열 계정명 대량 업데이트 (한 번에) - 새로운 매개변수 순서
            if all_account_data:
                account_range = f'L7:L{6 + len(all_account_data)}'
                sheet.update(values=all_account_data, range_name=account_range)
                print(f"    ✅ L열 계정명 {len([row for row in all_account_data if row[0]])}개 업데이트 완료")
            
            time.sleep(2)  # API 제한 회피
            
            # 배치 3: M열 값 대량 업데이트 (한 번에) - 새로운 매개변수 순서
            if all_value_data:
                value_range = f'{col_letter}7:{col_letter}{6 + len(all_value_data)}'
                sheet.update(values=all_value_data, range_name=value_range)
                print(f"    ✅ {col_letter}열 값 {len([row for row in all_value_data if row[0]])}개 업데이트 완료")
            
            # STEP 5: 신규 계정명 목록을 별도로 저장 (옵션)
            if new_accounts and os.environ.get('SAVE_NEW_ACCOUNTS', 'false').lower() == 'true':
                self._save_new_accounts_log(new_accounts, quarter_info)
            
            print(f"  ✅ XBRL 재무제표 Archive 배치 업데이트 완료")
            
        except Exception as e:
            print(f"❌ XBRL 재무제표 Archive 업데이트 실패: {str(e)}")
            import traceback
            print(f"📋 상세 오류: {traceback.format_exc()}")
    
    def _save_new_accounts_log(self, new_accounts, quarter_info):
        """신규 계정명을 별도 파일로 저장"""
        try:
            log_filename = f"new_accounts_{self.company_name}_{quarter_info}_{datetime.now().strftime('%Y%m%d')}.txt"
            with open(log_filename, 'w', encoding='utf-8') as f:
                f.write(f"신규 계정명 목록 - {self.company_name} ({quarter_info})\n")
                f.write(f"생성일시: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write("=" * 50 + "\n\n")
                for idx, name in new_accounts:
                    f.write(f"{name}\n")
            print(f"  📝 신규 계정명 로그 저장: {log_filename}")
        except Exception as e:
            print(f"  ⚠️ 신규 계정명 로그 저장 실패: {str(e)}")

    def _prepare_financial_data_for_batch_update(self, wb):
        """재무 데이터를 배치 업데이트용으로 준비 (모든 데이터 포함)"""
        try:
            print(f"  🔄 배치 업데이트용 데이터 준비 중...")
            
            all_account_data = []
            all_value_data = []
            
            # 모든 D로 시작하는 시트 처리
            d_sheets = [name for name in wb.sheetnames if name.startswith('D')]
            print(f"  📋 D로 시작하는 시트 {len(d_sheets)}개 발견")
            
            for sheet_name in sorted(d_sheets):
                worksheet = wb[sheet_name]
                
                # 시트 제목 찾기
                sheet_title = self._find_sheet_title(worksheet) or sheet_name
                
                # 연결/별도 구분
                sheet_type = ""
                if '연결' in sheet_title or sheet_name.endswith('0'):
                    sheet_type = "[연결]"
                elif '별도' in sheet_title or sheet_name.endswith('5'):
                    sheet_type = "[별도]"
                else:
                    sheet_type = "[기타]"
                
                # 재무제표 종류 판단
                fs_type = ""
                if '재무상태표' in sheet_title:
                    fs_type = "재무상태표"
                elif '손익계산서' in sheet_title or '포괄손익' in sheet_title:
                    fs_type = "손익계산서"
                elif '현금흐름표' in sheet_title:
                    fs_type = "현금흐름표"
                elif '자본변동표' in sheet_title:
                    fs_type = "자본변동표"
                else:
                    # 시트 내용에서 판단
                    for row in worksheet.iter_rows(min_row=1, max_row=10, values_only=True):
                        if row and row[0]:
                            cell_text = str(row[0])
                            if '자산' in cell_text or '부채' in cell_text:
                                fs_type = "재무상태표"
                                break
                            elif '매출' in cell_text or '영업이익' in cell_text:
                                fs_type = "손익계산서"
                                break
                            elif '현금' in cell_text and '흐름' in cell_text:
                                fs_type = "현금흐름표"
                                break
                            elif '자본' in cell_text and '변동' in cell_text:
                                fs_type = "자본변동표"
                                break
                
                if not fs_type:
                    continue  # 재무제표가 아닌 시트는 건너뛰기
                
                # 시트명 헤더 추가
                header_text = f"{sheet_type} {fs_type} ({sheet_name})"
                all_account_data.append([header_text])
                all_value_data.append([''])
                
                # 모든 데이터 추출 (필터링 최소화)
                data_count = 0
                for row_idx in range(1, min(worksheet.max_row + 1, 500)):
                    row = list(worksheet.iter_rows(min_row=row_idx, max_row=row_idx, values_only=True))[0]
                    
                    if not row or len(row) < 2:
                        continue
                    
                    # A열: 계정명 (어떤 텍스트든 허용)
                    account_name = str(row[0]).strip() if row[0] else ''
                    
                    # 최소한의 필터링만 적용
                    if (not account_name or 
                        len(account_name) < 2 or 
                        account_name.startswith('[') or
                        account_name.startswith('(단위')):
                        continue
                    
                    # B열: 값
                    value = None
                    if row[1] is not None:
                        if isinstance(row[1], (int, float)):
                            value = row[1]
                        elif isinstance(row[1], str):
                            try:
                                clean_str = str(row[1]).replace(',', '').replace('(', '-').replace(')', '').strip()
                                if clean_str and clean_str != '-':
                                    value = float(clean_str)
                            except:
                                pass
                    
                    # 모든 계정명 추가 (값이 없어도)
                    all_account_data.append([account_name])
                    all_value_data.append([self._format_number_for_archive(value) if value else ''])
                    data_count += 1
                
                if data_count > 0:
                    print(f"    ✅ {sheet_name}: {data_count}개 항목 추가")
                    # 구분을 위한 빈 행
                    all_account_data.append([''])
                    all_value_data.append([''])
            
            # 통계 출력
            total_items = len([row for row in all_account_data if row[0] and not row[0].startswith('[')])
            print(f"  📊 총 추출 항목: {total_items}개")
            
            return all_account_data, all_value_data
            
        except Exception as e:
            print(f"  ❌ 배치 데이터 준비 실패: {str(e)}")
            import traceback
            traceback.print_exc()
            return [], []

    def _find_sheet_title(self, worksheet):
        """시트 제목 찾기"""
        try:
            # 처음 10행에서 제목 찾기
            for row in worksheet.iter_rows(min_row=1, max_row=10, values_only=True):
                for cell in row:
                    if cell and isinstance(cell, str):
                        if any(keyword in str(cell) for keyword in ['재무상태표', '손익계산서', '현금흐름표', '자본변동표', '포괄손익']):
                            return str(cell).strip()
            return None
        except:
            return None

    def _extract_all_connected_financial_data(self, wb):
        """연결 재무제표 모든 데이터 추출 (개선된 버전)"""
        connected_data = {}
        
        try:
            print(f"\n    🔍 연결 재무제표 시트 스캔 중...")
            
            # 실제 시트명으로 직접 찾기 (시트 코드에 의존하지 않음)
            financial_sheet_patterns = [
                ('재무상태표', '연결 재무상태표'),
                ('손익계산서', '연결 손익계산서'),
                ('포괄손익계산서', '연결 포괄손익계산서'),
                ('현금흐름표', '연결 현금흐름표'),
                ('자본변동표', '연결 자본변동표')
            ]
            
            # 모든 시트 검사
            for sheet_name in wb.sheetnames:
                # D로 시작하는 시트만 처리
                if not sheet_name.startswith('D'):
                    continue
                    
                # 시트 내용 확인하여 재무제표 종류 파악
                worksheet = wb[sheet_name]
                sheet_title = self._get_sheet_title(worksheet)
                
                # 연결 재무제표인지 확인
                if '연결' in sheet_title or sheet_name.endswith('0'):
                    # 어떤 재무제표인지 파악
                    for pattern, display_name in financial_sheet_patterns:
                        if pattern in sheet_title or pattern in sheet_name:
                            print(f"      📄 발견: {sheet_name} → {display_name}")
                            
                            # 데이터 추출
                            sheet_data = self._extract_financial_sheet_data_v2(worksheet, display_name)
                            if sheet_data:
                                connected_data[sheet_name] = {
                                    'name': display_name,
                                    'data': sheet_data
                                }
                                print(f"        ✅ {len(sheet_data)}개 계정 추출 완료")
                            break
            
        except Exception as e:
            print(f"    ⚠️ 연결 데이터 추출 실패: {str(e)}")
            import traceback
            traceback.print_exc()
        
        return connected_data

    def _extract_all_separate_financial_data(self, wb):
        """별도 재무제표 모든 데이터 추출 (개선된 버전)"""
        separate_data = {}
        
        try:
            print(f"\n    🔍 별도 재무제표 시트 스캔 중...")
            
            # 실제 시트명으로 직접 찾기
            financial_sheet_patterns = [
                ('재무상태표', '별도 재무상태표'),
                ('손익계산서', '별도 손익계산서'),
                ('포괄손익계산서', '별도 포괄손익계산서'),
                ('현금흐름표', '별도 현금흐름표'),
                ('자본변동표', '별도 자본변동표')
            ]
            
            # 모든 시트 검사
            for sheet_name in wb.sheetnames:
                # D로 시작하는 시트만 처리
                if not sheet_name.startswith('D'):
                    continue
                    
                # 시트 내용 확인하여 재무제표 종류 파악
                worksheet = wb[sheet_name]
                sheet_title = self._get_sheet_title(worksheet)
                
                # 별도 재무제표인지 확인 (연결이 아니고 5로 끝나는 경우)
                if ('별도' in sheet_title or sheet_name.endswith('5')) and '연결' not in sheet_title:
                    # 어떤 재무제표인지 파악
                    for pattern, display_name in financial_sheet_patterns:
                        if pattern in sheet_title or pattern in sheet_name:
                            print(f"      📄 발견: {sheet_name} → {display_name}")
                            
                            # 데이터 추출
                            sheet_data = self._extract_financial_sheet_data_v2(worksheet, display_name)
                            if sheet_data:
                                separate_data[sheet_name] = {
                                    'name': display_name,
                                    'data': sheet_data
                                }
                                print(f"        ✅ {len(sheet_data)}개 계정 추출 완료")
                            break
            
        except Exception as e:
            print(f"    ⚠️ 별도 데이터 추출 실패: {str(e)}")
            import traceback
            traceback.print_exc()
        
        return separate_data

    def _get_sheet_title(self, worksheet):
        """시트의 제목 찾기 (처음 10행에서)"""
        try:
            for row_idx in range(1, min(11, worksheet.max_row + 1)):
                for col_idx in range(1, min(4, worksheet.max_column + 1)):
                    cell = worksheet.cell(row=row_idx, column=col_idx)
                    if cell.value and isinstance(cell.value, str):
                        value = str(cell.value).strip()
                        if len(value) > 5 and ('재무상태표' in value or '손익계산서' in value or 
                                               '현금흐름표' in value or '자본변동표' in value or
                                               '포괄손익' in value):
                            return value
            return ""
        except:
            return ""

    def _extract_financial_sheet_data_v2(self, worksheet, sheet_name):
        """개별 재무제표 시트에서 데이터 추출 (완전히 새로운 접근)"""
        data = []
        
        try:
            print(f"\n      🔍 {sheet_name} 데이터 추출 중...")
            
            # 시트 전체를 스캔하여 패턴 파악
            account_col = None  # 계정명이 있는 열
            value_col = None    # 값이 있는 열
            data_rows = []      # 실제 데이터가 있는 행들
            
            # 1. 계정명과 값이 있는 열 찾기 (처음 100행 검사)
            for row_idx in range(1, min(101, worksheet.max_row + 1)):
                row_values = []
                for col_idx in range(1, min(11, worksheet.max_column + 1)):
                    cell = worksheet.cell(row=row_idx, column=col_idx)
                    row_values.append(cell.value)
                
                # 계정명 열과 값 열 찾기
                for i, val in enumerate(row_values[:-1]):  # 마지막 열 제외
                    if val and isinstance(val, str) and len(str(val).strip()) > 2:
                        # 다음 열에 숫자가 있는지 확인
                        next_val = row_values[i + 1] if i + 1 < len(row_values) else None
                        if next_val and (isinstance(next_val, (int, float)) or 
                                       (isinstance(next_val, str) and self._is_numeric_string(next_val))):
                            if account_col is None:
                                account_col = i
                                value_col = i + 1
                            data_rows.append(row_idx)
                            break
            
            if account_col is None or value_col is None:
                print(f"      ⚠️ 데이터 구조를 파악할 수 없습니다.")
                return data
                
            print(f"      📍 계정명 열: {self._get_column_letter(account_col)}열, 값 열: {self._get_column_letter(value_col)}열")
            print(f"      📍 데이터 행 수: {len(set(data_rows))}개")
            
            # 2. 실제 데이터 추출
            processed_accounts = set()  # 중복 제거용
            
            for row_idx in sorted(set(data_rows)):
                # 계정명 가져오기
                account_cell = worksheet.cell(row=row_idx, column=account_col + 1)
                account_name = str(account_cell.value).strip() if account_cell.value else ''
                
                # 값 가져오기
                value_cell = worksheet.cell(row=row_idx, column=value_col + 1)
                raw_value = value_cell.value
                
                # 유효성 검사
                if not account_name or len(account_name) < 2:
                    continue
                    
                # 명백한 헤더나 메타데이터 제외
                skip_patterns = ['[', '(단위', '단위:', '주석', 'Index', 'Sheet', '합계', '총계']
                if any(pattern in account_name for pattern in skip_patterns):
                    continue
                
                # 중복 제거 (같은 계정명이 여러 번 나올 수 있음)
                account_key = f"{account_name}_{row_idx}"
                if account_key in processed_accounts:
                    continue
                processed_accounts.add(account_key)
                
                # 값 처리
                value = None
                if raw_value is not None:
                    if isinstance(raw_value, (int, float)):
                        value = raw_value
                    elif isinstance(raw_value, str):
                        value = self._parse_numeric_string(raw_value)
                
                # 데이터 추가
                data.append({
                    'account': account_name,
                    'value': value,
                    'formatted_value': self._format_number_for_archive(value) if value else '',
                    'row': row_idx
                })
            
            # 결과 요약
            valid_count = len([d for d in data if d['value'] is not None])
            print(f"      ✅ 추출 완료: 총 {len(data)}개 계정 (값 있음: {valid_count}개)")
            
        except Exception as e:
            print(f"      ❌ 데이터 추출 실패: {str(e)}")
            import traceback
            traceback.print_exc()
        
        return data

    def _is_numeric_string(self, value):
        """문자열이 숫자인지 확인"""
        if not isinstance(value, str):
            return False
        try:
            clean_str = str(value).replace(',', '').replace('(', '-').replace(')', '').strip()
            if clean_str and clean_str != '-':
                float(clean_str)
                return True
        except:
            pass
        return False

    def _parse_numeric_string(self, value):
        """문자열을 숫자로 변환"""
        try:
            clean_str = str(value).replace(',', '').replace('(', '-').replace(')', '').strip()
            if clean_str and clean_str != '-' and clean_str != '0':
                return float(clean_str)
        except:
            pass
        return None

    def _extract_financial_sheet_data(self, worksheet, sheet_name):
        """개별 재무제표 시트에서 데이터 추출 (디버깅 강화)"""
        data = []
        
        try:
            print(f"\n      🔍 {sheet_name} 시트 분석 시작")
            print(f"      📊 시트 크기: {worksheet.max_row}행 x {worksheet.max_column}열")
            
            # 처음 10행의 데이터 구조 확인 (디버깅용)
            print(f"      📋 처음 10행 데이터 구조:")
            for row_idx in range(1, min(11, worksheet.max_row + 1)):
                row_data = []
                for col_idx in range(1, min(6, worksheet.max_column + 1)):
                    cell = worksheet.cell(row=row_idx, column=col_idx)
                    if cell.value is not None:
                        row_data.append(f"{self._get_column_letter(col_idx-1)}:{repr(cell.value)[:30]}")
                if row_data:
                    print(f"        행{row_idx}: {', '.join(row_data)}")
            
            # 모든 행을 검사하여 실제 데이터 구조 파악
            data_start_row = None
            data_end_row = None
            
            # 전체 시트 스캔하여 데이터 영역 찾기
            for row_idx in range(1, min(worksheet.max_row + 1, 500)):
                row = list(worksheet.iter_rows(min_row=row_idx, max_row=row_idx, values_only=True))[0]
                
                # A열과 B열에 모두 데이터가 있는 행 찾기
                if row and len(row) >= 2 and row[0] and row[1]:
                    a_val = str(row[0]).strip() if row[0] else ''
                    b_val = row[1]
                    
                    # A열이 계정명이고 B열이 숫자인 경우
                    if (len(a_val) > 1 and 
                        not a_val.startswith(('[', 'Index', '(단위', '단위:')) and
                        not a_val.endswith(('영역]', '코드'))):
                        
                        # B열이 숫자인지 확인
                        is_numeric = False
                        if isinstance(b_val, (int, float)):
                            is_numeric = True
                        elif isinstance(b_val, str):
                            try:
                                clean_str = str(b_val).replace(',', '').replace('(', '-').replace(')', '').strip()
                                if clean_str and clean_str != '-':
                                    float(clean_str)
                                    is_numeric = True
                            except:
                                pass
                        
                        if is_numeric:
                            if data_start_row is None:
                                data_start_row = row_idx
                            data_end_row = row_idx
            
            print(f"      📍 데이터 영역: {data_start_row}행 ~ {data_end_row}행")
            
            if data_start_row is None:
                print(f"      ⚠️ 데이터를 찾을 수 없습니다!")
                return data
            
            # 실제 데이터 추출
            extracted_count = 0
            skipped_count = 0
            
            for row_idx in range(data_start_row, min(data_end_row + 1, 500)):
                row = list(worksheet.iter_rows(min_row=row_idx, max_row=row_idx, values_only=True))[0]
                
                if not row or len(row) < 2:
                    continue
                
                # A열: 계정명
                account_name = row[0]
                if not account_name:
                    continue
                
                account_name = str(account_name).strip()
                
                # 필터링 조건 (매우 완화)
                if len(account_name) < 2:
                    skipped_count += 1
                    continue
                    
                # 명백한 헤더나 메타데이터만 제외
                skip_keywords = ['[', 'Index', '(단위', '단위:', '영역]', '코드', '주석', '개요']
                if any(keyword in account_name for keyword in skip_keywords):
                    skipped_count += 1
                    print(f"        ⏭️ 건너뜀: {account_name}")
                    continue
                
                # B열 값 추출
                value = None
                raw_value = row[1] if len(row) > 1 else None
                
                if raw_value is not None and raw_value != '' and str(raw_value) != 'None':
                    # 숫자 확인
                    if isinstance(raw_value, (int, float)):
                        value = raw_value
                    # 문자열인 경우 숫자 변환 시도
                    elif isinstance(raw_value, str):
                        try:
                            clean_str = str(raw_value).replace(',', '').replace('(', '-').replace(')', '').strip()
                            if clean_str and clean_str != '-' and clean_str != '0':
                                value = float(clean_str)
                        except:
                            pass
                
                # 데이터 추가 (값이 없어도 계정명은 저장)
                data.append({
                    'account': account_name,
                    'value': value,
                    'formatted_value': self._format_number_for_archive(value) if value else '',
                    'row': row_idx
                })
                extracted_count += 1
                
                # 처음 몇 개 데이터 출력 (디버깅용)
                if extracted_count <= 5:
                    print(f"        ✅ 추출: {account_name} = {value}")
            
            # 결과 요약
            valid_count = len([d for d in data if d['value'] is not None])
            print(f"      📊 추출 결과: 총 {extracted_count}개 계정 (값 있음: {valid_count}개, 건너뜀: {skipped_count}개)")
            
        except Exception as e:
            print(f"      ❌ 시트 데이터 추출 실패: {str(e)}")
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
        """XBRL 재무제표주석 Archive 업데이트 (신규 표시 제거)"""
        try:
            print(f"  📝 XBRL 주석 데이터 분석 중... ({notes_type})")
            
            # 업데이트할 컬럼 위치
            col_letter = self._get_column_letter(col_index)
            print(f"  📍 데이터 입력 위치: {col_letter}열")
            
            # STEP 1: 기존 L열의 항목명 읽어오기
            existing_items = set()
            try:
                # L열 데이터 읽기 (7행부터)
                l_column_values = sheet.col_values(12)  # L열은 12번째 열
                for idx, item in enumerate(l_column_values[6:], start=7):  # 7행부터
                    if item and item.strip():
                        # display_name이 아닌 원본 항목명만 저장
                        clean_item = item.strip()
                        # 들여쓰기 표시 제거
                        if '└' in clean_item:
                            clean_item = clean_item.split('└')[-1].strip()
                        # 중분류/하위분류 표시 제거
                        if clean_item.startswith('[') and ']' in clean_item:
                            clean_item = clean_item.split(']')[1].strip()
                        existing_items.add(clean_item)
                
                print(f"  📋 기존 항목명 {len(existing_items)}개 발견")
            except Exception as e:
                print(f"  ⚠️ 기존 항목명 읽기 실패: {str(e)}")
            
            # 헤더 정보 업데이트
            report_date = datetime.now().strftime('%Y-%m-%d')
            quarter_info = self._get_quarter_info()
            
            # STEP 2: 모든 주석 데이터를 메모리에서 준비
            all_notes_account_data, all_notes_value_data = self._prepare_notes_data_for_batch_update(wb, notes_type)
            
            # STEP 3: 신규 항목명 추적 (표시는 하지 않음)
            new_items = []
            for idx, item_row in enumerate(all_notes_account_data):
                if item_row and item_row[0]:
                    item_name = item_row[0]
                    # 실제 항목명 추출 (display format에서)
                    original_name = item_name
                    if '└' in item_name:
                        original_name = item_name.split('└')[-1].strip()
                    elif item_name.startswith('[') and ']' in item_name:
                        continue  # 중분류/하위분류는 체크하지 않음
                    
                    # 신규 항목인지 체크
                    if original_name and original_name not in existing_items:
                        new_items.append((idx, original_name))
            
            if new_items:
                print(f"  🆕 신규 항목명 {len(new_items)}개 발견:")
                for idx, name in new_items[:5]:  # 처음 5개만 출력
                    print(f"     - {name}")
                if len(new_items) > 5:
                    print(f"     ... 외 {len(new_items) - 5}개")
            
            # STEP 4: 배치 업데이트
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
        """주석 데이터를 배치 업데이트용으로 준비 (들여쓰기 구조 보존)"""
        try:
            print(f"  🔄 주석 배치 업데이트용 데이터 준비 중... ({notes_type})")
            
            # D8로 시작하는 주석 시트 필터링
            if notes_type == 'connected':
                # 연결: D8로 시작하고 0으로 끝나거나 연결이 포함된 시트
                target_sheets = [name for name in wb.sheetnames 
                               if name.startswith('D8') and (name.endswith('0') or '연결' in name)]
            else:  # separate
                # 별도: D8로 시작하고 5로 끝나거나 별도가 포함된 시트
                target_sheets = [name for name in wb.sheetnames 
                               if name.startswith('D8') and (name.endswith('5') or '별도' in name)]
            
            print(f"    📄 {notes_type} 주석 시트 {len(target_sheets)}개 발견")
            
            # 전체 데이터를 하나의 배열로 통합
            all_notes_account_data = []
            all_notes_value_data = []
            
            # 각 주석 시트의 데이터 추출 및 배치
            for sheet_name in sorted(target_sheets):
                sheet_data = self._extract_notes_sheet_data(wb[sheet_name], sheet_name)
                if sheet_data:
                    # 시트 제목 추가
                    all_notes_account_data.append([f"===== {sheet_data['title']} ====="])
                    all_notes_value_data.append([''])
                    
                    # 각 항목들 배치
                    for item in sheet_data['items']:
                        # 표시할 이름 결정
                        if item.get('is_category'):
                            # 중분류/하위분류
                            display_name = item['name']  # 이미 [중분류] 또는 [하위분류] 포맷
                        elif 'display_name' in item:
                            # 들여쓰기가 적용된 표시 이름 사용
                            display_name = item['display_name']
                        else:
                            # 일반 항목
                            original_name = item.get('original_name', item['name'])
                            indent_level = item.get('indent_level', 0)
                            
                            # 들여쓰기 적용
                            if indent_level > 0:
                                display_name = "  " * indent_level + "└ " + original_name
                            else:
                                display_name = original_name
                        
                        all_notes_account_data.append([display_name])
                        all_notes_value_data.append([item['formatted_value']])
                    
                    # 구분을 위한 빈 행 추가
                    all_notes_account_data.append([''])
                    all_notes_value_data.append([''])
                    
                    # 통계 출력
                    categories = len([item for item in sheet_data['items'] if item.get('is_category')])
                    values = len([item for item in sheet_data['items'] if item.get('value') is not None])
                    text_items = len([item for item in sheet_data['items'] if item.get('value_type') == 'text'])
                    number_items = len([item for item in sheet_data['items'] if item.get('value_type') == 'number'])
                    
                    print(f"      ✅ {sheet_name}: {len(sheet_data['items'])}개 항목")
                    print(f"         - 분류: {categories}개, 값: {values}개 (숫자: {number_items}, 텍스트: {text_items})")
            
            # 통계 출력
            total_items = len([row for row in all_notes_account_data if row[0] and not row[0].startswith('=')])
            print(f"    📊 총 주석 항목: {total_items}개")
            
            return all_notes_account_data, all_notes_value_data
            
        except Exception as e:
            print(f"  ❌ 주석 배치 데이터 준비 실패: {str(e)}")
            import traceback
            traceback.print_exc()
            return [], []

    def _extract_cell_value(self, cell_value):
        """셀 값에서 실제 값과 타입 추출"""
        if cell_value is None:
            return None, None
            
        # 숫자인 경우
        if isinstance(cell_value, (int, float)):
            return cell_value, 'number'
        
        # 문자열인 경우
        elif isinstance(cell_value, str):
            str_val = str(cell_value).strip()
            if not str_val or str_val == '-':
                return None, None
                
            # 숫자 변환 시도
            try:
                clean_num = str_val.replace(',', '').replace('(', '-').replace(')', '').strip()
                if clean_num and clean_num != '-' and clean_num.replace('-', '').replace('.', '').isdigit():
                    return float(clean_num), 'number'
            except:
                pass
            
            # 텍스트로 처리
            if len(str_val) >= 2:
                return str_val, 'text'
        
        return None, None

    def _extract_notes_sheet_data(self, worksheet, sheet_name):
        """개별 주석 시트에서 데이터 추출 (들여쓰기 및 대괄호 처리 개선)"""
        try:
            sheet_data = {
                'title': sheet_name,
                'items': []
            }
            
            print(f"\n      🔍 {sheet_name} 주석 시트 분석 중...")
            
            # 전체 시트 스캔
            max_row = min(worksheet.max_row, 1000)
            max_col = min(worksheet.max_column, 20)
            
            # 모든 셀 데이터를 메모리에 로드
            all_data = []
            for row in worksheet.iter_rows(min_row=1, max_row=max_row, min_col=1, max_col=max_col, values_only=True):
                all_data.append(list(row))
            
            print(f"      📊 시트 크기: {len(all_data)}행 x {max_col}열")
            
            # 현재 중분류
            current_category = ""
            current_subcategory = ""
            
            for row_idx, row in enumerate(all_data):
                if not row or not any(row):  # 빈 행 건너뛰기
                    continue
                
                # 첫 번째 비어있지 않은 셀의 위치와 내용 찾기
                first_text = None
                first_col = -1
                text_positions = []  # (열번호, 텍스트) 쌍 저장
                
                for col_idx, cell in enumerate(row):
                    if cell and str(cell).strip():
                        text = str(cell).strip()
                        text_positions.append((col_idx, text))
                        if first_text is None:
                            first_text = text
                            first_col = col_idx
                
                if not first_text or len(first_text) < 2:
                    continue
                
                # 제외할 패턴 (단위 표시 등)
                if any(skip in first_text for skip in ['(단위', '단위:', 'Index', 'Sheet']):
                    continue
                
                # 대괄호로 둘러싸인 텍스트는 분류명으로 처리
                if first_text.startswith('[') and first_text.endswith(']'):
                    category_name = first_text[1:-1]  # 대괄호 제거
                    current_category = category_name
                    current_subcategory = ""  # 새 중분류시 하위분류 초기화
                    
                    sheet_data['items'].append({
                        'name': f"[중분류] {category_name}",
                        'value': None,
                        'formatted_value': '',
                        'category': category_name,
                        'is_category': True,
                        'original_name': first_text
                    })
                    continue
                
                # A열이 비어있고 B열(또는 그 이후)에 텍스트가 있는 경우 - 들여쓰기된 항목
                if first_col > 0:
                    # 들여쓰기된 항목으로 처리
                    indent_level = first_col
                    
                    # 값 찾기 - 텍스트 다음 위치부터
                    value = None
                    value_type = None
                    
                    # 같은 행에서 첫 번째 텍스트 이후의 값 찾기
                    for i, (col_idx, text) in enumerate(text_positions):
                        if col_idx == first_col and i < len(text_positions) - 1:
                            # 다음 항목이 값일 가능성
                            next_col, next_text = text_positions[i + 1]
                            value, value_type = self._extract_cell_value(next_text)
                            if value is not None:
                                break
                    
                    # 값을 못 찾았으면 첫 텍스트 이후의 모든 셀 확인
                    if value is None:
                        for col_idx in range(first_col + 1, len(row)):
                            if row[col_idx] is not None:
                                value, value_type = self._extract_cell_value(row[col_idx])
                                if value is not None:
                                    break
                    
                    # 들여쓰기 표시와 함께 항목 추가
                    display_name = "  " * indent_level + "└ " + first_text
                    unique_name = f"{current_category}_{current_subcategory}_{first_text}" if current_subcategory else f"{current_category}_{first_text}"
                    
                    sheet_data['items'].append({
                        'name': unique_name,
                        'original_name': first_text,
                        'display_name': display_name,
                        'value': value,
                        'formatted_value': self._format_notes_value(value, value_type) if value is not None else '',
                        'category': current_category,
                        'subcategory': current_subcategory,
                        'is_category': False,
                        'row_number': row_idx + 1,
                        'value_type': value_type,
                        'indent_level': indent_level
                    })
                else:
                    # A열에 있는 항목 (들여쓰기 없음)
                    # 하위 분류일 가능성 체크
                    is_subcategory = False
                    
                    # 다음 행들이 들여쓰기되어 있는지 확인
                    if row_idx + 1 < len(all_data):
                        next_rows_indented = 0
                        for check_idx in range(row_idx + 1, min(row_idx + 6, len(all_data))):
                            if check_idx < len(all_data):
                                check_row = all_data[check_idx]
                                # A열이 비어있고 B열 이후에 데이터가 있는지 확인
                                if check_row and (not check_row[0] or not str(check_row[0]).strip()):
                                    for col in range(1, min(5, len(check_row))):
                                        if check_row[col] and str(check_row[col]).strip():
                                            next_rows_indented += 1
                                            break
                        
                        if next_rows_indented >= 2:
                            is_subcategory = True
                            current_subcategory = first_text
                    
                    if is_subcategory:
                        # 하위 분류로 처리
                        sheet_data['items'].append({
                            'name': f"[하위분류] {first_text}",
                            'value': None,
                            'formatted_value': '',
                            'category': current_category,
                            'subcategory': first_text,
                            'is_category': True,
                            'is_subcategory': True,
                            'original_name': first_text
                        })
                    else:
                        # 일반 항목으로 처리
                        # 값 찾기
                        value = None
                        value_type = None
                        
                        # 같은 행의 다음 열들에서 값 찾기
                        for col_idx in range(first_col + 1, len(row)):
                            if row[col_idx] is not None:
                                value, value_type = self._extract_cell_value(row[col_idx])
                                if value is not None:
                                    break
                        
                        unique_name = f"{current_category}_{current_subcategory}_{first_text}" if current_subcategory else f"{current_category}_{first_text}" if current_category else first_text
                        
                        sheet_data['items'].append({
                            'name': unique_name,
                            'original_name': first_text,
                            'value': value,
                            'formatted_value': self._format_notes_value(value, value_type) if value is not None else '',
                            'category': current_category,
                            'subcategory': current_subcategory,
                            'is_category': False,
                            'row_number': row_idx + 1,
                            'value_type': value_type,
                            'indent_level': 0
                        })
            
            # 결과 요약
            if sheet_data['items']:
                category_count = len([item for item in sheet_data['items'] if item.get('is_category') and not item.get('is_subcategory')])
                subcategory_count = len([item for item in sheet_data['items'] if item.get('is_subcategory')])
                value_count = len([item for item in sheet_data['items'] if item.get('value') is not None])
                text_count = len([item for item in sheet_data['items'] if item.get('value_type') == 'text'])
                number_count = len([item for item in sheet_data['items'] if item.get('value_type') == 'number'])
                
                print(f"      ✅ 추출 완료: 총 {len(sheet_data['items'])}개 항목")
                print(f"         - 중분류: {category_count}개")
                print(f"         - 하위분류: {subcategory_count}개") 
                print(f"         - 값 있음: {value_count}개 (숫자: {number_count}, 텍스트: {text_count})")
            
            return sheet_data if sheet_data['items'] else None
            
        except Exception as e:
            print(f"      ❌ 주석 시트 {sheet_name} 데이터 추출 실패: {str(e)}")
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
        """주석 값 포맷팅 (숫자 및 텍스트 처리, 환경변수 단위 적용)"""
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
            
            # 숫자인 경우 - 환경변수 단위 적용
            elif isinstance(value, (int, float)):
                # 환경변수에서 단위 가져오기
                number_unit = os.environ.get('NUMBER_UNIT', 'million')
                
                if number_unit == 'million':  # 백만원
                    if abs(value) >= 1000000:
                        converted_value = value / 1000000
                        return f"{converted_value:.1f}백만원"
                    else:
                        return f"{value:,.0f}"
                
                elif number_unit == 'hundred_million':  # 억원
                    if abs(value) >= 100000000:
                        converted_value = value / 100000000
                        return f"{converted_value:.2f}억원"
                    elif abs(value) >= 1000000:
                        million_value = value / 1000000
                        return f"{million_value:.1f}백만원"
                    else:
                        return f"{value:,.0f}"
                
                elif number_unit == 'billion':  # 십억원
                    if abs(value) >= 1000000000:
                        converted_value = value / 1000000000
                        return f"{converted_value:.2f}십억원"
                    elif abs(value) >= 100000000:
                        hundred_million_value = value / 100000000
                        return f"{hundred_million_value:.1f}억원"
                    else:
                        return f"{value:,.0f}"
                
                else:  # 기본값: 백만원
                    if abs(value) >= 1000000:
                        converted_value = value / 1000000
                        return f"{converted_value:.1f}백만원"
                    else:
                        return f"{value:,.0f}"
            
            else:
                return str(value)
                
        except Exception as e:
            print(f"    ⚠️ 주석 값 포맷팅 오류 ({value}): {str(e)}")
            return str(value) if value else ''

    def _format_number_for_archive(self, value):
        """Archive용 숫자 포맷팅 (환경변수로 단위 설정)"""
        try:
            if not value:
                return ''
            
            # 숫자 변환
            num = self._clean_number(value)
            if num is None:
                return ''
            
            # 환경변수에서 단위 가져오기 (기본값: 백만원)
            number_unit = os.environ.get('NUMBER_UNIT', 'million')
            
            # 단위별 변환
            if number_unit == 'million':  # 백만원
                unit_value = num / 1000000
                unit_suffix = "백만원"
            elif number_unit == 'hundred_million':  # 억원
                unit_value = num / 100000000
                unit_suffix = "억원"
            elif number_unit == 'billion':  # 십억원
                unit_value = num / 1000000000
                unit_suffix = "십억원"
            else:  # 기본값: 백만원
                unit_value = num / 1000000
                unit_suffix = "백만원"
            
            # 소수점 자리 결정
            if abs(unit_value) >= 1000:
                formatted = f"{unit_value:.0f}"  # 1000 이상은 정수
            elif abs(unit_value) >= 100:
                formatted = f"{unit_value:.1f}"  # 100 이상은 소수점 1자리
            else:
                formatted = f"{unit_value:.2f}"  # 100 미만은 소수점 2자리
            
            # 단위 표시 여부 (처음 한 번만 표시하도록 할 수도 있음)
            # return f"{formatted} {unit_suffix}"  # 단위 포함
            return formatted  # 단위 제외 (헤더에 표시)
                
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
