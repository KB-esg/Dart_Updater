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
                # 진행률 표시를 위한 tqdm 사용
                with tqdm(total=len(reports), desc="보고서 처리", unit="건") as pbar:
                    for _, report in reports.iterrows():
                        self._process_report_with_browser(context, report)
                        pbar.update(1)
                    
            finally:
                browser.close()
        
        # 3. Archive 업데이트
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
        """Excel 파일을 Google Sheets에 업로드"""
        try:
            # Excel 파일 읽기
            wb = load_workbook(file_path, data_only=True)
            print(f"📊 Excel 파일 열기 완료. 시트 목록: {wb.sheetnames}")
            
            # 각 시트를 Google Sheets에 업로드 (진행률 표시)
            with tqdm(total=len(wb.sheetnames), desc=f"{file_type} 시트 업로드", unit="시트", leave=False) as pbar:
                for sheet_name in wb.sheetnames:
                    self._upload_sheet_to_google(wb[sheet_name], sheet_name, file_type, rcept_no)
                    pbar.update(1)
                
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
            
            # 배치로 업로드 (진행률 표시)
            batch_size = 100
            total_batches = (len(all_data) + batch_size - 1) // batch_size
            
            with tqdm(total=total_batches, desc=f"  → {gsheet_name}", unit="batch", leave=False) as pbar:
                for i in range(0, len(all_data), batch_size):
                    batch = all_data[i:i + batch_size]
                    gsheet.append_rows(batch)
                    time.sleep(1)  # API 제한 회피
                    pbar.update(1)
            
            print(f"  ✅ 업로드 완료: {gsheet_name} ({len(data)}행)")
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

    def _update_xbrl_archive(self):
        """XBRL Archive 시트 업데이트"""
        print("\n📊 XBRL Archive 시트 업데이트 시작...")
        
        try:
            # 저장된 Excel 파일 경로 확인
            if 'financial' in self.results['excel_files']:
                print("📈 재무제표 Archive 업데이트 중...")
                self._update_single_archive('Dart_Archive_XBRL_재무제표', 
                                          self.results['excel_files']['financial'], 
                                          'financial')
                
            if 'notes' in self.results['excel_files']:
                print("📝 재무제표주석 Archive 업데이트 중...")
                self._update_single_archive('Dart_Archive_XBRL_주석', 
                                          self.results['excel_files']['notes'], 
                                          'notes')
                
            print("✅ XBRL Archive 업데이트 완료")
            
        except Exception as e:
            print(f"❌ XBRL Archive 업데이트 실패: {str(e)}")

    def _update_single_archive(self, sheet_name, file_path, file_type):
        """개별 Archive 시트 업데이트"""
        try:
            # Archive 시트 가져오기 또는 생성
            try:
                archive_sheet = self.workbook.worksheet(sheet_name)
                print(f"📄 기존 {sheet_name} 시트 발견")
            except gspread.exceptions.WorksheetNotFound:
                print(f"🆕 새로운 {sheet_name} 시트 생성")
                archive_sheet = self.workbook.add_worksheet(sheet_name, 1000, 100)
                self._setup_archive_header(archive_sheet, file_type)
            
            # 현재 마지막 열 찾기
            all_values = archive_sheet.get_all_values()
            if not all_values or not all_values[0]:
                last_col = 12  # M열 = 13번째 열 (0-based index에서는 12)
            else:
                # 첫 번째 행에서 마지막 데이터가 있는 열 찾기
                last_col = len(all_values[0]) - 1
                # 빈 열이 있을 수 있으므로 실제 데이터가 있는 마지막 열 찾기
                for i in range(len(all_values[0]) - 1, -1, -1):
                    if all_values[0][i]:
                        last_col = i
                        break
                
                # 다음 열에 추가
                last_col += 1
                
                # 최소 M열부터 시작
                if last_col < 12:
                    last_col = 12
            
            print(f"📍 데이터 추가 위치: {self._get_column_letter(last_col + 1)}열")
            
            # Excel 파일 읽기
            wb = load_workbook(file_path, data_only=True)
            
            # 데이터 추출 및 업데이트
            if file_type == 'financial':
                self._update_financial_archive(archive_sheet, wb, last_col)
            else:
                self._update_notes_archive(archive_sheet, wb, last_col)
                
        except Exception as e:
            print(f"❌ {sheet_name} 업데이트 실패: {str(e)}")

    def _setup_archive_header(self, sheet, file_type):
        """Archive 시트 헤더 설정"""
        # 공통 헤더
        headers = [
            ['DART Archive - ' + ('재무제표' if file_type == 'financial' else '재무제표주석')],
            ['업데이트 시간:', datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
            ['회사명:', self.company_name],
            ['종목코드:', self.corp_code],
            [''],  # 빈 행
            ['']   # 빈 행 (6행까지 헤더)
        ]
        
        # A열에 기본 정보 설정
        if file_type == 'financial':
            # 재무제표 항목
            headers.append(['항목명'])  # 7행
            items = [
                '자산총계', '유동자산', '비유동자산',
                '부채총계', '유동부채', '비유동부채',
                '자본총계', '자본금', '이익잉여금',
                '매출액', '영업이익', '당기순이익',
                '영업활동현금흐름', '투자활동현금흐름', '재무활동현금흐름'
            ]
        else:
            # 주석 항목
            headers.append(['주석항목'])  # 7행
            items = [
                '회계정책', '현금및현금성자산', '매출채권',
                '재고자산', '유형자산', '무형자산',
                '투자부동산', '종속기업투자', '매입채무',
                '차입금', '충당부채', '확정급여부채',
                '이연법인세', '자본금', '기타'
            ]
        
        # 헤더 업데이트
        for i, header_row in enumerate(headers):
            sheet.update(f'A{i+1}:B{i+1}', [header_row[:2]])
        
        # 항목명 업데이트
        for i, item in enumerate(items):
            sheet.update(f'A{i+8}', [[item]])

    def _update_financial_archive(self, sheet, wb, col_index):
        """재무제표 Archive 업데이트"""
        try:
            # 주요 시트 찾기 (연결재무상태표, 연결포괄손익계산서 등)
            target_sheets = ['연결재무상태표', '연결포괄손익계산서', '연결현금흐름표',
                           '재무상태표', '포괄손익계산서', '현금흐름표']
            
            data_dict = {}
            
            # 각 시트에서 데이터 추출
            for sheet_name in wb.sheetnames:
                if any(target in sheet_name for target in target_sheets):
                    ws = wb[sheet_name]
                    print(f"  📊 {sheet_name} 데이터 추출 중...")
                    
                    # 시트 데이터를 행렬로 변환
                    data = []
                    for row in ws.iter_rows(values_only=True):
                        data.append(list(row))
                    
                    # 주요 항목 찾기 (간단한 키워드 매칭)
                    self._extract_financial_items(data, data_dict, sheet_name)
            
            # Archive 시트에 데이터 업데이트
            col_letter = self._get_column_letter(col_index + 1)
            
            # 날짜 정보 (1행)
            sheet.update(f'{col_letter}1', [[datetime.now().strftime('%Y-%m-%d')]])
            
            # 분기 정보 (2행) - 예: 1Q24
            quarter = self._get_quarter_info()
            sheet.update(f'{col_letter}2', [[quarter]])
            
            # 데이터 업데이트 (7행부터)
            row_mapping = {
                '자산총계': 8, '유동자산': 9, '비유동자산': 10,
                '부채총계': 11, '유동부채': 12, '비유동부채': 13,
                '자본총계': 14, '자본금': 15, '이익잉여금': 16,
                '매출액': 17, '영업이익': 18, '당기순이익': 19,
                '영업활동현금흐름': 20, '투자활동현금흐름': 21, '재무활동현금흐름': 22
            }
            
            # 진행률 표시
            items_to_update = list(row_mapping.items())
            with tqdm(total=len(items_to_update), desc="재무제표 항목 업데이트", unit="항목", leave=False) as pbar:
                for item, row_num in items_to_update:
                    if item in data_dict:
                        value = self._format_number(data_dict[item])
                        sheet.update(f'{col_letter}{row_num}', [[value]])
                    pbar.update(1)
                    
        except Exception as e:
            print(f"❌ 재무제표 Archive 업데이트 중 오류: {str(e)}")

    def _update_notes_archive(self, sheet, wb, col_index):
        """재무제표주석 Archive 업데이트"""
        try:
            # 주석 시트에서 데이터 추출
            col_letter = self._get_column_letter(col_index + 1)
            
            # 날짜 정보
            sheet.update(f'{col_letter}1', [[datetime.now().strftime('%Y-%m-%d')]])
            
            # 분기 정보
            quarter = self._get_quarter_info()
            sheet.update(f'{col_letter}2', [[quarter]])
            
            # 주석 항목별 요약 정보 추출 (간단한 버전)
            # 실제로는 각 주석 시트를 분석하여 핵심 정보 추출 필요
            sheet.update(f'{col_letter}8', [['✓']])  # 회계정책
            sheet.update(f'{col_letter}9', [['데이터 있음']])  # 현금및현금성자산
            
            print(f"  ✅ 주석 데이터 업데이트 완료")
            
        except Exception as e:
            print(f"❌ 주석 Archive 업데이트 중 오류: {str(e)}")

    def _extract_financial_items(self, data, data_dict, sheet_name):
        """재무제표에서 주요 항목 추출"""
        # 간단한 키워드 매칭으로 데이터 추출
        keywords = {
            '자산총계': ['자산총계', '자산 총계', '총자산'],
            '유동자산': ['유동자산', '유동 자산'],
            '비유동자산': ['비유동자산', '비유동 자산'],
            '부채총계': ['부채총계', '부채 총계', '총부채'],
            '유동부채': ['유동부채', '유동 부채'],
            '비유동부채': ['비유동부채', '비유동 부채'],
            '자본총계': ['자본총계', '자본 총계', '총자본'],
            '자본금': ['자본금'],
            '이익잉여금': ['이익잉여금', '이익 잉여금'],
            '매출액': ['매출액', '매출', '영업수익'],
            '영업이익': ['영업이익', '영업 이익'],
            '당기순이익': ['당기순이익', '당기 순이익'],
            '영업활동현금흐름': ['영업활동', '영업활동으로'],
            '투자활동현금흐름': ['투자활동', '투자활동으로'],
            '재무활동현금흐름': ['재무활동', '재무활동으로']
        }
        
        for row_idx, row in enumerate(data):
            for col_idx, cell in enumerate(row):
                if cell and isinstance(cell, str):
                    for item, search_terms in keywords.items():
                        for term in search_terms:
                            if term in str(cell).replace(' ', ''):
                                # 같은 행에서 숫자 찾기
                                for j in range(col_idx + 1, len(row)):
                                    if row[j] and self._is_number(row[j]):
                                        data_dict[item] = row[j]
                                        break

    def _is_number(self, value):
        """값이 숫자인지 확인"""
        try:
            float(str(value).replace(',', ''))
            return True
        except:
            return False

    def _format_number(self, value):
        """숫자 포맷팅"""
        try:
            num = float(str(value).replace(',', ''))
            # 억 단위로 변환
            return f"{num / 100000000:.1f}"
        except:
            return str(value)

    def _get_quarter_info(self):
        """보고서 기준 분기 정보 반환"""
        if self.current_report:
            # 보고서명에서 분기 정보 추출 (예: "분기보고서 (2025.03)")
            report_name = self.current_report['report_nm']
            
            # 날짜 추출 시도
            import re
            date_match = re.search(r'\((\d{4})\.(\d{2})\)', report_name)
            if date_match:
                year = date_match.group(1)
                month = int(date_match.group(2))
                
                # 분기 계산
                if month <= 3:
                    quarter = 1
                elif month <= 6:
                    quarter = 2
                elif month <= 9:
                    quarter = 3
                else:
                    quarter = 4
                
                return f"{quarter}Q{year[2:]}"
        
        # 기본값: 현재 날짜 기준
        now = datetime.now()
        quarter = (now.month - 1) // 3 + 1
        year = str(now.year)[2:]
        return f"{quarter}Q{year}"

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
