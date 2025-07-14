import os
from datetime import datetime, timedelta
import json
import time
import re
import gspread
from google.oauth2.service_account import Credentials
import OpenDartReader
import requests
from bs4 import BeautifulSoup
from html_table_parser import parser_functions as parser
import pandas as pd
from openpyxl import load_workbook
from playwright.sync_api import sync_playwright
import shutil
from tqdm import tqdm

class DartDualUpdater:
    """DART XBRL Excel 다운로드 + HTML 스크래핑 통합 시스템"""
    
    # HTML 스크래핑 대상 시트 (재무제표 관련 제외)
    HTML_TARGET_SHEETS = [
        'I. 회사의 개요', 'II. 사업의 내용', '1. 사업의 개요', '2. 주요 제품 및 서비스',
        '3. 원재료 및 생산설비', '4. 매출 및 수주상황', '5. 위험관리 및 파생거래',
        '6. 주요계약 및 연구활동', '7. 기타 참고 사항', '1. 요약재무정보',
        # 재무제표 관련 시트 제외 (XBRL에서 처리)
        # '2. 연결재무제표', '3. 연결재무제표 주석', '4. 재무제표', '5. 재무제표 주석',
        '6. 배당에 관한 사항', '8. 기타 재무에 관한 사항', 'VII. 주주에 관한 사항',
        'VIII. 임원 및 직원 등에 관한 사항', 'X. 대주주 등과의 거래내용',
        'XI. 그 밖에 투자자 보호를 위하여 필요한 사항'
    ]
    
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
        
        # 다운로드 폴더 설정 (XBRL용)
        self.download_dir = os.path.join(os.getcwd(), 'downloads')
        os.makedirs(self.download_dir, exist_ok=True)
        
        # 처리 결과 추적
        self.results = {
            'total_reports': 0,
            'xbrl': {
                'downloaded_files': [],
                'uploaded_sheets': [],
                'failed_downloads': [],
                'failed_uploads': [],
                'excel_files': {}
            },
            'html': {
                'processed_sheets': [],
                'failed_sheets': []
            }
        }
        
        # 현재 처리 중인 보고서 정보
        self.current_report = None
        
        # Archive 시트 행 영역 매핑 설정
        self._setup_archive_row_mapping()

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

    def _setup_archive_row_mapping(self):
        """Archive 시트의 행 영역 매핑 설정"""
        # 재무제표 Archive 시트 행 매핑
        self.financial_row_mapping = {
            'consolidated': {
                'D210000': {'start': 7, 'end': 80, 'name': '연결 재무상태표'},
                'D431410': {'start': 81, 'end': 140, 'name': '연결 손익계산서'},
                'D520000': {'start': 141, 'end': 200, 'name': '연결 현금흐름표'},
                'D610000': {'start': 201, 'end': 250, 'name': '연결 자본변동표'}
            },
            'standalone': {
                'D210005': {'start': 257, 'end': 330, 'name': '별도 재무상태표'},
                'D431415': {'start': 331, 'end': 390, 'name': '별도 손익계산서'},
                'D520005': {'start': 391, 'end': 450, 'name': '별도 현금흐름표'},
                'D610005': {'start': 451, 'end': 500, 'name': '별도 자본변동표'}
            }
        }

    def run(self):
        """메인 실행 함수 (XBRL + HTML 통합)"""
        print(f"\n🚀 {self.company_name}({self.corp_code}) DART 통합 업데이트 시작")
        print("📊 업데이트 모드: XBRL Excel + HTML 스크래핑")
        
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
        
        # 2. 각 보고서에 대해 XBRL과 HTML 처리 병행
        print("\n" + "="*50)
        print("📄 XBRL Excel 다운로드 시작")
        print("="*50)
        
        # XBRL 처리 (Playwright 사용)
        with sync_playwright() as p:
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
                with tqdm(total=len(reports), desc="XBRL 처리", unit="건") as pbar:
                    for _, report in reports.iterrows():
                        self._process_xbrl_report(context, report)
                        pbar.update(1)
            finally:
                browser.close()
        
        # 3. HTML 스크래핑 처리
        print("\n" + "="*50)
        print("🌐 HTML 스크래핑 시작")
        print("="*50)
        
        with tqdm(total=len(reports), desc="HTML 처리", unit="건") as pbar:
            for _, report in reports.iterrows():
                self._process_html_report(report['rcept_no'])
                pbar.update(1)
        
        # 4. XBRL Archive 업데이트
        if os.environ.get('ENABLE_ARCHIVE_UPDATE', 'true').lower() == 'true':
            self._update_xbrl_archive()
        
        # 5. HTML Archive 업데이트
        if os.environ.get('ENABLE_HTML_ARCHIVE', 'true').lower() == 'true':
            self._update_html_archive()
        
        # 6. 결과 요약
        self._print_summary()
        
        # 7. 다운로드 폴더 정리
        self._cleanup_downloads()

    def _get_recent_reports(self):
        """최근 보고서 목록 조회"""
        start_date, end_date = self._get_date_range()
        return self.dart.list(self.corp_code, start_date, end_date, kind='A', final='T')

    def _get_date_range(self):
        """날짜 범위 계산"""
        manual_start = os.environ.get('MANUAL_START_DATE')
        manual_end = os.environ.get('MANUAL_END_DATE')
        
        if manual_start and manual_end:
            print(f"📅 수동 설정 날짜: {manual_start} ~ {manual_end}")
            return manual_start, manual_end
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=90)
        date_range = start_date.strftime('%Y%m%d'), end_date.strftime('%Y%m%d')
        print(f"📅 기본 날짜 범위 (최근 3개월): {date_range[0]} ~ {date_range[1]}")
        return date_range

    # === XBRL 관련 메서드 ===
    def _process_xbrl_report(self, context, report):
        """XBRL 보고서 처리"""
        print(f"\n📄 XBRL 처리: {report['report_nm']} (접수번호: {report['rcept_no']})")
        
        self.current_report = report
        page = context.new_page()
        
        try:
            viewer_url = f"https://opendart.fss.or.kr/xbrl/viewer/main.do?rcpNo={report['rcept_no']}"
            print(f"🌐 페이지 열기: {viewer_url}")
            
            page.goto(viewer_url, wait_until='networkidle', timeout=60000)
            page.wait_for_timeout(2000)
            
            download_button = page.locator('button.btnDown').first
            if not download_button.is_visible():
                print("⚠️ 다운로드 버튼을 찾을 수 없습니다.")
                self.results['xbrl']['failed_downloads'].append(report['rcept_no'])
                return
            
            print("🖱️ 다운로드 버튼 클릭")
            
            with page.expect_popup() as popup_info:
                download_button.click()
            
            popup = popup_info.value
            popup.wait_for_load_state('networkidle')
            
            self._download_excel_files(popup, report['rcept_no'])
            popup.close()
            
        except Exception as e:
            print(f"❌ XBRL 처리 실패: {str(e)}")
            self.results['xbrl']['failed_downloads'].append(report['rcept_no'])
        finally:
            page.close()

    def _download_excel_files(self, popup_page, rcept_no):
        """팝업 페이지에서 Excel 파일 다운로드"""
        try:
            popup_page.wait_for_timeout(2000)
            print(f"📍 팝업 페이지 URL: {popup_page.url}")
            
            download_links = popup_page.locator('a.btnFile')
            link_count = download_links.count()
            print(f"📄 다운로드 가능한 파일 수: {link_count}개")
            
            # 재무제표 다운로드
            if link_count >= 1:
                print("📥 재무제표 다운로드 중...")
                
                with popup_page.expect_download() as download_info:
                    download_links.nth(0).click()
                
                download = download_info.value
                file_path = os.path.join(self.download_dir, f"재무제표_{rcept_no}.xlsx")
                download.save_as(file_path)
                
                print(f"✅ 재무제표 다운로드 완료: {file_path}")
                self.results['xbrl']['downloaded_files'].append(file_path)
                self.results['xbrl']['excel_files']['financial'] = file_path
                
                self._upload_excel_to_sheets(file_path, "재무제표", rcept_no)
                popup_page.wait_for_timeout(2000)
            
            # 재무제표주석 다운로드
            if link_count >= 2:
                print("📥 재무제표주석 다운로드 중...")
                
                with popup_page.expect_download() as download_info:
                    download_links.nth(1).click()
                
                download = download_info.value
                file_path = os.path.join(self.download_dir, f"재무제표주석_{rcept_no}.xlsx")
                download.save_as(file_path)
                
                print(f"✅ 재무제표주석 다운로드 완료: {file_path}")
                self.results['xbrl']['downloaded_files'].append(file_path)
                self.results['xbrl']['excel_files']['notes'] = file_path
                
                self._upload_excel_to_sheets(file_path, "재무제표주석", rcept_no)
                
        except Exception as e:
            print(f"❌ Excel 다운로드 실패: {str(e)}")
            self.results['xbrl']['failed_downloads'].append(f"Excel_{rcept_no}")

    # === HTML 스크래핑 관련 메서드 ===
    def _process_html_report(self, rcept_no):
        """HTML 보고서 처리"""
        try:
            print(f"\n🌐 HTML 처리: 보고서 접수번호 {rcept_no}")
            
            # 보고서 하위 문서 목록 조회
            report_index = self.dart.sub_docs(rcept_no)
            
            # HTML 대상 시트만 필터링 (재무제표 관련 제외)
            target_docs = report_index[report_index['title'].isin(self.HTML_TARGET_SHEETS)]
            
            print(f"📝 처리할 HTML 문서: {len(target_docs)}개")
            
            for _, doc in target_docs.iterrows():
                self._update_html_worksheet(doc['title'], doc['url'])
                
        except Exception as e:
            print(f"❌ HTML 보고서 처리 실패: {str(e)}")

    def _update_html_worksheet(self, sheet_name, url):
        """HTML 워크시트 업데이트"""
        try:
            # 워크시트 가져오기 또는 생성
            try:
                worksheet = self.workbook.worksheet(sheet_name)
            except gspread.exceptions.WorksheetNotFound:
                worksheet = self.workbook.add_worksheet(sheet_name, 1000, 10)
                print(f"🆕 새 시트 생성: {sheet_name}")
            
            # HTML 내용 가져오기
            response = requests.get(url)
            if response.status_code == 200:
                self._process_html_content(worksheet, response.text)
                print(f"✅ HTML 시트 업데이트 완료: {sheet_name}")
                self.results['html']['processed_sheets'].append(sheet_name)
            else:
                print(f"❌ HTML 가져오기 실패: {sheet_name}")
                self.results['html']['failed_sheets'].append(sheet_name)
                
        except Exception as e:
            print(f"❌ HTML 워크시트 업데이트 실패 ({sheet_name}): {str(e)}")
            self.results['html']['failed_sheets'].append(sheet_name)

    def _process_html_content(self, worksheet, html_content):
        """HTML 내용 처리 및 워크시트 업데이트"""
        soup = BeautifulSoup(html_content, 'html.parser')
        tables = soup.find_all("table")
        
        worksheet.clear()
        all_data = []
        
        for table in tables:
            table_data = parser.make2d(table)
            if table_data:
                all_data.extend(table_data)
        
        # 배치 업데이트
        BATCH_SIZE = 100
        for i in range(0, len(all_data), BATCH_SIZE):
            batch = all_data[i:i + BATCH_SIZE]
            try:
                worksheet.append_rows(batch)
                time.sleep(1)  # API 제한 회피
            except gspread.exceptions.APIError as e:
                if 'Quota exceeded' in str(e):
                    print("⏳ 할당량 제한. 60초 대기...")
                    time.sleep(60)
                    worksheet.append_rows(batch)
                else:
                    raise e

    def _update_html_archive(self):
        """HTML Archive 시트 업데이트"""
        try:
            print("\n📊 HTML Archive 시트 업데이트 시작...")
            
            # Dart_Archive 시트 접근
            archive = self.workbook.worksheet('Dart_Archive')
            sheet_values = archive.get_all_values()
            
            if not sheet_values:
                print("⚠️ Dart_Archive 시트가 비어있습니다")
                return
            
            last_col = len(sheet_values[0])
            control_value = archive.cell(1, last_col).value
            
            # control_value에 따라 열 조정
            if control_value:
                last_col += 1
            
            # 아카이브 데이터 처리
            self._process_archive_data(archive, 10, last_col)
            print("✅ HTML Archive 업데이트 완료")
            
        except Exception as e:
            print(f"❌ HTML Archive 업데이트 실패: {str(e)}")

    def _process_archive_data(self, archive, start_row, last_col):
        """아카이브 데이터 처리 (HTML 스크래핑용)"""
        try:
            current_cols = archive.col_count
            current_col_letter = self._get_column_letter(current_cols)
            target_col_letter = self._get_column_letter(last_col)
            
            print(f"시작 행: {start_row}, 대상 열: {last_col} ({target_col_letter})")
            print(f"현재 시트 열 수: {current_cols} ({current_col_letter})")
            
            # 필요한 경우 시트 크기 조정
            if last_col >= current_cols:
                new_cols = last_col + 5
                try:
                    print(f"시트 크기를 {current_cols}({current_col_letter})에서 {new_cols}({self._get_column_letter(new_cols)})로 조정합니다.")
                    archive.resize(rows=archive.row_count, cols=new_cols)
                    time.sleep(2)
                    print("시트 크기 조정 완료")
                except Exception as e:
                    print(f"시트 크기 조정 중 오류 발생: {str(e)}")
                    raise

            # 데이터 수집 시작
            all_rows = archive.get_all_values()
            update_data = []
            sheet_cache = {}
            
            sheet_rows = {}
            for row_idx in range(start_row - 1, len(all_rows)):
                if len(all_rows[row_idx]) < 5:
                    print(f"행 {row_idx + 1}: 데이터 부족 (컬럼 수: {len(all_rows[row_idx])})")
                    continue
                    
                sheet_name = all_rows[row_idx][0]
                if not sheet_name:
                    print(f"행 {row_idx + 1}: 시트명 없음")
                    continue
                
                print(f"행 {row_idx + 1} 처리: 시트={sheet_name}, " + 
                      f"키워드={all_rows[row_idx][1]}, n={all_rows[row_idx][2]}, " +
                      f"x={all_rows[row_idx][3]}, y={all_rows[row_idx][4]}")
                
                if sheet_name not in sheet_rows:
                    sheet_rows[sheet_name] = []
                sheet_rows[sheet_name].append({
                    'row_idx': row_idx + 1,
                    'keyword': all_rows[row_idx][1],
                    'n': all_rows[row_idx][2],
                    'x': all_rows[row_idx][3],
                    'y': all_rows[row_idx][4]
                })
            
            for sheet_name, rows in sheet_rows.items():
                try:
                    print(f"\n시트 '{sheet_name}' 처리 중...")
                    print(f"검색할 키워드 수: {len(rows)}")
                    
                    if sheet_name not in sheet_cache:
                        search_sheet = self.workbook.worksheet(sheet_name)
                        sheet_data = search_sheet.get_all_values()
                        df = pd.DataFrame(sheet_data)
                        sheet_cache[sheet_name] = df
                        print(f"시트 '{sheet_name}' 데이터 로드 완료 (크기: {df.shape})")
                    
                    df = sheet_cache[sheet_name]
                    
                    for row in rows:
                        keyword = row['keyword']
                        if not keyword or not row['n'] or not row['x'] or not row['y']:
                            print(f"행 {row['row_idx']}: 검색 정보 부족")
                            continue
                        
                        try:
                            n = int(row['n'])
                            x = int(row['x'])
                            y = int(row['y'])
                            
                            keyword_positions = []
                            for idx, df_row in df.iterrows():
                                for col_idx, value in enumerate(df_row):
                                    if value == keyword:
                                        keyword_positions.append((idx, col_idx))
                            
                            print(f"키워드 '{keyword}' 검색 결과: {len(keyword_positions)}개 발견")
                            
                            if keyword_positions and len(keyword_positions) >= n:
                                target_pos = keyword_positions[n - 1]
                                target_row = target_pos[0] + y
                                target_col = target_pos[1] + x
                                
                                if target_row >= 0 and target_row < df.shape[0] and \
                                   target_col >= 0 and target_col < df.shape[1]:
                                    value = df.iat[target_row, target_col]
                                    cleaned_value = self._remove_parentheses(str(value))
                                    print(f"찾은 값: {cleaned_value} (키워드: {keyword})")
                                    update_data.append((row['row_idx'], cleaned_value))
                                else:
                                    print(f"행 {row['row_idx']}: 대상 위치가 범위를 벗어남 ({target_row}, {target_col})")
                            else:
                                print(f"행 {row['row_idx']}: 키워드 '{keyword}'를 {n}번째로 찾을 수 없음")
                        
                        except Exception as e:
                            print(f"행 {row['row_idx']} 처리 중 오류: {str(e)}")
                
                except Exception as e:
                    print(f"시트 '{sheet_name}' 처리 중 오류 발생: {str(e)}")
            
            print(f"\n업데이트할 데이터 수: {len(update_data)}")
            
            if update_data:
                try:
                    # 업데이트할 열의 데이터만 준비
                    column_data = []
                    min_row = min(row for row, _ in update_data)
                    max_row = max(row for row, _ in update_data)
                    
                    # 빈 데이터로 초기화
                    for _ in range(max_row - min_row + 1):
                        column_data.append([''])
                    
                    # 업데이트할 데이터 삽입
                    for row, value in update_data:
                        adjusted_row = row - min_row
                        column_data[adjusted_row] = [value]
                    
                    # 단일 열 업데이트
                    range_label = f'{target_col_letter}{min_row}:{target_col_letter}{max_row}'
                    print(f"업데이트 범위: {range_label}")
                    
                    archive.batch_update([{
                        'range': range_label,
                        'values': column_data
                    }])
                    print(f"데이터 업데이트 완료: {min_row}~{max_row} 행")
                    
                    # 메타데이터 업데이트
                    today = datetime.now()
                    three_months_ago = today - timedelta(days=90)
                    year = str(three_months_ago.year)[2:]
                    quarter = (three_months_ago.month - 1) // 3 + 1
                    quarter_text = f"{quarter}Q{year}"
                    
                    meta_updates = [
                        {'range': 'J1', 'values': [[today.strftime('%Y-%m-%d')]]},
                        {'range': f'{target_col_letter}1', 'values': [['1']]},
                        {'range': f'{target_col_letter}5', 'values': [[today.strftime('%Y-%m-%d')]]},
                        {'range': f'{target_col_letter}6', 'values': [[quarter_text]]}
                    ]
                    
                    archive.batch_update(meta_updates)
                    print(f"최종 업데이트 완료 (이전 분기: {quarter_text})")
                    
                    message = (
                        f"🔄 HTML Archive 업데이트 완료\n\n"
                        f"• 종목: {self.company_name} ({self.corp_code})\n"
                        f"• 분기: {quarter_text}\n"
                        f"• 업데이트 일시: {today.strftime('%Y-%m-%d %H:%M:%S')}\n"
                        f"• 처리된 행: {len(update_data)}개\n"
                        f"• 시트 열: {target_col_letter} (#{last_col})"
                    )
                    self._send_telegram_message(message)
                    
                except Exception as e:
                    error_msg = f"업데이트 중 오류 발생: {str(e)}"
                    print(error_msg)
                    self._send_telegram_message(f"❌ {error_msg}")
                    raise e
                    
        except Exception as e:
            error_msg = f"아카이브 처리 중 오류 발생: {str(e)}"
            print(error_msg)
            self._send_telegram_message(f"❌ {error_msg}")
            raise e

    def _remove_parentheses(self, value):
        """괄호 내용 제거"""
        if not value:
            return value
        return re.sub(r'\s*\(.*?\)\s*', '', value).replace('%', '')

    def _upload_excel_to_sheets(self, file_path, file_type, rcept_no):
        """Excel 파일을 Google Sheets에 업로드"""
        try:
            wb = load_workbook(file_path, data_only=True)
            print(f"📊 Excel 파일 열기 완료. 시트 목록: {wb.sheetnames}")
            
            all_sheets_data = {}
            
            print(f"📥 {file_type} 데이터 수집 중...")
            with tqdm(total=len(wb.sheetnames), desc="데이터 수집", unit="시트", leave=False) as pbar:
                for sheet_name in wb.sheetnames:
                    data = []
                    worksheet = wb[sheet_name]
                    for row in worksheet.iter_rows(values_only=True):
                        row_data = [str(cell) if cell is not None else '' for cell in row]
                        if any(row_data):
                            data.append(row_data)
                    
                    if data:
                        gsheet_name = f"{file_type}_{sheet_name.replace(' ', '_')}"
                        if len(gsheet_name) > 100:
                            gsheet_name = gsheet_name[:97] + "..."
                        
                        all_sheets_data[gsheet_name] = {
                            'original_name': sheet_name,
                            'data': data
                        }
                    
                    pbar.update(1)
            
            print(f"📤 Google Sheets에 업로드 중... (총 {len(all_sheets_data)}개 시트)")
            self._batch_upload_to_google_sheets(all_sheets_data, rcept_no)
            
        except Exception as e:
            print(f"❌ Excel 처리 실패: {str(e)}")
            self.results['xbrl']['failed_uploads'].append(file_path)

    def _batch_upload_to_google_sheets(self, all_sheets_data, rcept_no):
        """여러 시트를 배치로 Google Sheets에 업로드"""
        try:
            existing_sheets = [ws.title for ws in self.workbook.worksheets()]
            
            sheets_to_create = []
            sheets_to_update = []
            
            for gsheet_name in all_sheets_data:
                if gsheet_name in existing_sheets:
                    sheets_to_update.append(gsheet_name)
                else:
                    sheets_to_create.append(gsheet_name)
            
            # 새 시트 생성
            if sheets_to_create:
                print(f"🆕 새 시트 {len(sheets_to_create)}개 생성 중...")
                
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
                    
                    time.sleep(3)
            
            # 기존 시트 클리어
            if sheets_to_update:
                print(f"🧹 기존 시트 {len(sheets_to_update)}개 초기화 중...")
                for sheet_name in sheets_to_update:
                    try:
                        worksheet = self.workbook.worksheet(sheet_name)
                        worksheet.clear()
                        time.sleep(1)
                    except Exception as e:
                        print(f"⚠️ 시트 초기화 실패 {sheet_name}: {str(e)}")
            
            # 데이터 업로드
            print(f"📝 데이터 업로드 중...")
            
            upload_count = 0
            total_sheets = len(all_sheets_data)
            
            with tqdm(total=total_sheets, desc="시트 업로드", unit="시트") as pbar:
                for gsheet_name, sheet_info in all_sheets_data.items():
                    try:
                        worksheet = self.workbook.worksheet(gsheet_name)
                        
                        header = [
                            [f"업데이트: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"],
                            [f"보고서: {rcept_no}"],
                            [f"원본 시트: {sheet_info['original_name']}"],
                            []
                        ]
                        
                        all_data = header + sheet_info['data']
                        
                        if len(all_data) > 0:
                            end_row = len(all_data)
                            end_col = max(len(row) for row in all_data) if all_data else 1
                            end_col_letter = self._get_column_letter(end_col - 1)
                            
                            range_name = f'A1:{end_col_letter}{end_row}'
                            worksheet.update(values=all_data, range_name=range_name)
                        
                        self.results['xbrl']['uploaded_sheets'].append(gsheet_name)
                        upload_count += 1
                        
                        if upload_count % 10 == 0:
                            print(f"  💤 API 제한 회피를 위해 10초 대기 중...")
                            time.sleep(10)
                        else:
                            time.sleep(2)
                        
                    except Exception as e:
                        print(f"❌ 시트 업로드 실패 '{gsheet_name}': {str(e)}")
                        self.results['xbrl']['failed_uploads'].append(gsheet_name)
                        
                        if "429" in str(e):
                            print(f"  ⏳ API 할당량 초과. 30초 대기 중...")
                            time.sleep(30)
                    
                    pbar.update(1)
            
            print(f"✅ 업로드 완료: 성공 {upload_count}/{total_sheets}개")
            
        except Exception as e:
            print(f"❌ 배치 업로드 실패: {str(e)}")

    def _update_xbrl_archive(self):
        """XBRL Archive 시트 업데이트"""
        print("\n📊 XBRL Archive 시트 업데이트 시작...")
        
        try:
            if 'financial' in self.results['xbrl']['excel_files']:
                print("📈 XBRL 재무제표 Archive 업데이트 중...")
                self._update_single_xbrl_archive('Dart_Archive_XBRL_재무제표', 
                                               self.results['xbrl']['excel_files']['financial'], 
                                               'financial')
            
            if 'notes' in self.results['xbrl']['excel_files']:
                print("📝 XBRL 재무제표주석 Archive 업데이트 중...")
                
                # 주석 데이터 수정된 메서드 적용
                self._update_single_xbrl_archive('Dart_Archive_XBRL_주석_연결', 
                                               self.results['xbrl']['excel_files']['notes'], 
                                               'notes_consolidated')
                
                self._update_single_xbrl_archive('Dart_Archive_XBRL_주석_별도', 
                                               self.results['xbrl']['excel_files']['notes'], 
                                               'notes_standalone')
            
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
            elif file_type == 'notes_consolidated':
                self._update_xbrl_notes_archive_batch(archive_sheet, wb, last_col, 'consolidated')
            elif file_type == 'notes_standalone':
                self._update_xbrl_notes_archive_batch(archive_sheet, wb, last_col, 'standalone')
                
        except Exception as e:
            print(f"❌ {sheet_name} 업데이트 실패: {str(e)}")
            
            if "429" in str(e):
                print(f"  ⏳ API 할당량 초과. 60초 대기 중...")
                time.sleep(60)

    def _setup_xbrl_archive_header(self, sheet, file_type):
        """XBRL Archive 시트 헤더 설정"""
        try:
            current_date = datetime.now().strftime('%Y-%m-%d')
            
            number_unit = os.environ.get('NUMBER_UNIT', 'million')
            unit_text = {
                'million': '백만원',
                'hundred_million': '억원',
                'billion': '십억원'
            }.get(number_unit, '백만원')
            
            header_data = []
            
            if file_type == 'financial':
                title_row = ['DART Archive XBRL 재무제표', '', '', '', '', '', '', '', '', f'최종업데이트: {current_date}', '', '계정과목']
            else:
                title_row = ['DART Archive XBRL 재무제표주석', '', '', '', '', '', '', '', '', f'최종업데이트: {current_date}', '', '계정과목']
            header_data.append(title_row)
            
            company_row = [f'회사명: {self.company_name}', '', '', '', '', '', '', '', '', f'단위: {unit_text}', '', '항목명↓']
            header_data.append(company_row)
            
            stock_row = [f'종목코드: {self.corp_code}', '', '', '', '', '', '', '', '', '', '', '']
            header_data.append(stock_row)
            
            for _ in range(3):
                header_data.append(['', '', '', '', '', '', '', '', '', '', '', ''])
            
            end_row = len(header_data)
            range_name = f'A1:L{end_row}'
            
            print(f"  📋 XBRL Archive 기본 헤더 설정: {range_name}")
            sheet.update(values=header_data, range_name=range_name)
            
            print(f"  ✅ XBRL Archive 기본 레이아웃 완료")
            
        except Exception as e:
            print(f"  ❌ XBRL Archive 헤더 설정 실패: {str(e)}")

    def _find_last_data_column(self, sheet):
        """마지막 데이터 열 찾기 (M열부터 시작)"""
        try:
            row_2_values = sheet.row_values(2)
            
            last_col = 11  # M열 = 12번째 열 (0-based index에서는 11)
            
            for i in range(11, len(row_2_values)):
                if row_2_values[i]:
                    last_col = i
            
            next_col = last_col + 1
            
            if next_col < 11:
                next_col = 11
            
            col_letter = self._get_column_letter(next_col)
            print(f"📍 새 데이터 추가 위치: {col_letter}열 (인덱스: {next_col})")
            
            return next_col
            
        except Exception as e:
            print(f"⚠️ 마지막 열 찾기 실패: {str(e)}")
            return 11

    def _update_xbrl_financial_archive_batch(self, sheet, wb, col_index):
        """XBRL 재무제표 Archive 업데이트"""
        try:
            print(f"  📊 XBRL 재무제표 데이터 추출 중...")
            
            col_letter = self._get_column_letter(col_index)
            print(f"  📍 데이터 입력 위치: {col_letter}열")
            
            # 기존 L열의 계정명 읽어오기
            existing_accounts = set()
            try:
                l_column_values = sheet.col_values(12)
                for idx, account in enumerate(l_column_values[6:], start=7):
                    if account and account.strip():
                        existing_accounts.add(account.strip())
                
                print(f"  📋 기존 계정명 {len(existing_accounts)}개 발견")
            except Exception as e:
                print(f"  ⚠️ 기존 계정명 읽기 실패: {str(e)}")
            
            # 헤더 정보 업데이트
            report_date = datetime.now().strftime('%Y-%m-%d')
            quarter_info = self._get_quarter_info()
            
            # 모든 재무 데이터를 메모리에서 준비
            all_account_data, all_value_data = self._prepare_financial_data_for_batch_update(wb)
            
            # 신규 계정명 추적
            new_accounts = []
            for idx, account_row in enumerate(all_account_data):
                if account_row and account_row[0]:
                    account_name = account_row[0]
                    if (not account_name.startswith('[') and 
                        not account_name.startswith('===') and
                        account_name not in existing_accounts):
                        new_accounts.append((idx, account_name))
            
            if new_accounts:
                print(f"  🆕 신규 계정명 {len(new_accounts)}개 발견")
            
            # 배치 업데이트
            print(f"  🚀 대용량 배치 업데이트 시작...")
            
            # 헤더 정보
            header_range = f'{col_letter}1:{col_letter}2'
            header_data = [[quarter_info], [report_date]]
            sheet.update(values=header_data, range_name=header_range)
            print(f"    ✅ 헤더 정보 업데이트 완료")
            
            # L열 계정명
            if all_account_data:
                account_range = f'L7:L{6 + len(all_account_data)}'
                sheet.update(values=all_account_data, range_name=account_range)
                print(f"    ✅ L열 계정명 업데이트 완료")
            
            time.sleep(2)
            
            # M열 값
            if all_value_data:
                value_range = f'{col_letter}7:{col_letter}{6 + len(all_value_data)}'
                sheet.update(values=all_value_data, range_name=value_range)
                print(f"    ✅ {col_letter}열 값 업데이트 완료")
            
            print(f"  ✅ XBRL 재무제표 Archive 배치 업데이트 완료")
            
        except Exception as e:
            print(f"❌ XBRL 재무제표 Archive 업데이트 실패: {str(e)}")

    def _prepare_financial_data_for_batch_update(self, wb):
        """재무 데이터를 배치 업데이트용으로 준비"""
        try:
            print(f"  🔄 배치 업데이트용 데이터 준비 중...")
            
            all_account_data = []
            all_value_data = []
            
            # D로 시작하는 시트 처리
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
                    continue
                
                # 시트명 헤더 추가
                header_text = f"{sheet_type} {fs_type} ({sheet_name})"
                all_account_data.append([header_text])
                all_value_data.append([''])
                
                # 데이터 추출
                data_count = 0
                for row_idx in range(1, min(worksheet.max_row + 1, 500)):
                    row = list(worksheet.iter_rows(min_row=row_idx, max_row=row_idx, values_only=True))[0]
                    
                    if not row or len(row) < 2:
                        continue
                    
                    # A열: 계정명
                    account_name = str(row[0]).strip() if row[0] else ''
                    
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
                    
                    all_account_data.append([account_name])
                    all_value_data.append([self._format_number_for_archive(value) if value else ''])
                    data_count += 1
                
                if data_count > 0:
                    print(f"    ✅ {sheet_name}: {data_count}개 항목 추가")
                    all_account_data.append([''])
                    all_value_data.append([''])
            
            return all_account_data, all_value_data
            
        except Exception as e:
            print(f"  ❌ 배치 데이터 준비 실패: {str(e)}")
            return [], []

    def _find_sheet_title(self, worksheet):
        """시트 제목 찾기"""
        try:
            for row in worksheet.iter_rows(min_row=1, max_row=10, values_only=True):
                for cell in row:
                    if cell and isinstance(cell, str):
                        if any(keyword in str(cell) for keyword in ['재무상태표', '손익계산서', '현금흐름표', '자본변동표', '포괄손익']):
                            return str(cell).strip()
            return None
        except:
            return None

    def _update_xbrl_notes_archive_batch(self, sheet, wb, col_index, notes_type='consolidated'):
        """XBRL 재무제표주석 Archive 업데이트"""
        try:
            print(f"  📝 XBRL 주석 데이터 분석 중... ({notes_type})")
            
            col_letter = self._get_column_letter(col_index)
            print(f"  📍 데이터 입력 위치: {col_letter}열")
            
            # 헤더 정보 업데이트
            report_date = datetime.now().strftime('%Y-%m-%d')
            quarter_info = self._get_quarter_info()
            
            # 모든 주석 데이터를 메모리에서 준비
            all_notes_account_data, all_notes_value_data = self._prepare_notes_data_for_batch_update(wb, notes_type)
            
            # 배치 업데이트
            print(f"  🚀 주석 배치 업데이트 시작...")
            
            # 헤더 정보
            header_range = f'{col_letter}1:{col_letter}2'
            header_data = [[quarter_info], [report_date]]
            sheet.update(values=header_data, range_name=header_range)
            print(f"    ✅ 헤더 정보 업데이트 완료")
            
            # L열 주석 항목명
            if all_notes_account_data:
                account_range = f'L7:L{6 + len(all_notes_account_data)}'
                sheet.update(values=all_notes_account_data, range_name=account_range)
                print(f"    ✅ L열 주석 항목 업데이트 완료")
            
            time.sleep(2)
            
            # M열 주석 값
            if all_notes_value_data:
                value_range = f'{col_letter}7:{col_letter}{6 + len(all_notes_value_data)}'
                sheet.update(values=all_notes_value_data, range_name=value_range)
                print(f"    ✅ {col_letter}열 주석 값 업데이트 완료")
            
            print(f"  ✅ XBRL 주석 Archive 배치 업데이트 완료")
            
        except Exception as e:
            print(f"❌ XBRL 주석 Archive 업데이트 실패: {str(e)}")

    def _prepare_notes_data_for_batch_update(self, wb, notes_type):
        """주석 데이터를 배치 업데이트용으로 준비 (개선된 로직)"""
        try:
            print(f"  🔄 주석 배치 업데이트용 데이터 준비 중... ({notes_type})")
            
            # 전체 시트 목록 출력
            print(f"    📋 전체 시트 목록: {wb.sheetnames}")
            
            # 개선된 주석 시트 찾기 로직
            target_sheets = self._find_notes_sheets(wb, notes_type)
            
            print(f"    📄 {notes_type} 주석 시트 {len(target_sheets)}개 발견: {target_sheets}")
            
            # 전체 데이터를 하나의 배열로 통합
            all_notes_account_data = []
            all_notes_value_data = []
            
            # 각 주석 시트의 데이터 추출 및 배치
            for sheet_name in sorted(target_sheets):
                sheet_data = self._extract_notes_sheet_data_improved(wb[sheet_name], sheet_name)
                if sheet_data:
                    # 시트 제목 추가
                    all_notes_account_data.append([f"===== {sheet_data['title']} ====="])
                    all_notes_value_data.append([''])
                    
                    # 각 항목들 배치
                    for item in sheet_data['items']:
                        if item.get('is_category'):
                            display_name = item['name']
                        elif 'display_name' in item:
                            display_name = item['display_name']
                        else:
                            original_name = item.get('original_name', item['name'])
                            indent_level = item.get('indent_level', 0)
                            
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
            total_items = len([row for row in all_notes_account_data if row[0] and not row[0].startswith('=')])
            print(f"    📊 총 주석 항목: {total_items}개")
            
            return all_notes_account_data, all_notes_value_data
            
        except Exception as e:
            print(f"  ❌ 주석 배치 데이터 준비 실패: {str(e)}")
            return [], []

    def _find_notes_sheets(self, wb, notes_type):
        """주석 시트를 찾는 개선된 로직 (D8/U8 규칙 적용)"""
        target_sheets = []
        
        print(f"    🔍 {notes_type} 주석 시트 검색 중...")
        
        for sheet_name in wb.sheetnames:
            if sheet_name in ['Index', '공시기본정보']:
                continue
            
            is_target_sheet = False
            
            # 주석 시트 명명 규칙 체크: D8/U8로 시작하고 연결(0)/별도(5)로 끝남
            if notes_type == 'consolidated':
                # 연결: D8xxx0 또는 U8xxx0
                if (sheet_name.startswith('D8') or sheet_name.startswith('U8')) and sheet_name.endswith('0'):
                    is_target_sheet = True
                    print(f"      ✅ 연결 주석 시트 발견: {sheet_name}")
            else:  # standalone
                # 별도: D8xxx5 또는 U8xxx5
                if (sheet_name.startswith('D8') or sheet_name.startswith('U8')) and sheet_name.endswith('5'):
                    is_target_sheet = True
                    print(f"      ✅ 별도 주석 시트 발견: {sheet_name}")
            
            # 추가: 내용 기반 체크 (위 규칙에 맞지 않지만 주석일 가능성이 있는 시트)
            if not is_target_sheet:
                # 시트명에 '주석'이 명시적으로 포함된 경우
                if '주석' in sheet_name or 'Notes' in sheet_name or 'Note' in sheet_name:
                    worksheet = wb[sheet_name]
                    sheet_title = self._get_sheet_title(worksheet)
                    
                    if notes_type == 'consolidated':
                        if '연결' in sheet_title or ('별도' not in sheet_title and not sheet_name.endswith('5')):
                            is_target_sheet = True
                            print(f"      ✅ 내용 기반 연결 주석 시트: {sheet_name}")
                    else:
                        if '별도' in sheet_title or sheet_name.endswith('5'):
                            is_target_sheet = True
                            print(f"      ✅ 내용 기반 별도 주석 시트: {sheet_name}")
            
            if is_target_sheet:
                target_sheets.append(sheet_name)
        
        return target_sheets

    def _extract_notes_sheet_data_improved(self, worksheet, sheet_name):
        """개별 주석 시트에서 데이터 추출 (긴 텍스트 처리 개선)"""
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
            last_item = None  # 마지막으로 추가한 항목
            
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
                    last_item = None  # 카테고리 변경시 리셋
                    continue
                
                # 긴 텍스트 판별 (50자 이상)
                is_long_text = len(first_text) > 50
                text_pattern = self._analyze_text_pattern(first_text)
                
                # 긴 텍스트이거나 설명문 패턴이고, 바로 이전에 짧은 항목명이 있는 경우
                if (is_long_text or text_pattern == 'description') and last_item and not last_item.get('is_category'):
                    # 이전 항목의 값으로 처리
                    if last_item.get('value'):
                        # 이미 값이 있으면 추가
                        existing_value = str(last_item['value'])
                        last_item['value'] = existing_value + "\n" + first_text
                    else:
                        # 값이 없으면 새로 설정
                        last_item['value'] = first_text
                        last_item['value_type'] = 'text'
                    
                    # formatted_value 업데이트
                    last_item['formatted_value'] = self._format_notes_value(last_item['value'], 'text')
                    continue
                
                # A열이 비어있고 B열(또는 그 이후)에 텍스트가 있는 경우 - 들여쓰기된 항목
                if first_col > 0:
                    # 들여쓰기된 항목으로 처리
                    indent_level = first_col
                    
                    # 긴 텍스트이고 마지막 항목이 있으면 그 항목의 값으로 처리
                    if (is_long_text or text_pattern == 'description') and last_item and not last_item.get('is_category'):
                        if last_item.get('value'):
                            existing_value = str(last_item['value'])
                            last_item['value'] = existing_value + "\n" + ("  " * indent_level) + first_text
                        else:
                            last_item['value'] = ("  " * indent_level) + first_text
                            last_item['value_type'] = 'text'
                        
                        last_item['formatted_value'] = self._format_notes_value(last_item['value'], 'text')
                        continue
                    
                    # 일반적인 들여쓰기 항목 처리
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
                    
                    new_item = {
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
                    }
                    sheet_data['items'].append(new_item)
                    last_item = new_item
                else:
                    # A열에 있는 항목 (들여쓰기 없음)
                    # 하위 분류일 가능성 체크
                    is_subcategory = False
                    
                    # 다음 행들이 들여쓰기되어 있는지 확인
                    if row_idx + 1 < len(all_data) and not is_long_text and text_pattern != 'description':
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
                        new_item = {
                            'name': f"[하위분류] {first_text}",
                            'value': None,
                            'formatted_value': '',
                            'category': current_category,
                            'subcategory': first_text,
                            'is_category': True,
                            'is_subcategory': True,
                            'original_name': first_text
                        }
                        sheet_data['items'].append(new_item)
                        last_item = new_item
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
                        
                        new_item = {
                            'name': unique_name,
                            'original_name': first_text,
                            'value': value,
                            'formatted_value': self._format_notes_value(value, value_type) if value is not None else '',
                            'category': current_category,
                            'subcategory': current_subcategory,
                            'is_category': False,
                            'row_number': row_idx + 1,
                            'value_type': value_type,
                            'indent_level': 0,
                            'text_length': len(first_text)  # 텍스트 길이 저장
                        }
                        sheet_data['items'].append(new_item)
                        last_item = new_item
            
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

    def _analyze_text_pattern(self, text):
        """텍스트 패턴 분석하여 항목명인지 긴 설명인지 판단"""
        # 항목명 패턴
        item_patterns = [
            r'^\d+\.',  # 숫자로 시작 (1. 2. 등)
            r'^\([가-힣]\)',  # (가) (나) 등
            r'^\[[가-힣]\]',  # [가] [나] 등
            r'^[①-⑩]',  # 원 숫자
            r'^[가-힣]{2,10}  # 짧은 한글 단어
        ]
        
        # 설명 패턴
        description_patterns = [
            r'[은는이가을를에서의로와과]',  # 조사가 많이 포함된 경우
            r'[했습니다|합니다|됩니다|있습니다]',  # 문장 종결어
            r'[하였고|하였으며|되었고|되었으며]'  # 연결어
        ]
        
        # 항목명 패턴 체크
        for pattern in item_patterns:
            if re.match(pattern, text):
                return 'item'
        
        # 설명 패턴 체크
        description_score = 0
        for pattern in description_patterns:
            if re.search(pattern, text):
                description_score += 1
        
        # 길이와 설명 점수로 판단
        if len(text) > 50 and description_score >= 2:
            return 'description'
        elif len(text) > 100:
            return 'description'
        
        return 'item'

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
                                               '포괄손익' in value or '주석' in value):
                            return value
            return ""
        except:
            return ""

    def _get_column_letter(self, col_index):
        """컬럼 인덱스를 문자로 변환 (0-based)"""
        result = ""
        num = col_index + 1  # 1-based로 변환
        while num > 0:
            num, remainder = divmod(num - 1, 26)
            result = chr(65 + remainder) + result
        return result

    def _send_telegram_message(self, message):
        """텔레그램 메시지 전송"""
        try:
            if self.telegram_bot_token and self.telegram_channel_id:
                import requests
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

    def _cleanup_downloads(self):
        """다운로드 폴더 정리"""
        try:
            # Archive 업데이트가 완료된 후에만 정리
            if os.path.exists(self.download_dir) and self.results.get('xbrl', {}).get('excel_files'):
                # Excel 파일들만 남기고 다른 파일들 정리
                for file in os.listdir(self.download_dir):
                    file_path = os.path.join(self.download_dir, file)
                    if file_path not in self.results['xbrl']['downloaded_files']:
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
        print(f"XBRL 다운로드 성공: {len(self.results['xbrl']['downloaded_files'])}개")
        print(f"XBRL 업로드된 시트: {len(self.results['xbrl']['uploaded_sheets'])}개")
        print(f"XBRL 다운로드 실패: {len(self.results['xbrl']['failed_downloads'])}개")
        print(f"XBRL 업로드 실패: {len(self.results['xbrl']['failed_uploads'])}개")
        print(f"HTML 처리된 시트: {len(self.results['html']['processed_sheets'])}개")
        print(f"HTML 실패: {len(self.results['html']['failed_sheets'])}개")
        
        # 텔레그램 메시지 전송
        if self.telegram_bot_token and self.telegram_channel_id:
            self._send_telegram_summary()

    def _send_telegram_summary(self):
        """텔레그램 요약 메시지 전송"""
        try:
            import requests
            
            message = (
                f"📊 DART 통합 업데이트 완료\n\n"
                f"• 종목: {self.company_name} ({self.corp_code})\n"
                f"• 처리 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"• 전체 보고서: {self.results['total_reports']}개\n"
                f"• XBRL 다운로드: {len(self.results['xbrl']['downloaded_files'])}개\n"
                f"• XBRL 업로드: {len(self.results['xbrl']['uploaded_sheets'])}개\n"
                f"• HTML 처리: {len(self.results['html']['processed_sheets'])}개"
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
        
        print(f"🤖 DART 통합 업데이터 시스템")
        print(f"🏢 대상 기업: {company_config['company_name']} ({company_config['corp_code']})")
        
        # 통합 업데이터 실행
        updater = DartDualUpdater(company_config)
        updater.run()
        
        print("\n✅ 모든 작업이 완료되었습니다!")
        
    except Exception as e:
        print(f"\n❌ 오류 발생: {str(e)}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()
