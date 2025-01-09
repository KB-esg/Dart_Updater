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

class DartReportUpdater:
    TARGET_SHEETS = [
        'I. 회사의 개요', 'II. 사업의 내용', '1. 사업의 개요', '2. 주요 제품 및 서비스',
        '3. 원재료 및 생산설비', '4. 매출 및 수주상황', '5. 위험관리 및 파생거래',
        '6. 주요계약 및 연구활동', '7. 기타 참고 사항', '1. 요약재무정보',
        '2. 연결재무제표', '3. 연결재무제표 주석', '4. 재무제표', '5. 재무제표 주석',
        '6. 배당에 관한 사항', '8. 기타 재무에 관한 사항', 'VII. 주주에 관한 사항',
        'VIII. 임원 및 직원 등에 관한 사항', 'X. 대주주 등과의 거래내용',
        'XI. 그 밖에 투자자 보호를 위하여 필요한 사항'
    ]

    def __init__(self, corp_code, spreadsheet_var_name):
        """
        초기화
        :param corp_code: 종목 코드 (예: '018260')
        :param spreadsheet_var_name: 스프레드시트 환경변수 이름 (예: 'SDS_SPREADSHEET_ID')
        """
        self.corp_code = corp_code
        self.spreadsheet_var_name = spreadsheet_var_name
        
        print("환경변수 확인:")
        print("DART_API_KEY 존재:", 'DART_API_KEY' in os.environ)
        print("GOOGLE_CREDENTIALS 존재:", 'GOOGLE_CREDENTIALS' in os.environ)
        print(f"{spreadsheet_var_name} 존재:", spreadsheet_var_name in os.environ)
        
        if spreadsheet_var_name not in os.environ:
            raise ValueError(f"{spreadsheet_var_name} 환경변수가 설정되지 않았습니다.")
            
        self.credentials = self.get_credentials()
        self.gc = gspread.authorize(self.credentials)
        self.dart = OpenDartReader(os.environ['DART_API_KEY'])
        self.workbook = self.gc.open_by_key(os.environ[spreadsheet_var_name])

    def get_credentials(self):
        """Google Sheets 인증 설정"""
        creds_json = json.loads(os.environ['GOOGLE_CREDENTIALS'])
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        return Credentials.from_service_account_info(creds_json, scopes=scopes)

    def get_recent_dates(self):
        """최근 3개월 날짜 범위 계산"""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=90)
        return start_date.strftime('%Y%m%d'), end_date.strftime('%Y%m%d')

    def update_dart_reports(self):
        """DART 보고서 데이터 업데이트"""
        start_date, end_date = self.get_recent_dates()
        report_list = self.dart.list(self.corp_code, start_date, end_date, kind='A', final='T')
        
        if not report_list.empty:
            for _, report in report_list.iterrows():
                self.process_report(report['rcept_no'])
                print(f"보고서 처리 완료: {report['report_nm']}")

    def process_report(self, rcept_no):
        """개별 보고서 처리"""
        report_index = self.dart.sub_docs(rcept_no)
        target_docs = report_index[report_index['title'].isin(self.TARGET_SHEETS)]
        
        for _, doc in target_docs.iterrows():
            self.update_worksheet(doc['title'], doc['url'])

    def update_worksheet(self, sheet_name, url):
        """워크시트 업데이트"""
        try:
            worksheet = self.workbook.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = self.workbook.add_worksheet(sheet_name, 1000, 10)
            
        response = requests.get(url)
        if response.status_code == 200:
            self.process_html_content(worksheet, response.text)
            print(f"시트 업데이트 완료: {sheet_name}")

    def process_html_content(self, worksheet, html_content):
        """HTML 내용 처리 및 워크시트 업데이트"""
        soup = BeautifulSoup(html_content, 'html.parser')
        tables = soup.find_all("table")
        
        worksheet.clear()
        all_data = []
        
        for table in tables:
            table_data = parser.make2d(table)
            if table_data:
                all_data.extend(table_data)
                
        BATCH_SIZE = 50
        MAX_RETRIES = 5
        
        for i in range(0, len(all_data), BATCH_SIZE):
            batch = all_data[i:i + BATCH_SIZE]
            retry_count = 0
            success = False
            
            while not success and retry_count < MAX_RETRIES:
                try:
                    worksheet.append_rows(batch)
                    time.sleep(3)
                    success = True
                    print(f"배치 업데이트 성공: {i+1}~{min(i+BATCH_SIZE, len(all_data))} 행")
                except gspread.exceptions.APIError as e:
                    if 'Quota exceeded' in str(e):
                        retry_count += 1
                        wait_time = 60 * (retry_count + 1)
                        print(f"할당량 제한 도달. {wait_time}초 대기 후 {retry_count}번째 재시도...")
                        time.sleep(wait_time)
                    else:
                        raise e
            
            if not success:
                print(f"최대 재시도 횟수 초과. 배치 {i//BATCH_SIZE + 1} 처리 실패")
                raise Exception("API 할당량 문제로 인한 업데이트 실패")

    def remove_parentheses(self, value):
        """괄호 내용 제거"""
        if not value:
            return value
        return re.sub(r'\s*\(.*?\)\s*', '', value).replace('%', '')

    def process_archive_data(self, archive, start_row, last_col):
        """아카이브 데이터 처리"""
        print(f"시작 행: {start_row}, 대상 열: {last_col}")
        all_rows = archive.get_all_values()
        update_data = []
        sheet_cache = {}
        
        # ... (기존 코드는 동일) ...
    
        if update_data:
            try:
                batch_size = 50
                for i in range(0, len(update_data), batch_size):
                    batch = update_data[i:i + batch_size]
                    for row, value in batch:
                        try:
                            archive.update_cell(row, last_col, value)
                            time.sleep(1)
                        except gspread.exceptions.APIError as e:
                            if 'Quota exceeded' in str(e):
                                print(f"할당량 제한 도달. 60초 대기 후 재시도... (행: {row})")
                                time.sleep(60)
                                archive.update_cell(row, last_col, value)
                            else:
                                raise e
                    print(f"배치 업데이트 완료: {i+1}~{min(i+batch_size, len(update_data))} 행")
                
                # 이전 분기 정보 계산
                today = datetime.now()
                # 3개월 전 날짜 계산
                three_months_ago = today - timedelta(days=90)
                year = str(three_months_ago.year)[2:]  # 년도의 마지막 2자리
                quarter = (three_months_ago.month - 1) // 3 + 1  # 분기 계산
                quarter_text = f"{quarter}Q{year}"
                
                # 분기 정보 업데이트 (6번째 행)
                archive.update_cell(6, last_col, quarter_text)
                
                # 기존 날짜 업데이트
                archive.update_cell(1, 10, today.strftime('%Y-%m-%d'))
                archive.update_cell(1, last_col, '1')
                archive.update_cell(5, last_col, today.strftime('%Y-%m-%d'))
                
                print(f"전체 업데이트 완료 (이전 분기: {quarter_text})")
                
            except Exception as e:
                print(f"최종 업데이트 중 오류 발생: {str(e)}")
                raise e

def main():
    try:
        import sys
        
        def log(msg):
            print(msg)
            sys.stdout.flush()
        
        COMPANY_INFO = {
            'code': '018260',
            'name': '삼성SDS',
            'spreadsheet_var': 'SDS_SPREADSHEET_ID'
        }
        
        log(f"{COMPANY_INFO['name']}({COMPANY_INFO['code']}) 보고서 업데이트 시작")
        updater = DartReportUpdater(COMPANY_INFO['code'], COMPANY_INFO['spreadsheet_var'])
        
        updater.update_dart_reports()
        log("보고서 시트 업데이트 완료")
        
        log("Dart_Archive 시트 업데이트 시작")
        try:
            archive = updater.workbook.worksheet('Dart_Archive')
            log("Archive 시트 접근 성공")
            
            sheet_values = archive.get_all_values()
            if not sheet_values:
                raise ValueError("Dart_Archive 시트가 비어있습니다")
            
            last_col = len(sheet_values[0])
            current_cols = archive.col_count  # 현재 열 수 확인
            log(f"현재 마지막 열: {last_col}, 전체 행 수: {len(sheet_values)}, 시트 열 수: {current_cols}")
            
            control_value = archive.cell(1, last_col).value
            log(f"Control value: {control_value}")
            
            # 다음 열이 필요한 경우 시트 크기 조정
            if control_value or (last_col >= current_cols - 1):
                new_cols = current_cols + 10  # 한 번에 10개의 열을 추가
                try:
                    archive.resize(rows=archive.row_count, cols=new_cols)
                    log(f"시트 크기를 {new_cols}열로 조정했습니다.")
                    current_cols = new_cols
                except Exception as e:
                    log(f"시트 크기 조정 중 오류 발생: {str(e)}")
                    raise
            
            if not control_value:
                data = archive.col_values(last_col)
                # 실제 데이터가 있는 행만 찾기
                non_empty_rows = [i for i, x in enumerate(data) if x.strip()]
                if non_empty_rows:
                    start_row = max(max(non_empty_rows) + 1, 10)
                else:
                    start_row = 10
            else:
                last_col += 1
                start_row = 10
            
            log(f"처리 시작 행: {start_row}, 대상 열: {last_col}")
            updater.process_archive_data(archive, start_row, last_col)
            log("Dart_Archive 시트 업데이트 완료")
            
        except Exception as e:
            log(f"Dart_Archive 시트 처리 중 오류 발생: {str(e)}")
            import traceback
            log(traceback.format_exc())
            raise

    except Exception as e:
        log(f"전체 작업 중 오류 발생: {str(e)}")
        log(f"오류 상세 정보: {type(e).__name__}")
        import traceback
        log(traceback.format_exc())
        raise

if __name__ == "__main__":
    main()
