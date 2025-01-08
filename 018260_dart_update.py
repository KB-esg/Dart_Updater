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

class SamsungSDSUpdater:
    CORP_CODE = '018260'  # 삼성에스디에스 종목코드
    TARGET_SHEETS = [
        'I. 회사의 개요', 'II. 사업의 내용', '1. 요약재무정보',
        '2. 연결재무제표', '3. 연결재무제표 주석', '4. 재무제표',
        '5. 재무제표 주석', '6. 배당에 관한 사항', '8. 기타 재무에 관한 사항',
        'VII. 주주에 관한 사항', 'VIII. 임원 및 직원 등에 관한 사항',
        'X. 대주주 등과의 거래내용', 'XI. 그 밖에 투자자 보호를 위하여 필요한 사항'
    ]

    def __init__(self):
        # 환경변수 확인을 위한 디버깅 코드
        print("환경변수 확인:")
        print("DART_API_KEY 존재:", 'DART_API_KEY' in os.environ)
        print("GOOGLE_CREDENTIALS 존재:", 'GOOGLE_CREDENTIALS' in os.environ)
        print("SDS_SPREADSHEET_ID 존재:", 'SDS_SPREADSHEET_ID' in os.environ)
        
        if 'SDS_SPREADSHEET_ID' not in os.environ:
            raise ValueError("SDS_SPREADSHEET_ID 환경변수가 설정되지 않았습니다.")
            
        self.credentials = self.get_credentials()
        self.gc = gspread.authorize(self.credentials)
        self.dart = OpenDartReader(os.environ['DART_API_KEY'])
        self.workbook = self.gc.open_by_key(os.environ['SDS_SPREADSHEET_ID'])
        
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
        report_list = self.dart.list(self.CORP_CODE, start_date, end_date, kind='A', final='T')
        
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
        
        # 더 작은 배치 크기와 더 긴 지연 시간 설정
        BATCH_SIZE = 50  # 배치 크기 축소
        MAX_RETRIES = 5  # 최대 재시도 횟수
        
        for i in range(0, len(all_data), BATCH_SIZE):
            batch = all_data[i:i + BATCH_SIZE]
            retry_count = 0
            success = False
            
            while not success and retry_count < MAX_RETRIES:
                try:
                    worksheet.append_rows(batch)
                    time.sleep(3)  # 배치 사이 지연 시간 증가
                    success = True
                    print(f"배치 업데이트 성공: {i+1}~{min(i+BATCH_SIZE, len(all_data))} 행")
                except gspread.exceptions.APIError as e:
                    if 'Quota exceeded' in str(e):
                        retry_count += 1
                        wait_time = 60 * (retry_count + 1)  # 재시도마다 대기 시간 증가
                        print(f"할당량 제한 도달. {wait_time}초 대기 후 {retry_count}번째 재시도...")
                        time.sleep(wait_time)
                    else:
                        raise e
            
            if not success:
                print(f"최대 재시도 횟수 초과. 배치 {i//BATCH_SIZE + 1} 처리 실패")
                raise Exception("API 할당량 문제로 인한 업데이트 실패")

    def process_dart_archive(self):
        """Dart_Archive 시트 처리"""
        archive = self.workbook.worksheet('Dart_Archive')
        last_col = len(archive.get_all_values()[0])
        control_value = archive.cell(1, last_col).value
        
        # 시작 행 결정
        if not control_value:
            data = archive.col_values(last_col)
            last_row_with_data = len(data) - next(i for i, x in enumerate(reversed(data)) if x) - 1
            start_row = max(last_row_with_data + 1, 7)
        else:
            last_col += 1
            start_row = 947

        self.update_archive_data(archive, start_row, last_col)

    def update_archive_data(self, archive, start_row, last_col):
        """아카이브 데이터 업데이트"""
        all_rows = archive.get_all_values()
        for row in range(start_row, len(all_rows) + 1):
            cell_values = archive.row_values(row)
            if len(cell_values) < 5:
                continue

            sheet_name = cell_values[0]
            keyword = cell_values[1]
            keyword_n = cell_values[2]
            keyword_x = cell_values[3]
            keyword_y = cell_values[4]

            if not all([keyword, keyword_n, keyword_x, keyword_y, sheet_name]):
                continue

            self.process_archive_row(archive, row, last_col, keyword, int(keyword_n), 
                                  int(keyword_x), int(keyword_y), sheet_name)

        # 작업 완료 후 상태 업데이트
        self.update_archive_status(archive, last_col)

    def process_archive_row(self, archive, row, col, keyword, n, x, y, sheet_name):
        """아카이브 행 처리"""
        try:
            search_sheet = self.workbook.worksheet(sheet_name)
            cell_list = search_sheet.findall(keyword)
            
            if cell_list and len(cell_list) >= n:
                target_cell = cell_list[n - 1]
                value = search_sheet.cell(target_cell.row + y, target_cell.col + x).value
                cleaned_value = self.remove_parentheses(value)
                archive.update_cell(row, col, cleaned_value)
        except Exception as e:
            print(f"행 {row} 처리 중 오류 발생: {str(e)}")

    def remove_parentheses(self, value):
        """괄호 내용 및 % 기호 제거"""
        if not value:
            return value
        return re.sub(r'\s*\(.*?\)\s*', '', value).replace('%', '')

    def update_archive_status(self, archive, last_col):
        """아카이브 상태 업데이트"""
        today = datetime.now().strftime('%Y-%m-%d')
        archive.update_cell(1, last_col, '1')
        archive.update_cell(1, 10, today)
        archive.update_cell(5, last_col, today)

def main():
    print("삼성에스디에스(018260) 보고서 업데이트 시작")
    updater = SamsungSDSUpdater()
    updater.update_dart_reports()
    updater.process_dart_archive()
    print("업데이트 완료")

if __name__ == "__main__":
    main()
