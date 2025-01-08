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
        'I. 회사의 개요', 'II. 사업의 내용', '1. 사업의 개요','2. 주요 제품 및 서비스','3. 원재료 및 생산설비', '4. 매출 및 수주상황','5. 위험관리 및 파생거래','6. 주요계약 및 연구활동','7. 기타 참고 사항',
        '1. 요약재무정보','2. 연결재무제표', '3. 연결재무제표 주석', '4. 재무제표','5. 재무제표 주석', '6. 배당에 관한 사항', '8. 기타 재무에 관한 사항',
        'VII. 주주에 관한 사항', 'VIII. 임원 및 직원 등에 관한 사항',
        'X. 대주주 등과의 거래내용', 'XI. 그 밖에 투자자 보호를 위하여 필요한 사항'
    ]


    def __init__(self, corp_code, spreadsheet_var_name):
        """
        초기화
        :param corp_code: 종목 코드 (예: '018260')
        :param spreadsheet_var_name: 스프레드시트 환경변수 이름 (예: 'SDS_SPREADSHEET_ID')
        """
        self.corp_code = corp_code
        self.spreadsheet_var_name = spreadsheet_var_name
        
        # 환경변수 확인
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

    def process_archive_data(self, archive, start_row, last_col):
        """Dart_Archive 데이터를 배치로 처리"""
        print(f"시작 행: {start_row}, 대상 열: {last_col}")
        all_rows = archive.get_all_values()
        batch_updates = []
        batch_size = 100
        
        # 처리할 데이터 수집
        for row_idx in range(start_row - 1, len(all_rows)):  # 0-based index로 변환
            if len(all_rows[row_idx]) < 5:
                continue

            sheet_name = all_rows[row_idx][0]
            keyword = all_rows[row_idx][1]
            keyword_n = all_rows[row_idx][2]
            keyword_x = all_rows[row_idx][3]
            keyword_y = all_rows[row_idx][4]

            print(f"처리중: 시트={sheet_name}, 키워드={keyword}, n={keyword_n}, x={keyword_x}, y={keyword_y}")

            if not all([keyword, keyword_n, keyword_x, keyword_y, sheet_name]):
                continue

            try:
                # 한 번에 시트의 모든 데이터를 가져옴
                search_sheet = self.workbook.worksheet(sheet_name)
                sheet_data = search_sheet.get_all_values()
                
                # 키워드 위치 찾기
                keyword_positions = []
                for i, row_data in enumerate(sheet_data):
                    for j, cell in enumerate(row_data):
                        if cell == keyword:
                            keyword_positions.append((i, j))
                
                if keyword_positions and len(keyword_positions) >= int(keyword_n):
                    target_pos = keyword_positions[int(keyword_n) - 1]
                    target_row = target_pos[0] + int(keyword_y)
                    target_col = target_pos[1] + int(keyword_x)
                    
                    if target_row < len(sheet_data) and target_col < len(sheet_data[target_row]):
                        value = sheet_data[target_row][target_col]
                        cleaned_value = self.remove_parentheses(value)
                        print(f"찾은 값: {cleaned_value}")
                        batch_updates.append({
                            'range': f'R{row_idx + 1}C{last_col}',
                            'values': [[cleaned_value]]
                        })
            except Exception as e:
                print(f"Error processing row {row_idx + 1}: {str(e)}")
            
            # 배치 크기에 도달하면 업데이트 실행
            if len(batch_updates) >= batch_size:
                try:
                    archive.batch_update(batch_updates)
                    print(f"배치 업데이트 완료: {len(batch_updates)} 행")
                    batch_updates = []
                    time.sleep(3)  # API 제한 고려
                except gspread.exceptions.APIError as e:
                    if 'Quota exceeded' in str(e):
                        wait_time = 60
                        print(f"할당량 제한 도달. {wait_time}초 대기 후 재시도...")
                        time.sleep(wait_time)
                        archive.batch_update(batch_updates)
                        batch_updates = []
                    else:
                        raise e
        
        # 남은 데이터 처리
        if batch_updates:
            try:
                archive.batch_update(batch_updates)
                print(f"최종 배치 업데이트 완료: {len(batch_updates)} 행")
                # 작업 완료 후 상태 업데이트
                today = datetime.now().strftime('%Y-%m-%d')
                archive.update_cell(1, last_col, '1')
                archive.update_cell(1, 10, today)
                archive.update_cell(5, last_col, today)
            except gspread.exceptions.APIError as e:
                if 'Quota exceeded' in str(e):
                    time.sleep(60)
                    archive.batch_update(batch_updates)
                    # 작업 완료 후 상태 업데이트
                    today = datetime.now().strftime('%Y-%m-%d')
                    archive.update_cell(1, last_col, '1')
                    archive.update_cell(1, 10, today)
                    archive.update_cell(5, last_col, today)
                else:
                    raise e

    def remove_parentheses(self, value):
        """괄호 내용 및 % 기호 제거"""
        if not value:
            return value
        return re.sub(r'\s*\(.*?\)\s*', '', value).replace('%', '')


def main():
    try:
        # 종목 정보 설정
        COMPANY_INFO = {
            'code': '018260',
            'name': '삼성에스디에스',
            'spreadsheet_var': 'SDS_SPREADSHEET_ID'
        }
        
        print(f"{COMPANY_INFO['name']}({COMPANY_INFO['code']}) 보고서 업데이트 시작")
        updater = DartReportUpdater(COMPANY_INFO['code'], COMPANY_INFO['spreadsheet_var'])
        
        # DART 보고서 시트들 업데이트
        updater.update_dart_reports()
        print("보고서 시트 업데이트 완료")
        
        # Dart_Archive 시트 업데이트
        print("Dart_Archive 시트 업데이트 시작")
        try:
            archive = updater.workbook.worksheet('Dart_Archive')
        except Exception as e:
            print(f"Dart_Archive 시트를 찾을 수 없음: {str(e)}")
            raise
            
        try:
            last_col = len(archive.get_all_values()[0])
            print(f"현재 마지막 열: {last_col}")
        except Exception as e:
            print(f"열 정보 가져오기 실패: {str(e)}")
            raise
            
        try:
            control_value = archive.cell(1, last_col).value
            print(f"Control value: {control_value}")
        except Exception as e:
            print(f"Control value 가져오기 실패: {str(e)}")
            raise
        
        # 시작 행 결정
        try:
            if not control_value:
                data = archive.col_values(last_col)
                last_row_with_data = len(data) - next(i for i, x in enumerate(reversed(data)) if x) - 1
                start_row = max(last_row_with_data + 1, 7)
            else:
                last_col += 1
                start_row = 947
            print(f"처리 시작 행: {start_row}, 대상 열: {last_col}")
        except Exception as e:
            print(f"시작 행 결정 중 오류 발생: {str(e)}")
            raise
        
        updater.process_archive_data(archive, start_row, last_col)
        print("Dart_Archive 시트 업데이트 완료")
        
    except Exception as e:
        print(f"작업 중 오류 발생: {str(e)}")
        print(f"오류 상세 정보: {type(e).__name__}")
        import traceback
        print(traceback.format_exc())
        raise

if __name__ == "__main__":
    main()
