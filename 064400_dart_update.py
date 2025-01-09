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

   def __init__(self, corp_code, spreadsheet_var_name, corp_name):
       """
       초기화
       :param corp_code: 종목 코드 (예: '018260')
       :param spreadsheet_var_name: 스프레드시트 환경변수 이름 (예: 'SDS_SPREADSHEET_ID')
       """
       self.corp_code = corp_code
       self.corp_name = corp_name
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
       
       try:
           # 먼저 상장코드로 시도
           report_list = self.dart.list(self.corp_code, start_date, end_date, kind='A', final='T')
       except ValueError as e:
           print(f"상장코드 {self.corp_code}로 검색 실패, 회사명으로 재시도합니다.")
           try:
               # 회사명으로 검색 시도
               corp_info = self.dart.corp_codes
               corp_info = corp_info[corp_info['corp_name'].str.contains(self.corp_name, case=False)]
               
               if len(corp_info) == 0:
                   raise ValueError(f"회사명 '{self.corp_name}'을 찾을 수 없습니다.")
               
               # 첫 번째 일치하는 회사의 고유번호 사용
               corp_code = corp_info.iloc[0]['corp_code']
               print(f"회사명으로 검색된 고유번호: {corp_code}")
               
               report_list = self.dart.list(corp_code, start_date, end_date, kind='A', final='T')
           except Exception as e2:
               print(f"회사명 검색 중 오류 발생: {str(e2)}")
               raise
       
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
       
       # 시트별로 데이터 처리
       for sheet_name in self.TARGET_SHEETS:
           try:
               if sheet_name not in sheet_cache:
                   try:
                       worksheet = self.workbook.worksheet(sheet_name)
                       sheet_cache[sheet_name] = worksheet.get_all_values()
                   except gspread.exceptions.WorksheetNotFound:
                       continue
               
               sheet_data = sheet_cache[sheet_name]
               if not sheet_data:
                   continue
               
               # 검색어 열에서 키워드 가져오기
               for row_idx in range(start_row - 1, len(all_rows)):
                   search_key = all_rows[row_idx][1].strip()  # 2열(인덱스 1)의 검색어
                   if not search_key or search_key == '-' or search_key == '[-]':
                       continue
                   
                   found = False
                   value = '-'
                   
                   # 키워드로 데이터 검색
                   for sheet_row in sheet_data:
                       try:
                           if any(search_key in cell for cell in sheet_row):
                               # 숫자 데이터 찾기
                               for cell in sheet_row:
                                   cleaned_cell = self.remove_parentheses(cell)
                                   if cleaned_cell and cleaned_cell.replace(',', '').replace('.', '').replace('-', '').isdigit():
                                       value = cleaned_cell
                                       found = True
                                       break
                           if found:
                               break
                       except Exception as e:
                           print(f"행 처리 중 오류 발생: {str(e)}")
                           continue
                   
                   # 결과 저장
                   update_data.append((row_idx + 1, value))
                   
           except Exception as e:
               print(f"시트 {sheet_name} 처리 중 오류 발생: {str(e)}")
               continue
   
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
            'code': '064400',
            'name': '엘지씨엔에스',
            'spreadsheet_var': 'LGCNS_SPREADSHEET_ID'
        }
        
        log(f"{COMPANY_INFO['name']}({COMPANY_INFO['code']}) 보고서 업데이트 시작")
        updater = DartReportUpdater(COMPANY_INFO['code'], COMPANY_INFO['spreadsheet_var'], COMPANY_INFO['name'])
        
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
                # 2열의 유효 데이터 범위 확인
                search_terms_col = archive.col_values(2)
                valid_rows = [i for i, x in enumerate(search_terms_col) if x.strip() and x.strip() != '-' and x.strip() != '[-]']
                valid_data_end = max(valid_rows) if valid_rows else 7
                
                # 현재 열 데이터 확인
                current_col_data = archive.col_values(last_col)
                
                # 현재 열의 유효 데이터 범위까지 데이터가 채워져 있는지 확인
                current_col_filled = all(x.strip() for x in current_col_data[7:valid_data_end+1])
                
                if current_col_filled:
                    # 현재 열이 모두 채워져 있으면 다음 열로 이동
                    last_col += 1
                    start_row = 7
                    log("현재 열 작업 완료, 다음 열 7행부터 시작")
                else:
                    # 현재 열에서 이어서 작업
                    current_col_valid = [i for i, x in enumerate(current_col_data) if x.strip() and x.strip() != '-' and x.strip() != '[-]']
                    start_row = max(max(current_col_valid) + 1, 7) if current_col_valid else 7
                    log("현재 열에서 이어서 작업")
            else:
                last_col += 1
                start_row = 7
            
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
