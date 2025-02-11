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

    def __init__(self, corp_code, spreadsheet_var_name, company_name):
        """
        초기화
        :param corp_code: 종목 코드 (예: '018260')
        :param spreadsheet_var_name: 스프레드시트 환경변수 이름 (예: 'SDS_SPREADSHEET_ID')
        :param company_name: 회사명 (예: '삼성에스디에스')
        """
        self.corp_code = corp_code
        self.company_name = company_name
        self.spreadsheet_var_name = spreadsheet_var_name
        
        print("환경변수 확인:")
        print("DART_API_KEY 존재:", 'DART_API_KEY' in os.environ)
        print("GOOGLE_CREDENTIALS 존재:", 'GOOGLE_CREDENTIALS' in os.environ)
        print(f"{spreadsheet_var_name} 존재:", spreadsheet_var_name in os.environ)
        print("TELEGRAM_BOT_TOKEN 존재:", 'TELEGRAM_BOT_TOKEN' in os.environ)
        print("TELEGRAM_CHANNEL_ID 존재:", 'TELEGRAM_CHANNEL_ID' in os.environ)
        
        if spreadsheet_var_name not in os.environ:
            raise ValueError(f"{spreadsheet_var_name} 환경변수가 설정되지 않았습니다.")
            
        self.credentials = self.get_credentials()
        self.gc = gspread.authorize(self.credentials)
        self.dart = OpenDartReader(os.environ['DART_API_KEY'])
        self.workbook = self.gc.open_by_key(os.environ[spreadsheet_var_name])
        self.telegram_bot_token = os.environ.get('TELEGRAM_BOT_TOKEN')
        self.telegram_channel_id = os.environ.get('TELEGRAM_CHANNEL_ID')

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
        start_date = end_date - timedelta(days=1100)
        return start_date.strftime('%Y%m%d'), end_date.strftime('%Y%m%d')

    def get_column_letter(self, col_num):
        """숫자를 엑셀 열 문자로 변환 (예: 1 -> A, 27 -> AA)"""
        result = ""
        while col_num > 0:
            col_num, remainder = divmod(col_num - 1, 26)
            result = chr(65 + remainder) + result
        return result

    def send_telegram_message(self, message):
        """텔레그램으로 메시지 전송"""
        if not self.telegram_bot_token or not self.telegram_channel_id:
            print("텔레그램 설정이 없습니다.")
            return
        
        try:
            url = f"https://api.telegram.org/bot{self.telegram_bot_token}/sendMessage"
            data = {
                "chat_id": self.telegram_channel_id,
                "text": message,
                "parse_mode": "HTML"
            }
            response = requests.post(url, data=data)
            response.raise_for_status()
            print("텔레그램 메시지 전송 완료")
        except Exception as e:
            print(f"텔레그램 메시지 전송 실패: {str(e)}")

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

    def remove_parentheses(self, value):
        """괄호 내용 제거"""
        if not value:
            return value
        return re.sub(r'\s*\(.*?\)\s*', '', value).replace('%', '')

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
                
        BATCH_SIZE = 100
        for i in range(0, len(all_data), BATCH_SIZE):
            batch = all_data[i:i + BATCH_SIZE]
            try:
                worksheet.append_rows(batch)
                print(f"배치 업데이트 완료: {i+1}~{min(i+BATCH_SIZE, len(all_data))} 행")
            except gspread.exceptions.APIError as e:
                if 'Quota exceeded' in str(e):
                    print("할당량 제한 도달. 60초 대기 후 재시도...")
                    time.sleep(60)
                    worksheet.append_rows(batch)
                else:
                    raise e


    def process_archive_data(self, archive, start_row, last_col):
        """아카이브 데이터 처리"""
        try:
            # 현재 시트의 크기 확인
            current_cols = archive.col_count
            current_col_letter = self.get_column_letter(current_cols)
            target_col_letter = self.get_column_letter(last_col)
            
            print(f"시작 행: {start_row}, 대상 열: {last_col} ({target_col_letter})")
            print(f"현재 시트 열 수: {current_cols} ({current_col_letter})")
            
            # 필요한 경우 시트 크기 조정
            if last_col >= current_cols:
                new_cols = last_col + 5  # 여유 있게 5열 추가
                try:
                    print(f"시트 크기를 {current_cols}({current_col_letter})에서 {new_cols}({self.get_column_letter(new_cols)})로 조정합니다.")
                    archive.resize(rows=archive.row_count, cols=new_cols)
                    time.sleep(2)  # API 호출 후 대기
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
                                    cleaned_value = self.remove_parentheses(str(value))
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
                        f"🔄 DART 업데이트 완료\n\n"
                        f"• 종목: {self.company_name} ({self.corp_code})\n"
                        f"• 분기: {quarter_text}\n"
                        f"• 업데이트 일시: {today.strftime('%Y-%m-%d %H:%M:%S')}\n"
                        f"• 처리된 행: {len(update_data)}개\n"
                        f"• 시트 열: {target_col_letter} (#{last_col})"
                    )
                    self.send_telegram_message(message)
                    
                except Exception as e:
                    error_msg = f"업데이트 중 오류 발생: {str(e)}"
                    print(error_msg)
                    self.send_telegram_message(f"❌ {error_msg}")
                    raise e
                    
        except Exception as e:
            error_msg = f"아카이브 처리 중 오류 발생: {str(e)}"
            print(error_msg)
            self.send_telegram_message(f"❌ {error_msg}")
            raise e



def main():
    try:
        import sys
        
        def log(msg):
            print(msg)
            sys.stdout.flush()  # 즉시 출력 보장
        
        COMPANY_INFO = {
            'code': '00139834',
            'name': '엘지씨엔에스',
            'spreadsheet_var': 'LGCNS_SPREADSHEET_ID'
        }
        
        log(f"{COMPANY_INFO['name']}({COMPANY_INFO['code']}) 보고서 업데이트 시작")
        
        try:
            updater = DartReportUpdater(
                COMPANY_INFO['code'], 
                COMPANY_INFO['spreadsheet_var'],
                COMPANY_INFO['name']
            )
            
            updater.update_dart_reports()
            log("보고서 시트 업데이트 완료")
            
            log("Dart_Archive 시트 업데이트 시작")
            archive = updater.workbook.worksheet('Dart_Archive')
            log("Archive 시트 접근 성공")
            
            sheet_values = archive.get_all_values()
            if not sheet_values:
                raise ValueError("Dart_Archive 시트가 비어있습니다")
            
            last_col = len(sheet_values[0])
            log(f"현재 마지막 열: {last_col}, 전체 행 수: {len(sheet_values)}")
            
            control_value = archive.cell(1, last_col).value
            log(f"Control value: {control_value}")
            
            # 시작 행은 항상 10으로 설정
            start_row = 10
            
            # control_value에 따라 열만 조정
            if control_value:
                last_col += 1
            
            log(f"처리 시작 행: {start_row}, 대상 열: {last_col}")
            updater.process_archive_data(archive, start_row, last_col)
            log("Dart_Archive 시트 업데이트 완료")
            
        except Exception as e:
            log(f"Dart_Archive 시트 처리 중 오류 발생: {str(e)}")
            if 'updater' in locals():
                updater.send_telegram_message(
                    f"❌ DART 업데이트 실패\n\n"
                    f"• 종목: {COMPANY_INFO['name']} ({COMPANY_INFO['code']})\n"
                    f"• 오류: {str(e)}"
                )
            raise

    except Exception as e:
        log(f"전체 작업 중 오류 발생: {str(e)}")
        log(f"오류 상세 정보: {type(e).__name__}")
        if 'updater' in locals():
            updater.send_telegram_message(
                f"❌ DART 업데이트 중 치명적 오류 발생\n\n"
                f"• 종목: {COMPANY_INFO['name']} ({COMPANY_INFO['code']})\n"
                f"• 오류: {str(e)}"
            )
        raise e

if __name__ == "__main__":
    main()
