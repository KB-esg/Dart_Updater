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
        self.telegram_channel_id = os.environ.get('TELEGRAM_CHANNEL_ID')    try:
        COMPANY_INFO = {
            'code': '037560',
            'name': 'LG헬로비전',
            'spreadsheet_var': 'HELLO_SPREADSHEET_ID'
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
            
            if not control_value:
                data = archive.col_values(last_col)
                try:
                    last_row_with_data = len(data) - next(i for i, x in enumerate(reversed(data)) if x) - 1
                    start_row = max(last_row_with_data + 1, 10)
                except StopIteration:
                    start_row = 10
            else:
                last_col += 1
                start_row = 10
            
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
