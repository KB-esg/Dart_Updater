import os
import json
import time
import re
import logging
from datetime import datetime, timedelta
from functools import wraps
from typing import List, Dict, Optional, Any, Union

import requests
import pandas as pd
import gspread
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials
from html_table_parser import parser_functions as parser
import OpenDartReader

# 1. Configuration Management
class DartConfig:
    """DART 설정 관리 클래스"""
    
    def __init__(self):
        self.target_sheets = [
            'I. 회사의 개요', 'II. 사업의 내용', '1. 사업의 개요', '2. 주요 제품 및 서비스',
            '3. 원재료 및 생산설비', '4. 매출 및 수주상황', '5. 위험관리 및 파생거래',
            '6. 주요계약 및 연구활동', '7. 기타 참고 사항', '1. 요약재무정보',
            '2. 연결재무제표', '3. 연결재무제표 주석', '4. 재무제표', '5. 재무제표 주석',
            '6. 배당에 관한 사항', '8. 기타 재무에 관한 사항', 'VII. 주주에 관한 사항',
            'VIII. 임원 및 직원 등에 관한 사항', 'X. 대주주 등과의 거래내용',
            'XI. 그 밖에 투자자 보호를 위하여 필요한 사항'
        ]
        
        self.required_env_vars = {
            'DART_API_KEY': 'DART API key for authentication',
            'GOOGLE_CREDENTIALS': 'Google service account credentials',
            'TELEGRAM_BOT_TOKEN': 'Telegram bot token for notifications',
            'TELEGRAM_CHANNEL_ID': 'Telegram channel ID for notifications'
        }
        
        self.api_limits = {
            'dart_calls_per_minute': 100,
            'sheets_writes_per_minute': 60,
            'telegram_messages_per_minute': 30
        }
    
    def validate_environment(self) -> None:
        """환경 변수 유효성 검사"""
        missing_vars = [var for var in self.required_env_vars if var not in os.environ]
        if missing_vars:
            raise ValueError(f"Missing environment variables: {', '.join(missing_vars)}")

# 2. Custom Exceptions
class DartUpdateError(Exception):
    """DART 업데이트 관련 기본 예외 클래스"""
    pass

class SheetUpdateError(DartUpdateError):
    """Google Sheets 업데이트 관련 예외"""
    pass

class DartAPIError(DartUpdateError):
    """DART API 관련 예외"""
    pass

class NotificationError(DartUpdateError):
    """알림 시스템 관련 예외"""
    pass

# 3. Rate Limiting
def rate_limit(calls: int, period: int):
    """API 호출 속도 제한 데코레이터
    
    Args:
        calls: 허용된 호출 수
        period: 시간 간격 (초)
    """
    def decorator(func):
        last_reset = time.time()
        calls_made = 0
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            nonlocal last_reset, calls_made
            
            current_time = time.time()
            if current_time - last_reset > period:
                calls_made = 0
                last_reset = current_time
                
            if calls_made >= calls:
                sleep_time = period - (current_time - last_reset)
                if sleep_time > 0:
                    time.sleep(sleep_time)
                calls_made = 0
                last_reset = time.time()
                
            calls_made += 1
            return func(*args, **kwargs)
        return wrapper
    return decorator

# 4. Data Processing
class DataProcessor:
    """데이터 처리 클래스"""
    
    @staticmethod
    def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        """DataFrame 정제
        
        Args:
            df: 원본 DataFrame
        
        Returns:
            정제된 DataFrame
        """
        # 빈 행과 열 제거
        df = df.dropna(how='all').dropna(axis=1, how='all')
        
        # 텍스트 데이터 정제
        df = df.apply(lambda x: x.str.strip() if isinstance(x, str) else x)
        
        return df
    
    @staticmethod
    def extract_numeric_values(value: str) -> Optional[float]:
        """문자열에서 숫자 값 추출
        
        Args:
            value: 변환할 문자열
        
        Returns:
            변환된 숫자 값 또는 None
        """
        if not isinstance(value, str):
            return value
            
        # 괄호와 내용 제거
        value = re.sub(r'\([^)]*\)', '', value)
        
        # 한국어 통화 표시 변환
        value = value.replace('원', '').replace('억', '00000000')
        value = value.replace('조', '000000000000')
        
        # 쉼표 제거 후 float로 변환
        try:
            return float(value.replace(',', ''))
        except ValueError:
            return None
    
    @staticmethod
    def parse_html_tables(html_content: str) -> List[List[str]]:
        """HTML 테이블 파싱
        
        Args:
            html_content: HTML 문자열
        
        Returns:
            파싱된 테이블 데이터
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        tables = soup.find_all("table")
        
        all_data = []
        for table in tables:
            table_data = parser.make2d(table)
            if table_data:
                all_data.extend(table_data)
        
        return all_data

# 5. Notification System
class NotificationSystem:
    """알림 시스템 클래스"""
    
    def __init__(self, bot_token: str, channel_id: str):
        """
        Args:
            bot_token: Telegram 봇 토큰
            channel_id: Telegram 채널 ID
        """
        self.bot_token = bot_token
        self.channel_id = channel_id
        self.logger = logging.getLogger('dart.notification')
    
    @rate_limit(calls=30, period=60)
    def send_notification(self, message: str, retry_count: int = 3) -> bool:
        """알림 메시지 전송
        
        Args:
            message: 전송할 메시지
            retry_count: 재시도 횟수
        
        Returns:
            전송 성공 여부
        
        Raises:
            NotificationError: 알림 전송 실패시
        """
        for attempt in range(retry_count):
            try:
                url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
                response = requests.post(url, data={
                    "chat_id": self.channel_id,
                    "text": message,
                    "parse_mode": "HTML"
                })
                response.raise_for_status()
                self.logger.info("Notification sent successfully")
                return True
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Attempt {attempt + 1} failed: {str(e)}")
                if attempt == retry_count - 1:
                    raise NotificationError(f"Failed to send notification: {str(e)}")
                time.sleep(2 ** attempt)  # 지수 백오프
        return False

# 6. Enhanced Logging
class DartLogger:
    """로깅 시스템 클래스"""
    
    def __init__(self, name: str, log_file: Optional[str] = None):
        """
        Args:
            name: 로거 이름
            log_file: 로그 파일 경로 (선택)
        """
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)
        
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        # 콘솔 핸들러
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
        
        # 파일 핸들러 (선택)
        if log_file:
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)

# 7. Sheet Management
class SheetManager:
    """Google Sheets 관리 클래스"""
    
    def __init__(self, credentials: Credentials, spreadsheet_id: str):
        """
        Args:
            credentials: Google 서비스 계정 인증 정보
            spreadsheet_id: 스프레드시트 ID
        """
        self.gc = gspread.authorize(credentials)
        self.workbook = self.gc.open_by_key(spreadsheet_id)
        self.logger = logging.getLogger('dart.sheets')
    
    @rate_limit(calls=60, period=60)
    def update_sheet(self, sheet_name: str, data: List[List[Any]], 
                    start_row: int = 1, start_col: int = 1) -> None:
        """시트 데이터 업데이트
        
        Args:
            sheet_name: 시트 이름
            data: 업데이트할 데이터
            start_row: 시작 행 번호
            start_col: 시작 열 번호
            
        Raises:
            SheetUpdateError: 시트 업데이트 실패시
        """
        try:
            worksheet = self.workbook.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            self.logger.info(f"Creating new worksheet: {sheet_name}")
            worksheet = self.workbook.add_worksheet(sheet_name, 1000, 26)
        
        try:
            # 배치 크기 설정
            BATCH_SIZE = 100
            for i in range(0, len(data), BATCH_SIZE):
                batch = data[i:i + BATCH_SIZE]
                range_label = f'{self._get_column_letter(start_col)}{start_row + i}'
                worksheet.batch_update([{
                    'range': range_label,
                    'values': batch
                }])
                self.logger.info(f"Updated rows {i+1} to {i+len(batch)}")
        except gspread.exceptions.APIError as e:
            if 'Quota exceeded' in str(e):
                self.logger.warning("Rate limit exceeded, waiting 60 seconds")
                time.sleep(60)
                self.update_sheet(sheet_name, data, start_row, start_col)
            else:
                raise SheetUpdateError(f"Failed to update sheet: {str(e)}")
    
    @staticmethod
    def _get_column_letter(col_num: int) -> str:
        """열 번호를 엑셀 열 문자로 변환
        
        Args:
            col_num: 열 번호
        
        Returns:
            엑셀 열 문자 (예: 1 -> A, 27 -> AA)
        """
        result = ""
        while col_num > 0:
            col_num, remainder = divmod(col_num - 1, 26)
            result = chr(65 + remainder) + result
        return result

# 8. Main DART Report Updater
class DartReportUpdater:
    """DART 보고서 업데이트 클래스"""
    
    def __init__(self, corp_code: str, spreadsheet_id: str, company_name: str):
        """
        Args:
            corp_code: 종목 코드
            spreadsheet_id: 스프레드시트 ID
            company_name: 회사명
        """
        self.config = DartConfig()
        self.config.validate_environment()
        
        self.corp_code = corp_code
        self.company_name = company_name
        
        # 인증 설정
        self.credentials = self._get_credentials()
        self.sheet_manager = SheetManager(self.credentials, spreadsheet_id)
        self.dart = OpenDartReader(os.environ['DART_API_KEY'])
        
        # 알림 시스템 설정
        self.notification = NotificationSystem(
            os.environ['TELEGRAM_BOT_TOKEN'],
            os.environ['TELEGRAM_CHANNEL_ID']
        )
        
        # 로거 설정
        self.logger = DartLogger(
            'dart.updater',
            f'dart_update_{company_name}_{datetime.now():%Y%m%d}.log'
        ).logger
    
    def _get_credentials(self) -> Credentials:
        """Google 인증 정보 설정"""
        creds_json = json.loads(os.environ['GOOGLE_CREDENTIALS'])
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        return Credentials.from_service_account_info(creds_json, scopes=scopes)
    
    def get_recent_dates(self) -> tuple[str, str]:
        """최근 3개월 날짜 범위 계산"""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=90)
        return start_date.strftime('%Y%m%d'), end_date.strftime('%Y%m%d')
    
    @rate_limit(calls=100, period=60)
    def update_dart_reports(self) -> None:
        """DART 보고서 데이터 업데이트"""
        try:
            start_date, end_date = self.get_recent_dates()
            report_list = self.dart.list(
                self.corp_code, 
                start_date, 
                end_date, 
                kind='A', 
                final='T'
            )
            
            if report_list.empty:
                self.logger.info("No new reports found")
                return
            
            for _, report in report_list.iterrows():
                self.process_report(report['rcept_no'])
                self.logger.info(f"Processed report: {report['report_nm']}")
                
        except Exception as e:
            self.logger.error(f"Failed to update reports: {str(e)}")
            raise DartUpdateError(f"Report update failed: {str(e)}")
    
    def process_report(self, rcept_no: str) -> None:
        """개별 보고서 처리
        
        Args:
            rcept_no: 보고서 접수번호
        """
        try:
            report_index = self.dart.sub_docs(rcept_no)
            target_docs = report_index[
                report_index['title'].isin(self.config.target_sheets)
            ]
            
            for _, doc in target_docs.iterrows():
                self.update_worksheet(doc['title'], doc['url'])
                
        except Exception as e:
            self.logger.error(f"Failed to process report {rcept_no}: {str(e)}")
            raise DartUpdateError(f"Report processing failed: {str(e)}")
    
    def update_worksheet(self, sheet_name: str, url: str) -> None:
        """워크시트 업데이트
        
        Args:
            sheet_name: 시트 이름
            url: 문서 URL
        """
        try:
            response = requests.get(url)
            response.raise_for_status()
            
            # HTML 테이블 파싱
            html_content = response.text
            processed_data = DataProcessor.parse_html_tables(html_content)
            
            if not processed_data:
                self.logger.warning(f"No data found in {sheet_name}")
                return
            
            # 데이터 정제
            df = pd.DataFrame(processed_data)
            df = DataProcessor.clean_dataframe(df)
            
            # 시트 업데이트
            self.sheet_manager.update_sheet(sheet_name, df.values.tolist())
            self.logger.info(f"Updated worksheet: {sheet_name}")
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to fetch URL {url}: {str(e)}")
            raise DartUpdateError(f"URL fetch failed: {str(e)}")
        except Exception as e:
            self.logger.error(f"Failed to update worksheet {sheet_name}: {str(e)}")
            raise SheetUpdateError(f"Worksheet update failed: {str(e)}")
    
    def process_archive_data(self, archive_sheet_name: str = 'Dart_Archive', 
                           start_row: int = 6) -> None:
        """아카이브 데이터 처리
        
        Args:
            archive_sheet_name: 아카이브 시트 이름
            start_row: 시작 행 번호
        """
        try:
            archive = self.sheet_manager.workbook.worksheet(archive_sheet_name)
            sheet_values = archive.get_all_values()
            
            if not sheet_values:
                raise ValueError(f"{archive_sheet_name} 시트가 비어있습니다")
            
            last_col = len(sheet_values[0])
            control_value = archive.cell(1, last_col).value
            
            if control_value:
                last_col += 1
            
            self._process_archive_sheet(archive, sheet_values, start_row, last_col)
            
        except Exception as e:
            error_msg = f"아카이브 처리 중 오류 발생: {str(e)}"
            self.logger.error(error_msg)
            self.notification.send_notification(
                f"❌ {self.company_name} 아카이브 업데이트 실패\n\n"
                f"오류: {str(e)}"
            )
            raise DartUpdateError(error_msg)
    
    def _process_archive_sheet(self, archive, sheet_values: List[List[str]], 
                             start_row: int, last_col: int) -> None:
        """아카이브 시트 상세 처리
        
        Args:
            archive: 워크시트 객체
            sheet_values: 시트 데이터
            start_row: 시작 행
            last_col: 마지막 열
        """
        sheet_cache = {}
        update_data = []
        
        for row_idx in range(start_row - 1, len(sheet_values)):
            row_data = sheet_values[row_idx]
            if len(row_data) < 5:
                continue
                
            sheet_name = row_data[0]
            if not sheet_name:
                continue
                
            try:
                value = self._process_archive_row(
                    sheet_name, row_data, sheet_cache, row_idx + 1
                )
                if value is not None:
                    update_data.append((row_idx + 1, value))
                    
            except Exception as e:
                self.logger.error(f"행 {row_idx + 1} 처리 중 오류: {str(e)}")
        
        if update_data:
            self._update_archive_data(archive, update_data, last_col)
    
    def _process_archive_row(self, sheet_name: str, row_data: List[str], 
                           sheet_cache: Dict[str, pd.DataFrame], 
                           row_num: int) -> Optional[str]:
        """아카이브 행 처리
        
        Args:
            sheet_name: 시트 이름
            row_data: 행 데이터
            sheet_cache: 시트 캐시
            row_num: 행 번호
            
        Returns:
            처리된 값 또는 None
        """
        keyword = row_data[1]
        if not keyword or not row_data[2] or not row_data[3] or not row_data[4]:
            return None
            
        if sheet_name not in sheet_cache:
            search_sheet = self.sheet_manager.workbook.worksheet(sheet_name)
            sheet_data = search_sheet.get_all_values()
            sheet_cache[sheet_name] = pd.DataFrame(sheet_data)
            
        df = sheet_cache[sheet_name]
        n = int(row_data[2])
        x = int(row_data[3])
        y = int(row_data[4])
        
        keyword_positions = []
        for idx, df_row in df.iterrows():
            for col_idx, value in enumerate(df_row):
                if value == keyword:
                    keyword_positions.append((idx, col_idx))
        
        if keyword_positions and len(keyword_positions) >= n:
            target_pos = keyword_positions[n - 1]
            target_row = target_pos[0] + y
            target_col = target_pos[1] + x
            
            if 0 <= target_row < df.shape[0] and 0 <= target_col < df.shape[1]:
                value = df.iat[target_row, target_col]
                return DataProcessor.remove_parentheses(str(value))
        
        return None
    
    def _update_archive_data(self, archive, update_data: List[tuple[int, str]], 
                           last_col: int) -> None:
        """아카이브 데이터 업데이트
        
        Args:
            archive: 워크시트 객체
            update_data: 업데이트할 데이터
            last_col: 마지막 열
        """
        try:
            # 데이터 준비
            min_row = min(row for row, _ in update_data)
            max_row = max(row for row, _ in update_data)
            column_data = [[''] for _ in range(max_row - min_row + 1)]
            
            for row, value in update_data:
                adjusted_row = row - min_row
                column_data[adjusted_row] = [value]
            
            # 데이터 업데이트
            col_letter = self.sheet_manager._get_column_letter(last_col)
            range_label = f'{col_letter}{min_row}:{col_letter}{max_row}'
            
            archive.batch_update([{
                'range': range_label,
                'values': column_data
            }])
            
            # 메타데이터 업데이트
            self._update_archive_metadata(archive, last_col)
            
            # 완료 알림
            self._send_update_notification(len(update_data), last_col)
            
        except Exception as e:
            error_msg = f"아카이브 데이터 업데이트 실패: {str(e)}"
            self.logger.error(error_msg)
            raise SheetUpdateError(error_msg)
    
    def _update_archive_metadata(self, archive, last_col: int) -> None:
        """아카이브 메타데이터 업데이트
        
        Args:
            archive: 워크시트 객체
            last_col: 마지막 열
        """
        today = datetime.now()
        three_months_ago = today - timedelta(days=90)
        year = str(three_months_ago.year)[2:]
        quarter = (three_months_ago.month - 1) // 3 + 1
        quarter_text = f"{quarter}Q{year}"
        
        col_letter = self.sheet_manager._get_column_letter(last_col)
        
        meta_updates = [
            {'range': 'J1', 'values': [[today.strftime('%Y-%m-%d')]]},
            {'range': f'{col_letter}1', 'values': [['1']]},
            {'range': f'{col_letter}5', 'values': [[today.strftime('%Y-%m-%d')]]},
            {'range': f'{col_letter}6', 'values': [[quarter_text]]}
        ]
        
        archive.batch_update(meta_updates)
    
    def _send_update_notification(self, rows_updated: int, last_col: int) -> None:
        """업데이트 완료 알림 전송
        
        Args:
            rows_updated: 업데이트된 행 수
            last_col: 마지막 열
        """
        today = datetime.now()
        three_months_ago = today - timedelta(days=90)
        quarter = (three_months_ago.month - 1) // 3 + 1
        year = str(three_months_ago.year)[2:]
        quarter_text = f"{quarter}Q{year}"
        
        message = (
            f"🔄 DART 업데이트 완료\n\n"
            f"• 종목: {self.company_name} ({self.corp_code})\n"
            f"• 분기: {quarter_text}\n"
            f"• 업데이트 일시: {today.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"• 처리된 행: {rows_updated}개\n"
            f"• 시트 열: {self.sheet_manager._get_column_letter(last_col)} (#{last_col})"
        )
        
        self.notification.send_notification(message)

def main():
    """메인 실행 함수"""
    logger = logging.getLogger('dart.main')
    
    try:
        COMPANY_INFO = {
            'code': '00139834',
            'name': '엘지씨엔에스',
            'spreadsheet_id': os.environ['LGCNS_SPREADSHEET_ID']
        }
        
        logger.info(f"{COMPANY_INFO['name']}({COMPANY_INFO['code']}) 보고서 업데이트 시작")
        
        updater = DartReportUpdater(
            COMPANY_INFO['code'],
            COMPANY_INFO['spreadsheet_id'],
            COMPANY_INFO['name']
        )
        
        # DART 보고서 업데이트
        updater.update_dart_reports()
        logger.info("보고서 시트 업데이트 완료")
        
        # 아카이브 시트 업데이트
        logger.info("Dart_Archive 시트 업데이트 시작")
        updater.process_archive_data()
        logger.info("Dart_Archive 시트 업데이트 완료")
        
    except Exception as e:
        error_msg = f"전체 작업 중 오류 발생: {str(e)}"
        logger.error(error_msg)
        logger.error(f"오류 상세 정보: {type(e).__name__}")
        
        if 'updater' in locals():
            updater.notification.send_notification(
                f"❌ DART 업데이트 중 치명적 오류 발생\n\n"
                f"• 종목: {COMPANY_INFO['name']} ({COMPANY_INFO['code']})\n"
                f"• 오류: {str(e)}"
            )
        raise e

if __name__ == "__main__":
    # 로깅 설정
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(
                f'dart_update_{datetime.now():%Y%m%d}.log',
                encoding='utf-8'
            )
        ]
    )
    
    try:
        main()
    except KeyboardInterrupt:
        logging.info("사용자에 의해 프로그램이 중단되었습니다.")
        sys.exit(0)
    except Exception as e:
        logging.error(f"프로그램 실행 중 오류 발생: {str(e)}")
        sys.exit(1)
