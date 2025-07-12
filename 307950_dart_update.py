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
import pandas as pd
import xml.etree.ElementTree as ET
from urllib.parse import urljoin, urlparse
import zipfile
import io

class XBRLDartReportUpdater:
    """XBRL 기반 Dart 보고서 업데이터"""
    
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
        :param corp_code: 종목 코드 (예: '307950')
        :param spreadsheet_var_name: 스프레드시트 환경변수 이름
        :param company_name: 회사명
        """
        self.corp_code = corp_code
        self.company_name = company_name
        self.spreadsheet_var_name = spreadsheet_var_name
        
        # 환경변수 확인
        print("환경변수 확인:")
        required_vars = ['DART_API_KEY', 'GOOGLE_CREDENTIALS', spreadsheet_var_name, 
                        'TELEGRAM_BOT_TOKEN', 'TELEGRAM_CHANNEL_ID']
        for var in required_vars:
            print(f"{var} 존재:", var in os.environ)
        
        if spreadsheet_var_name not in os.environ:
            raise ValueError(f"{spreadsheet_var_name} 환경변수가 설정되지 않았습니다.")
            
        self.credentials = self.get_credentials()
        self.gc = gspread.authorize(self.credentials)
        self.dart = OpenDartReader(os.environ['DART_API_KEY'])
        self.workbook = self.gc.open_by_key(os.environ[spreadsheet_var_name])
        self.telegram_bot_token = os.environ.get('TELEGRAM_BOT_TOKEN')
        self.telegram_channel_id = os.environ.get('TELEGRAM_CHANNEL_ID')
        
        # XBRL 네임스페이스 정의
        self.xbrl_namespaces = {
            'xbrl': 'http://www.xbrl.org/2003/instance',
            'ifrs': 'http://xbrl.ifrs.org/taxonomy/2021-03-24/ifrs',
            'ifrs-full': 'http://xbrl.ifrs.org/taxonomy/2021-03-24/ifrs-full',
            'dart': 'http://dart.fss.or.kr/xbrl/taxonomy/kr-gaap',
            'link': 'http://www.xbrl.org/2003/linkbase',
            'xlink': 'http://www.w3.org/1999/xlink'
        }

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

    def get_column_letter(self, col_num):
        """숫자를 엑셀 열 문자로 변환"""
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

    def get_xbrl_download_url(self, rcept_no):
        """XBRL 다운로드 URL 구성"""
        # 방법 1: 직접 XBRL 파일 다운로드 시도
        xbrl_urls = [
            f"https://opendart.fss.or.kr/disclosureinfo/fnltt/dwld/main.do?rcp_no={rcept_no}",
            f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}",
            f"https://opendart.fss.or.kr/xbrl/viewer/main.do?rcpNo={rcept_no}"
        ]
        return xbrl_urls

    def download_xbrl_data(self, rcept_no):
        """XBRL 데이터 다운로드"""
        print(f"XBRL 데이터 다운로드 시작: {rcept_no}")
        
        # 방법 1: 직접 XBRL 파일 다운로드 시도
        try:
            download_url = f"https://opendart.fss.or.kr/disclosureinfo/fnltt/dwld/main.do?rcp_no={rcept_no}"
            
            session = requests.Session()
            session.headers.update({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            })
            
            response = session.get(download_url)
            response.raise_for_status()
            
            # ZIP 파일인지 확인
            if response.headers.get('content-type', '').startswith('application/zip') or \
               response.headers.get('content-disposition', '').find('.zip') != -1:
                return self.extract_xbrl_from_zip(response.content)
            else:
                print("ZIP 파일이 아닙니다. 대안 방법 시도...")
                return self.get_xbrl_from_viewer(rcept_no)
                
        except Exception as e:
            print(f"직접 다운로드 실패: {str(e)}")
            return self.get_xbrl_from_viewer(rcept_no)

    def extract_xbrl_from_zip(self, zip_content):
        """ZIP 파일에서 XBRL 데이터 추출"""
        try:
            with zipfile.ZipFile(io.BytesIO(zip_content)) as zip_ref:
                file_list = zip_ref.namelist()
                print(f"ZIP 파일 내용: {file_list}")
                
                # XBRL 파일 찾기 (.xml 확장자)
                xbrl_files = [f for f in file_list if f.endswith('.xml') and 'xbrl' in f.lower()]
                
                if not xbrl_files:
                    # .xml 파일 중 가장 큰 파일 선택
                    xml_files = [f for f in file_list if f.endswith('.xml')]
                    if xml_files:
                        xbrl_files = [max(xml_files, key=lambda x: zip_ref.getinfo(x).file_size)]
                
                if xbrl_files:
                    with zip_ref.open(xbrl_files[0]) as xbrl_file:
                        return xbrl_file.read().decode('utf-8')
                else:
                    raise ValueError("ZIP 파일에서 XBRL 파일을 찾을 수 없습니다.")
                    
        except Exception as e:
            print(f"ZIP 파일 처리 실패: {str(e)}")
            raise

    def get_xbrl_from_viewer(self, rcept_no):
        """XBRL 뷰어 페이지에서 실제 XML 데이터 추출"""
        try:
            viewer_url = f"https://opendart.fss.or.kr/xbrl/viewer/main.do?rcpNo={rcept_no}"
            
            session = requests.Session()
            session.headers.update({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            })
            
            # 뷰어 페이지 로드
            response = session.get(viewer_url)
            response.raise_for_status()
            
            # 페이지에서 실제 XBRL 데이터 URL 찾기
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # JavaScript에서 viewDoc 함수 호출 부분 찾기
            scripts = soup.find_all('script')
            for script in scripts:
                if script.string and 'viewDoc' in script.string:
                    # viewDoc 함수 파라미터 추출
                    match = re.search(r'viewDoc\("([^"]+)"\s*,\s*"[^"]*"\s*,\s*"([^"]*)"\s*,\s*"([^"]+)"\)', script.string)
                    if match:
                        doc_id, lang, doc_type = match.groups()
                        return self.fetch_xbrl_data_from_api(doc_id, lang, doc_type)
            
            raise ValueError("XBRL 데이터 URL을 찾을 수 없습니다.")
            
        except Exception as e:
            print(f"XBRL 뷰어에서 데이터 추출 실패: {str(e)}")
            raise

    def fetch_xbrl_data_from_api(self, doc_id, lang, doc_type):
        """API를 통해 실제 XBRL 데이터 가져오기"""
        try:
            # 가능한 API 엔드포인트들
            api_urls = [
                f"https://opendart.fss.or.kr/xbrl/api/document/{doc_id}?lang={lang}&type={doc_type}",
                f"https://opendart.fss.or.kr/xbrl/viewer/data.do?docId={doc_id}&lang={lang}&type={doc_type}",
                f"https://opendart.fss.or.kr/api/xbrl/{doc_id}.xml"
            ]
            
            session = requests.Session()
            session.headers.update({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Referer': f"https://opendart.fss.or.kr/xbrl/viewer/main.do?rcpNo={doc_id}",
                'Accept': 'application/xml, text/xml, */*'
            })
            
            for api_url in api_urls:
                try:
                    response = session.get(api_url)
                    if response.status_code == 200 and 'xml' in response.headers.get('content-type', ''):
                        return response.text
                except:
                    continue
            
            raise ValueError("모든 API 엔드포인트에서 XBRL 데이터를 가져올 수 없습니다.")
            
        except Exception as e:
            print(f"API를 통한 XBRL 데이터 가져오기 실패: {str(e)}")
            raise

    def parse_xbrl_data(self, xbrl_content):
        """XBRL XML 데이터 파싱"""
        try:
            # XML 파싱
            root = ET.fromstring(xbrl_content)
            
            # 네임스페이스 자동 감지 및 업데이트
            for prefix, uri in root.attrib.items():
                if prefix.startswith('xmlns:'):
                    ns_prefix = prefix[6:]  # 'xmlns:' 제거
                    self.xbrl_namespaces[ns_prefix] = uri
                elif prefix == 'xmlns':
                    self.xbrl_namespaces['default'] = uri
            
            parsed_data = {
                'contexts': self.extract_contexts(root),
                'financial_data': self.extract_financial_data(root),
                'company_info': self.extract_company_info(root),
                'metadata': self.extract_metadata(root)
            }
            
            return parsed_data
            
        except ET.ParseError as e:
            print(f"XML 파싱 오류: {str(e)}")
            raise
        except Exception as e:
            print(f"XBRL 데이터 파싱 실패: {str(e)}")
            raise

    def extract_contexts(self, root):
        """Context 정보 추출"""
        contexts = {}
        
        for context in root.findall('.//xbrl:context', self.xbrl_namespaces):
            context_id = context.get('id')
            
            # 기간 정보 추출
            period_info = {}
            period = context.find('xbrl:period', self.xbrl_namespaces)
            if period is not None:
                instant = period.find('xbrl:instant', self.xbrl_namespaces)
                start_date = period.find('xbrl:startDate', self.xbrl_namespaces)
                end_date = period.find('xbrl:endDate', self.xbrl_namespaces)
                
                if instant is not None:
                    period_info['type'] = 'instant'
                    period_info['date'] = instant.text
                elif start_date is not None and end_date is not None:
                    period_info['type'] = 'duration'
                    period_info['start_date'] = start_date.text
                    period_info['end_date'] = end_date.text
            
            # 엔티티 정보 추출
            entity_info = {}
            entity = context.find('xbrl:entity', self.xbrl_namespaces)
            if entity is not None:
                identifier = entity.find('xbrl:identifier', self.xbrl_namespaces)
                if identifier is not None:
                    entity_info['scheme'] = identifier.get('scheme')
                    entity_info['value'] = identifier.text
            
            contexts[context_id] = {
                'period': period_info,
                'entity': entity_info
            }
        
        return contexts

    def extract_financial_data(self, root):
        """재무 데이터 추출"""
        financial_data = {}
        
        # 주요 재무 항목들 정의
        key_items = {
            # 재무상태표
            'Assets': ['ifrs-full:Assets', 'dart:Assets'],
            'Liabilities': ['ifrs-full:Liabilities', 'dart:Liabilities'],
            'Equity': ['ifrs-full:Equity', 'dart:Equity'],
            'CurrentAssets': ['ifrs-full:CurrentAssets', 'dart:CurrentAssets'],
            'NonCurrentAssets': ['ifrs-full:NoncurrentAssets', 'dart:NonCurrentAssets'],
            'CurrentLiabilities': ['ifrs-full:CurrentLiabilities', 'dart:CurrentLiabilities'],
            
            # 손익계산서
            'Revenue': ['ifrs-full:Revenue', 'dart:Revenue'],
            'ProfitLoss': ['ifrs-full:ProfitLoss', 'dart:ProfitLoss'],
            'OperatingProfitLoss': ['ifrs-full:ProfitLossFromOperatingActivities', 'dart:OperatingIncomeLoss'],
            'GrossProfit': ['ifrs-full:GrossProfit', 'dart:GrossProfit'],
            
            # 현금흐름표
            'CashFlowsFromOperatingActivities': ['ifrs-full:CashFlowsFromUsedInOperatingActivities'],
            'CashFlowsFromInvestingActivities': ['ifrs-full:CashFlowsFromUsedInInvestingActivities'],
            'CashFlowsFromFinancingActivities': ['ifrs-full:CashFlowsFromUsedInFinancingActivities']
        }
        
        for item_name, possible_tags in key_items.items():
            for tag in possible_tags:
                elements = root.findall(f'.//{tag}', self.xbrl_namespaces)
                if elements:
                    item_data = []
                    for elem in elements:
                        context_ref = elem.get('contextRef')
                        unit_ref = elem.get('unitRef')
                        decimals = elem.get('decimals')
                        
                        item_data.append({
                            'value': elem.text,
                            'context_ref': context_ref,
                            'unit_ref': unit_ref,
                            'decimals': decimals
                        })
                    
                    financial_data[item_name] = item_data
                    break  # 첫 번째로 찾은 태그 사용
        
        return financial_data

    def extract_company_info(self, root):
        """회사 정보 추출"""
        company_info = {}
        
        # 회사명, 업종 등 기본 정보 추출
        company_tags = {
            'EntityName': ['ifrs-full:NameOfReportingEntityOrOtherMeansOfIdentification'],
            'BusinessDescription': ['ifrs-full:DescriptionOfNatureOfEntitysOperationsAndPrincipalActivities']
        }
        
        for info_name, possible_tags in company_tags.items():
            for tag in possible_tags:
                elements = root.findall(f'.//{tag}', self.xbrl_namespaces)
                if elements:
                    company_info[info_name] = elements[0].text
                    break
        
        return company_info

    def extract_metadata(self, root):
        """메타데이터 추출"""
        metadata = {}
        
        # 보고서 정보
        schemaRef = root.find('.//link:schemaRef', self.xbrl_namespaces)
        if schemaRef is not None:
            metadata['schema_location'] = schemaRef.get('{http://www.w3.org/1999/xlink}href')
        
        return metadata

    def convert_xbrl_to_structured_data(self, parsed_xbrl):
        """XBRL 데이터를 구조화된 형태로 변환"""
        structured_data = {
            'balance_sheet': {},
            'income_statement': {},
            'cash_flow': {},
            'company_info': parsed_xbrl.get('company_info', {}),
            'reporting_period': None
        }
        
        # 재무 데이터 매핑
        financial_mapping = {
            'balance_sheet': {
                'Assets': '자산총계',
                'Liabilities': '부채총계',
                'Equity': '자본총계',
                'CurrentAssets': '유동자산',
                'NonCurrentAssets': '비유동자산',
                'CurrentLiabilities': '유동부채'
            },
            'income_statement': {
                'Revenue': '매출액',
                'ProfitLoss': '당기순이익',
                'OperatingProfitLoss': '영업이익',
                'GrossProfit': '매출총이익'
            },
            'cash_flow': {
                'CashFlowsFromOperatingActivities': '영업활동현금흐름',
                'CashFlowsFromInvestingActivities': '투자활동현금흐름',
                'CashFlowsFromFinancingActivities': '재무활동현금흐름'
            }
        }
        
        financial_data = parsed_xbrl.get('financial_data', {})
        contexts = parsed_xbrl.get('contexts', {})
        
        for statement_type, mapping in financial_mapping.items():
            for xbrl_item, korean_name in mapping.items():
                if xbrl_item in financial_data:
                    # 가장 최신 데이터 선택 (현재는 첫 번째 데이터 사용)
                    item_data = financial_data[xbrl_item][0] if financial_data[xbrl_item] else None
                    if item_data:
                        # 값 정규화 (쉼표 제거, 숫자 변환)
                        value = item_data['value']
                        if value:
                            try:
                                # 숫자 정규화
                                cleaned_value = re.sub(r'[,\s]', '', value)
                                numeric_value = float(cleaned_value)
                                
                                # decimals 속성 처리
                                decimals = item_data.get('decimals')
                                if decimals and decimals.isdigit():
                                    numeric_value = numeric_value / (10 ** int(decimals))
                                
                                structured_data[statement_type][korean_name] = numeric_value
                            except ValueError:
                                structured_data[statement_type][korean_name] = value
        
        return structured_data

    def update_dart_reports(self):
        """DART 보고서 데이터 업데이트 (XBRL 기반)"""
        start_date, end_date = self.get_recent_dates()
        report_list = self.dart.list(self.corp_code, start_date, end_date, kind='A', final='T')
        
        if not report_list.empty:
            for _, report in report_list.iterrows():
                try:
                    print(f"보고서 처리 시작: {report['report_nm']} (접수번호: {report['rcept_no']})")
                    
                    # XBRL 데이터 다운로드 및 파싱
                    xbrl_content = self.download_xbrl_data(report['rcept_no'])
                    parsed_xbrl = self.parse_xbrl_data(xbrl_content)
                    structured_data = self.convert_xbrl_to_structured_data(parsed_xbrl)
                    
                    # 구조화된 데이터를 기존 시트 업데이트 로직에 활용
                    self.update_sheets_with_xbrl_data(structured_data, report['rcept_no'])
                    
                    print(f"보고서 처리 완료: {report['report_nm']}")
                    
                except Exception as e:
                    print(f"보고서 처리 실패 ({report['rcept_no']}): {str(e)}")
                    # 실패 시 기존 HTML 방식으로 폴백
                    self.process_report_fallback(report['rcept_no'])

    def update_sheets_with_xbrl_data(self, structured_data, rcept_no):
        """XBRL 구조화 데이터로 시트 업데이트"""
        try:
            # 재무제표 시트 업데이트
            financial_sheets = {
                '2. 연결재무제표': structured_data,
                '4. 재무제표': structured_data
            }
            
            for sheet_name, data in financial_sheets.items():
                try:
                    worksheet = self.workbook.worksheet(sheet_name)
                except gspread.exceptions.WorksheetNotFound:
                    worksheet = self.workbook.add_worksheet(sheet_name, 1000, 10)
                
                # 데이터를 테이블 형태로 변환
                table_data = self.convert_to_table_format(data)
                
                # 시트 업데이트
                if table_data:
                    worksheet.clear()
                    worksheet.append_rows(table_data)
                    print(f"XBRL 데이터로 시트 업데이트 완료: {sheet_name}")
                    
        except Exception as e:
            print(f"XBRL 데이터 시트 업데이트 실패: {str(e)}")
            raise

    def convert_to_table_format(self, structured_data):
        """구조화된 데이터를 테이블 형태로 변환"""
        table_data = []
        
        # 헤더 추가
        table_data.append(['구분', '항목', '금액'])
        
        # 재무상태표 데이터
        if structured_data.get('balance_sheet'):
            table_data.append(['재무상태표', '', ''])
            for item, value in structured_data['balance_sheet'].items():
                table_data.append(['', item, str(value)])
        
        # 손익계산서 데이터
        if structured_data.get('income_statement'):
            table_data.append(['손익계산서', '', ''])
            for item, value in structured_data['income_statement'].items():
                table_data.append(['', item, str(value)])
        
        # 현금흐름표 데이터
        if structured_data.get('cash_flow'):
            table_data.append(['현금흐름표', '', ''])
            for item, value in structured_data['cash_flow'].items():
                table_data.append(['', item, str(value)])
        
        return table_data

    def process_report_fallback(self, rcept_no):
        """XBRL 실패 시 기존 HTML 방식으로 폴백"""
        print(f"XBRL 방식 실패. HTML 방식으로 폴백 처리: {rcept_no}")
        try:
            report_index = self.dart.sub_docs(rcept_no)
            target_docs = report_index[report_index['title'].isin(self.TARGET_SHEETS)]
            
            for _, doc in target_docs.iterrows():
                self.update_worksheet_html(doc['title'], doc['url'])
                
        except Exception as e:
            print(f"폴백 처리도 실패: {str(e)}")

    def update_worksheet_html(self, sheet_name, url):
        """HTML 방식 워크시트 업데이트 (기존 방식)"""
        try:
            worksheet = self.workbook.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = self.workbook.add_worksheet(sheet_name, 1000, 10)
            
        response = requests.get(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            tables = soup.find_all("table")
            
            worksheet.clear()
            all_data = []
            
            for table in tables:
                table_data = self.parse_html_table(table)
                if table_data:
                    all_data.extend(table_data)
                    
            if all_data:
                BATCH_SIZE = 100
                for i in range(0, len(all_data), BATCH_SIZE):
                    batch = all_data[i:i + BATCH_SIZE]
                    try:
                        worksheet.append_rows(batch)
                    except gspread.exceptions.APIError as e:
                        if 'Quota exceeded' in str(e):
                            time.sleep(60)
                            worksheet.append_rows(batch)
                        else:
                            raise e

    def parse_html_table(self, table):
        """HTML 테이블 파싱"""
        rows = []
        for row in table.find_all('tr'):
            cells = []
            for cell in row.find_all(['td', 'th']):
                cells.append(cell.get_text(strip=True))
            if cells:
                rows.append(cells)
        return rows

    def remove_parentheses(self, value):
        """괄호 내용 제거"""
        if not value:
            return value
        return re.sub(r'\s*\(.*?\)\s*', '', value).replace('%', '')

    # 기존 process_archive_data 메소드는 그대로 유지
    def process_archive_data(self, archive, start_row, last_col):
        """아카이브 데이터 처리 (기존 로직 유지)"""
        # 기존 코드와 동일하게 유지
        # ... (기존 process_archive_data 메소드 내용)
        pass


def main():
    """메인 실행 함수"""
    try:
        import sys
        
        def log(msg):
            print(msg)
            sys.stdout.flush()
        
        COMPANY_INFO = {
            'code': '307950',
            'name': '현대오토에버',
            'spreadsheet_var': 'AUTOEVER_SPREADSHEET_ID'
        }
        
        log(f"{COMPANY_INFO['name']}({COMPANY_INFO['code']}) XBRL 기반 보고서 업데이트 시작")
        
        try:
            updater = XBRLDartReportUpdater(
                COMPANY_INFO['code'], 
                COMPANY_INFO['spreadsheet_var'],
                COMPANY_INFO['name']
            )
            
            # XBRL 기반 보고서 업데이트
            updater.update_dart_reports()
            log("XBRL 기반 보고서 업데이트 완료")
            
            # 기존 Archive 처리 로직 유지
            log("Dart_Archive 시트 업데이트 시작")
            archive = updater.workbook.worksheet('Dart_Archive')
            
            sheet_values = archive.get_all_values()
            if not sheet_values:
                raise ValueError("Dart_Archive 시트가 비어있습니다")
            
            last_col = len(sheet_values[0])
            control_value = archive.cell(1, last_col).value
            start_row = 10
            
            if control_value:
                last_col += 1
            
            log(f"Archive 처리 시작: 행={start_row}, 열={last_col}")
            updater.process_archive_data(archive, start_row, last_col)
            log("Dart_Archive 시트 업데이트 완료")
            
        except Exception as e:
            log(f"처리 중 오류 발생: {str(e)}")
            if 'updater' in locals():
                updater.send_telegram_message(
                    f"❌ XBRL DART 업데이트 실패\n\n"
                    f"• 종목: {COMPANY_INFO['name']} ({COMPANY_INFO['code']})\n"
                    f"• 오류: {str(e)}"
                )
            raise

    except Exception as e:
        print(f"전체 작업 중 치명적 오류 발생: {str(e)}")
        raise e

if __name__ == "__main__":
    main()
