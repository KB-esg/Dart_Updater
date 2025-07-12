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

# HTML 테이블 파서 대안 구현
try:
    from html_table_parser import parser_functions as parser
    HTML_PARSER_AVAILABLE = True
    print("✅ html_table_parser 로드 성공")
except ImportError:
    try:
        from html_table_parser_python3 import parser_functions as parser
        HTML_PARSER_AVAILABLE = True
        print("✅ html_table_parser_python3 로드 성공")
    except ImportError:
        HTML_PARSER_AVAILABLE = False
        print("⚠️ HTML 파서 패키지가 없습니다. 내장 파서를 사용합니다.")

class XBRLDartReportUpdater:
    """XBRL 기반 Dart 보고서 업데이터 (견고한 HTML 파싱 포함)"""
    
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
        """초기화"""
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

    def parse_html_table_fallback(self, table):
        """내장 HTML 테이블 파서 (대안)"""
        try:
            rows = []
            for tr in table.find_all('tr'):
                row = []
                for cell in tr.find_all(['td', 'th']):
                    # 셀 병합 처리
                    colspan = int(cell.get('colspan', 1))
                    rowspan = int(cell.get('rowspan', 1))
                    
                    # 텍스트 정리
                    text = cell.get_text(separator=' ', strip=True)
                    text = re.sub(r'\s+', ' ', text)  # 연속 공백 제거
                    
                    row.append(text)
                    
                    # colspan 처리 (빈 셀 추가)
                    for _ in range(colspan - 1):
                        row.append('')
                
                if row:  # 빈 행 제외
                    rows.append(row)
            
            return rows
            
        except Exception as e:
            print(f"내장 HTML 파서 오류: {str(e)}")
            return []

    def parse_html_table_robust(self, table):
        """견고한 HTML 테이블 파싱"""
        try:
            # 우선 외부 라이브러리 시도
            if HTML_PARSER_AVAILABLE:
                try:
                    return parser.make2d(table)
                except Exception as e:
                    print(f"외부 HTML 파서 실패, 내장 파서로 전환: {str(e)}")
            
            # 내장 파서 사용
            return self.parse_html_table_fallback(table)
            
        except Exception as e:
            print(f"HTML 테이블 파싱 실패: {str(e)}")
            return []

    def process_html_content(self, worksheet, html_content):
        """HTML 내용 처리 및 워크시트 업데이트 (개선된 버전)"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            tables = soup.find_all("table")
            
            worksheet.clear()
            all_data = []
            
            print(f"발견된 테이블 수: {len(tables)}")
            
            for i, table in enumerate(tables):
                print(f"테이블 {i+1} 처리 중...")
                
                # 견고한 HTML 파싱 사용
                table_data = self.parse_html_table_robust(table)
                
                if table_data:
                    print(f"테이블 {i+1}: {len(table_data)}행 추출")
                    all_data.extend(table_data)
                    # 테이블 간 구분을 위한 빈 행 추가
                    all_data.append([''])
                else:
                    print(f"테이블 {i+1}: 데이터 없음")
            
            if all_data:
                # 마지막 빈 행 제거
                if all_data and all_data[-1] == ['']:
                    all_data.pop()
                
                print(f"전체 {len(all_data)}행 데이터 준비 완료")
                
                # 배치 처리로 업로드
                BATCH_SIZE = 100
                for i in range(0, len(all_data), BATCH_SIZE):
                    batch = all_data[i:i + BATCH_SIZE]
                    try:
                        # 행 길이 정규화 (가장 긴 행에 맞춤)
                        max_cols = max(len(row) for row in batch) if batch else 0
                        normalized_batch = []
                        for row in batch:
                            normalized_row = row + [''] * (max_cols - len(row))
                            normalized_batch.append(normalized_row)
                        
                        worksheet.append_rows(normalized_batch)
                        print(f"배치 업로드 완료: {i+1}~{min(i+BATCH_SIZE, len(all_data))} 행")
                        
                    except gspread.exceptions.APIError as e:
                        if 'Quota exceeded' in str(e):
                            print("API 할당량 초과. 60초 대기 후 재시도...")
                            time.sleep(60)
                            worksheet.append_rows(normalized_batch)
                        else:
                            print(f"API 오류: {str(e)}")
                            raise e
                    except Exception as e:
                        print(f"배치 업로드 오류: {str(e)}")
                        # 개별 행으로 재시도
                        for row in batch:
                            try:
                                worksheet.append_row(row)
                                time.sleep(0.1)  # API 제한 방지
                            except Exception as row_e:
                                print(f"개별 행 업로드 실패: {str(row_e)}")
                                continue
            else:
                print("⚠️ 추출된 데이터가 없습니다.")
                
        except Exception as e:
            print(f"HTML 콘텐츠 처리 중 전체 오류: {str(e)}")
            raise

    def download_xbrl_data(self, rcept_no):
        """XBRL 데이터 다운로드"""
        print(f"XBRL 데이터 다운로드 시작: {rcept_no}")
        
        try:
            # 방법 1: 직접 XBRL 파일 다운로드
            download_url = f"https://opendart.fss.or.kr/disclosureinfo/fnltt/dwld/main.do?rcp_no={rcept_no}"
            
            session = requests.Session()
            session.headers.update({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'application/zip, application/xml, text/xml, */*',
                'Referer': 'https://opendart.fss.or.kr/'
            })
            
            response = session.get(download_url, timeout=30)
            response.raise_for_status()
            
            content_type = response.headers.get('content-type', '').lower()
            content_disposition = response.headers.get('content-disposition', '').lower()
            
            # ZIP 파일 확인
            if ('application/zip' in content_type or 
                'application/x-zip' in content_type or 
                '.zip' in content_disposition):
                print("ZIP 파일 감지, 압축 해제 중...")
                return self.extract_xbrl_from_zip(response.content)
            
            # XML 파일 확인
            elif ('xml' in content_type or 
                  response.content.strip().startswith(b'<?xml')):
                print("XML 파일 감지")
                return response.content.decode('utf-8')
            
            else:
                print(f"알 수 없는 파일 형식: {content_type}")
                print("XBRL 뷰어 방식으로 전환...")
                return self.get_xbrl_from_viewer(rcept_no)
                
        except Exception as e:
            print(f"XBRL 다운로드 실패: {str(e)}")
            print("뷰어 방식으로 전환...")
            return self.get_xbrl_from_viewer(rcept_no)

    def extract_xbrl_from_zip(self, zip_content):
        """ZIP 파일에서 XBRL 데이터 추출"""
        try:
            with zipfile.ZipFile(io.BytesIO(zip_content)) as zip_ref:
                file_list = zip_ref.namelist()
                print(f"ZIP 파일 내용: {file_list}")
                
                # XBRL 파일 우선순위로 찾기
                xbrl_patterns = [
                    r'.*\.xbrl$',
                    r'.*xbrl.*\.xml$',
                    r'.*_financial.*\.xml$',
                    r'.*\.xml$'
                ]
                
                xbrl_file = None
                for pattern in xbrl_patterns:
                    matching_files = [f for f in file_list if re.match(pattern, f, re.IGNORECASE)]
                    if matching_files:
                        # 가장 큰 파일 선택
                        xbrl_file = max(matching_files, key=lambda x: zip_ref.getinfo(x).file_size)
                        print(f"XBRL 파일 선택: {xbrl_file}")
                        break
                
                if xbrl_file:
                    with zip_ref.open(xbrl_file) as f:
                        content = f.read()
                        # UTF-8 또는 UTF-8-BOM으로 디코딩 시도
                        try:
                            return content.decode('utf-8')
                        except UnicodeDecodeError:
                            try:
                                return content.decode('utf-8-sig')
                            except UnicodeDecodeError:
                                return content.decode('euc-kr')
                else:
                    raise ValueError("ZIP 파일에서 XBRL 파일을 찾을 수 없습니다.")
                    
        except Exception as e:
            print(f"ZIP 파일 처리 실패: {str(e)}")
            raise

    def get_xbrl_from_viewer(self, rcept_no):
        """XBRL 뷰어에서 데이터 추출 (대안)"""
        print(f"XBRL 뷰어 방식으로 데이터 추출 시도: {rcept_no}")
        
        try:
            # 다양한 XBRL 접근 URL 시도
            potential_urls = [
                f"https://opendart.fss.or.kr/xbrl/viewer/main.do?rcpNo={rcept_no}",
                f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}",
                f"https://opendart.fss.or.kr/api/xbrl/{rcept_no}.xml"
            ]
            
            session = requests.Session()
            session.headers.update({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            })
            
            for url in potential_urls:
                try:
                    print(f"시도 중: {url}")
                    response = session.get(url, timeout=30)
                    
                    if response.status_code == 200:
                        content_type = response.headers.get('content-type', '')
                        
                        # XML 응답인 경우
                        if 'xml' in content_type or response.text.strip().startswith('<?xml'):
                            print(f"XML 데이터 발견: {url}")
                            return response.text
                        
                        # HTML 뷰어 페이지인 경우 JavaScript 분석
                        elif 'html' in content_type:
                            print(f"HTML 뷰어 페이지 분석: {url}")
                            return self.extract_from_html_viewer(response.text, rcept_no)
                            
                except requests.RequestException as e:
                    print(f"URL {url} 실패: {str(e)}")
                    continue
            
            raise ValueError("모든 XBRL 접근 방법이 실패했습니다.")
            
        except Exception as e:
            print(f"XBRL 뷰어 추출 실패: {str(e)}")
            raise

    def extract_from_html_viewer(self, html_content, rcept_no):
        """HTML 뷰어에서 XBRL API 호출 정보 추출"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            scripts = soup.find_all('script')
            
            for script in scripts:
                if script.string and 'viewDoc' in script.string:
                    # viewDoc 함수 파라미터 추출
                    patterns = [
                        r'viewDoc\("([^"]+)"\s*,\s*"([^"]*)"\s*,\s*"([^"]*)"\s*,\s*"([^"]+)"\)',
                        r'viewDoc\(\'([^\']+)\'\s*,\s*\'([^\']*)\'\s*,\s*\'([^\']*)\'\s*,\s*\'([^\']+)\'\)'
                    ]
                    
                    for pattern in patterns:
                        match = re.search(pattern, script.string)
                        if match:
                            doc_id, param2, lang, doc_type = match.groups()
                            print(f"ViewDoc 파라미터: doc_id={doc_id}, lang={lang}, type={doc_type}")
                            return self.fetch_xbrl_data_from_api(doc_id, lang, doc_type)
            
            raise ValueError("viewDoc 함수 호출을 찾을 수 없습니다.")
            
        except Exception as e:
            print(f"HTML 뷰어 분석 실패: {str(e)}")
            raise

    def fetch_xbrl_data_from_api(self, doc_id, lang, doc_type):
        """API를 통해 XBRL 데이터 가져오기"""
        try:
            api_endpoints = [
                f"https://opendart.fss.or.kr/xbrl/viewer/data/{doc_id}?lang={lang}&type={doc_type}",
                f"https://opendart.fss.or.kr/xbrl/api/document/{doc_id}?lang={lang}&type={doc_type}",
                f"https://opendart.fss.or.kr/xbrl/data/{doc_id}.xml",
                f"https://dart.fss.or.kr/api/xbrl/{doc_id}.xml"
            ]
            
            session = requests.Session()
            session.headers.update({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Referer': f"https://opendart.fss.or.kr/xbrl/viewer/main.do?rcpNo={doc_id}",
                'Accept': 'application/xml, text/xml, */*'
            })
            
            for endpoint in api_endpoints:
                try:
                    print(f"API 호출: {endpoint}")
                    response = session.get(endpoint, timeout=30)
                    
                    if response.status_code == 200:
                        content_type = response.headers.get('content-type', '')
                        if ('xml' in content_type or 
                            response.text.strip().startswith('<?xml')):
                            print(f"XBRL API 성공: {endpoint}")
                            return response.text
                            
                except requests.RequestException as e:
                    print(f"API 호출 실패 {endpoint}: {str(e)}")
                    continue
            
            raise ValueError("모든 XBRL API 엔드포인트가 실패했습니다.")
            
        except Exception as e:
            print(f"XBRL API 호출 실패: {str(e)}")
            raise

    # 기존 메소드들 (parse_xbrl_data, extract_contexts 등)은 동일하게 유지
    def parse_xbrl_data(self, xbrl_content):
        """XBRL XML 데이터 파싱"""
        try:
            print("XBRL 데이터 파싱 시작...")
            root = ET.fromstring(xbrl_content)
            print("XML 파싱 성공")
            
            # 네임스페이스 자동 감지
            for prefix, uri in root.attrib.items():
                if prefix.startswith('xmlns:'):
                    ns_prefix = prefix[6:]
                    self.xbrl_namespaces[ns_prefix] = uri
                elif prefix == 'xmlns':
                    self.xbrl_namespaces['default'] = uri
            
            print(f"감지된 네임스페이스: {len(self.xbrl_namespaces)}개")
            
            parsed_data = {
                'contexts': self.extract_contexts(root),
                'financial_data': self.extract_financial_data(root),
                'company_info': self.extract_company_info(root),
                'metadata': self.extract_metadata(root)
            }
            
            print("XBRL 데이터 파싱 완료")
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
        context_elements = root.findall('.//xbrl:context', self.xbrl_namespaces)
        print(f"발견된 Context 수: {len(context_elements)}")
        
        for context in context_elements:
            context_id = context.get('id')
            
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
        
        key_items = {
            'Assets': ['ifrs-full:Assets', 'dart:Assets'],
            'Liabilities': ['ifrs-full:Liabilities', 'dart:Liabilities'],
            'Equity': ['ifrs-full:Equity', 'dart:Equity'],
            'CurrentAssets': ['ifrs-full:CurrentAssets', 'dart:CurrentAssets'],
            'NonCurrentAssets': ['ifrs-full:NoncurrentAssets', 'dart:NonCurrentAssets'],
            'CurrentLiabilities': ['ifrs-full:CurrentLiabilities', 'dart:CurrentLiabilities'],
            'Revenue': ['ifrs-full:Revenue', 'dart:Revenue'],
            'ProfitLoss': ['ifrs-full:ProfitLoss', 'dart:ProfitLoss'],
            'OperatingProfitLoss': ['ifrs-full:ProfitLossFromOperatingActivities', 'dart:OperatingIncomeLoss'],
            'GrossProfit': ['ifrs-full:GrossProfit', 'dart:GrossProfit']
        }
        
        total_elements_found = 0
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
                        total_elements_found += 1
                    
                    financial_data[item_name] = item_data
                    break
        
        print(f"추출된 재무 데이터 항목: {len(financial_data)}개, 총 요소: {total_elements_found}개")
        return financial_data

    def extract_company_info(self, root):
        """회사 정보 추출"""
        company_info = {}
        
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
        
        schemaRef = root.find('.//link:schemaRef', self.xbrl_namespaces)
        if schemaRef is not None:
            metadata['schema_location'] = schemaRef.get('{http://www.w3.org/1999/xlink}href')
        
        return metadata

    def update_dart_reports(self):
        """DART 보고서 데이터 업데이트 (XBRL 우선, HTML 폴백)"""
        start_date, end_date = self.get_recent_dates()
        report_list = self.dart.list(self.corp_code, start_date, end_date, kind='A', final='T')
        
        if not report_list.empty:
            print(f"발견된 보고서: {len(report_list)}개")
            
            for _, report in report_list.iterrows():
                print(f"\n📋 보고서 처리: {report['report_nm']} (접수번호: {report['rcept_no']})")
                
                try:
                    # XBRL 방식 시도
                    try:
                        print("🔄 XBRL 방식으로 처리 시도...")
                        xbrl_content = self.download_xbrl_data(report['rcept_no'])
                        parsed_xbrl = self.parse_xbrl_data(xbrl_content)
                        
                        if parsed_xbrl['financial_data']:
                            structured_data = self.convert_xbrl_to_structured_data(parsed_xbrl)
                            self.update_sheets_with_xbrl_data(structured_data, report['rcept_no'])
                            print(f"✅ XBRL 방식으로 처리 완료: {report['report_nm']}")
                            continue
                        else:
                            print("⚠️ XBRL에서 재무 데이터를 찾을 수 없음. HTML 방식으로 전환...")
                            
                    except Exception as xbrl_e:
                        print(f"❌ XBRL 방식 실패: {str(xbrl_e)}")
                        print("🔄 HTML 방식으로 전환...")
                    
                    # HTML 방식 폴백
                    try:
                        print("🔄 HTML 방식으로 처리...")
                        self.process_report_fallback(report['rcept_no'])
                        print(f"✅ HTML 방식으로 처리 완료: {report['report_nm']}")
                        
                    except Exception as html_e:
                        print(f"❌ HTML 방식도 실패: {str(html_e)}")
                        raise Exception(f"XBRL과 HTML 방식 모두 실패: XBRL={str(xbrl_e)[:100]}, HTML={str(html_e)[:100]}")
                        
                except Exception as e:
                    print(f"❌ 보고서 처리 완전 실패 ({report['rcept_no']}): {str(e)}")
                    continue
        else:
            print("📭 최근 3개월 내 새로운 보고서가 없습니다.")

    def convert_xbrl_to_structured_data(self, parsed_xbrl):
        """XBRL 데이터를 구조화된 형태로 변환"""
        structured_data = {
            'balance_sheet': {},
            'income_statement': {},
            'cash_flow': {},
            'company_info': parsed_xbrl.get('company_info', {}),
            'reporting_period': None
        }
        
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
            }
        }
        
        financial_data = parsed_xbrl.get('financial_data', {})
        
        for statement_type, mapping in financial_mapping.items():
            for xbrl_item, korean_name in mapping.items():
                if xbrl_item in financial_data and financial_data[xbrl_item]:
                    item_data = financial_data[xbrl_item][0]
                    value = item_data['value']
                    
                    if value:
                        try:
                            cleaned_value = re.sub(r'[,\s]', '', value)
                            numeric_value = float(cleaned_value)
                            
                            decimals = item_data.get('decimals')
                            if decimals and decimals.isdigit():
                                numeric_value = numeric_value / (10 ** int(decimals))
                            
                            structured_data[statement_type][korean_name] = numeric_value
                        except ValueError:
                            structured_data[statement_type][korean_name] = value
        
        return structured_data

    def update_sheets_with_xbrl_data(self, structured_data, rcept_no):
        """XBRL 구조화 데이터로 시트 업데이트"""
        try:
            financial_sheets = {
                '2. 연결재무제표': structured_data,
                '4. 재무제표': structured_data
            }
            
            for sheet_name, data in financial_sheets.items():
                try:
                    try:
                        worksheet = self.workbook.worksheet(sheet_name)
                    except gspread.exceptions.WorksheetNotFound:
                        worksheet = self.workbook.add_worksheet(sheet_name, 1000, 10)
                    
                    table_data = self.convert_to_table_format(data)
                    
                    if table_data:
                        worksheet.clear()
                        worksheet.append_rows(table_data)
                        print(f"✅ XBRL 데이터로 시트 업데이트 완료: {sheet_name}")
                    
                except Exception as sheet_e:
                    print(f"❌ 시트 {sheet_name} 업데이트 실패: {str(sheet_e)}")
                    continue
                    
        except Exception as e:
            print(f"❌ XBRL 데이터 시트 업데이트 실패: {str(e)}")
            raise

    def convert_to_table_format(self, structured_data):
        """구조화된 데이터를 테이블 형태로 변환"""
        table_data = []
        
        table_data.append(['구분', '항목', '금액', 'XBRL 출처'])
        
        if structured_data.get('balance_sheet'):
            table_data.append(['재무상태표', '', '', ''])
            for item, value in structured_data['balance_sheet'].items():
                table_data.append(['', item, str(value), 'XBRL'])
        
        if structured_data.get('income_statement'):
            table_data.append(['손익계산서', '', '', ''])
            for item, value in structured_data['income_statement'].items():
                table_data.append(['', item, str(value), 'XBRL'])
        
        if structured_data.get('cash_flow'):
            table_data.append(['현금흐름표', '', '', ''])
            for item, value in structured_data['cash_flow'].items():
                table_data.append(['', item, str(value), 'XBRL'])
        
        return table_data

    def process_report_fallback(self, rcept_no):
        """XBRL 실패 시 HTML 방식으로 폴백"""
        print(f"🔄 HTML 폴백 처리 시작: {rcept_no}")
        try:
            report_index = self.dart.sub_docs(rcept_no)
            target_docs = report_index[report_index['title'].isin(self.TARGET_SHEETS)]
            
            print(f"📑 처리할 문서 수: {len(target_docs)}")
            
            for _, doc in target_docs.iterrows():
                try:
                    print(f"📄 문서 처리: {doc['title']}")
                    self.update_worksheet_html(doc['title'], doc['url'])
                    print(f"✅ 문서 완료: {doc['title']}")
                except Exception as doc_e:
                    print(f"❌ 문서 처리 실패 {doc['title']}: {str(doc_e)}")
                    continue
                    
        except Exception as e:
            print(f"❌ HTML 폴백 처리 실패: {str(e)}")
            raise

    def update_worksheet_html(self, sheet_name, url):
        """HTML 방식 워크시트 업데이트"""
        try:
            try:
                worksheet = self.workbook.worksheet(sheet_name)
            except gspread.exceptions.WorksheetNotFound:
                worksheet = self.workbook.add_worksheet(sheet_name, 1000, 10)
            
            print(f"🌐 HTML 데이터 다운로드: {url}")
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            if response.status_code == 200:
                print(f"📊 HTML 콘텐츠 처리 시작...")
                self.process_html_content(worksheet, response.text)
                print(f"✅ HTML 시트 업데이트 완료: {sheet_name}")
            else:
                raise Exception(f"HTTP 오류: {response.status_code}")
                
        except Exception as e:
            print(f"❌ HTML 워크시트 업데이트 실패: {str(e)}")
            raise

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
            print("📱 텔레그램 설정이 없습니다.")
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
            print("📱 텔레그램 메시지 전송 완료")
        except Exception as e:
            print(f"📱 텔레그램 메시지 전송 실패: {str(e)}")

    def remove_parentheses(self, value):
        """괄호 내용 제거"""
        if not value:
            return value
        return re.sub(r'\s*\(.*?\)\s*', '', value).replace('%', '')

    def process_archive_data(self, archive, start_row, last_col):
        """아카이브 데이터 처리 (기존 로직 유지)"""
        try:
            print(f"📊 Archive 데이터 처리 시작: 행={start_row}, 열={last_col}")
            
            # 현재 시트 크기 확인
            current_cols = archive.col_count
            current_col_letter = self.get_column_letter(current_cols)
            target_col_letter = self.get_column_letter(last_col)
            
            print(f"현재 시트 열 수: {current_cols} ({current_col_letter})")
            print(f"대상 열: {last_col} ({target_col_letter})")
            
            # 필요한 경우 시트 크기 조정
            if last_col >= current_cols:
                new_cols = last_col + 5
                print(f"🔧 시트 크기 조정: {current_cols} → {new_cols}")
                archive.resize(rows=archive.row_count, cols=new_cols)
                time.sleep(2)
                print("✅ 시트 크기 조정 완료")

            # 데이터 수집
            all_rows = archive.get_all_values()
            update_data = []
            sheet_cache = {}
            
            # 처리할 행들 그룹화
            sheet_rows = {}
            processed_count = 0
            
            for row_idx in range(start_row - 1, len(all_rows)):
                if len(all_rows[row_idx]) < 5:
                    continue
                    
                sheet_name = all_rows[row_idx][0]
                if not sheet_name:
                    continue
                
                if sheet_name not in sheet_rows:
                    sheet_rows[sheet_name] = []
                    
                sheet_rows[sheet_name].append({
                    'row_idx': row_idx + 1,
                    'keyword': all_rows[row_idx][1],
                    'n': all_rows[row_idx][2],
                    'x': all_rows[row_idx][3],
                    'y': all_rows[row_idx][4]
                })
                processed_count += 1
            
            print(f"📋 처리할 시트 수: {len(sheet_rows)}, 총 행 수: {processed_count}")
            
            # 시트별 데이터 처리
            for sheet_name, rows in sheet_rows.items():
                try:
                    print(f"\n🔍 시트 '{sheet_name}' 처리 중 (항목: {len(rows)}개)")
                    
                    # 시트 데이터 캐싱
                    if sheet_name not in sheet_cache:
                        try:
                            search_sheet = self.workbook.worksheet(sheet_name)
                            sheet_data = search_sheet.get_all_values()
                            df = pd.DataFrame(sheet_data)
                            sheet_cache[sheet_name] = df
                            print(f"✅ 시트 데이터 로드: {df.shape}")
                        except gspread.exceptions.WorksheetNotFound:
                            print(f"⚠️ 시트 '{sheet_name}' 없음. 건너뜀.")
                            continue
                    
                    df = sheet_cache[sheet_name]
                    
                    # 각 행의 키워드 검색
                    for row in rows:
                        try:
                            keyword = row['keyword']
                            if not all([keyword, row['n'], row['x'], row['y']]):
                                continue
                            
                            n, x, y = int(row['n']), int(row['x']), int(row['y'])
                            
                            # 키워드 위치 검색
                            keyword_positions = []
                            for idx, df_row in df.iterrows():
                                for col_idx, value in enumerate(df_row):
                                    if str(value).strip() == keyword.strip():
                                        keyword_positions.append((idx, col_idx))
                            
                            if keyword_positions and len(keyword_positions) >= n:
                                target_pos = keyword_positions[n - 1]
                                target_row = target_pos[0] + y
                                target_col = target_pos[1] + x
                                
                                if (0 <= target_row < df.shape[0] and 
                                    0 <= target_col < df.shape[1]):
                                    value = df.iat[target_row, target_col]
                                    cleaned_value = self.remove_parentheses(str(value))
                                    update_data.append((row['row_idx'], cleaned_value))
                                    print(f"✅ 값 발견: {keyword} → {cleaned_value}")
                                else:
                                    print(f"⚠️ 범위 초과: {keyword}")
                            else:
                                print(f"⚠️ 키워드 미발견: {keyword}")
                                
                        except Exception as row_e:
                            print(f"❌ 행 처리 오류: {str(row_e)}")
                            continue
                
                except Exception as sheet_e:
                    print(f"❌ 시트 '{sheet_name}' 처리 오류: {str(sheet_e)}")
                    continue
            
            print(f"\n📊 업데이트할 데이터: {len(update_data)}개")
            
            # 데이터 업데이트
            if update_data:
                self.update_archive_column(archive, update_data, target_col_letter, last_col)
                
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
                print(f"✅ 메타데이터 업데이트 완료 (분기: {quarter_text})")
                
                # 텔레그램 알림
                message = (
                    f"🔄 DART 업데이트 완료\n\n"
                    f"• 종목: {self.company_name} ({self.corp_code})\n"
                    f"• 분기: {quarter_text}\n"
                    f"• 업데이트 일시: {today.strftime('%Y-%m-%d %H:%M:%S')}\n"
                    f"• 처리된 행: {len(update_data)}개\n"
                    f"• 시트 열: {target_col_letter} (#{last_col})"
                )
                self.send_telegram_message(message)
                
            else:
                print("⚠️ 업데이트할 데이터가 없습니다.")
                
        except Exception as e:
            error_msg = f"Archive 데이터 처리 중 오류: {str(e)}"
            print(f"❌ {error_msg}")
            self.send_telegram_message(f"❌ {error_msg}")
            raise

    def update_archive_column(self, archive, update_data, target_col_letter, last_col):
        """Archive 열 데이터 업데이트"""
        try:
            min_row = min(row for row, _ in update_data)
            max_row = max(row for row, _ in update_data)
            
            # 업데이트할 데이터 준비
            column_data = [''] * (max_row - min_row + 1)
            for row, value in update_data:
                adjusted_row = row - min_row
                column_data[adjusted_row] = value
            
            # 2D 배열로 변환 (Google Sheets API 요구사항)
            column_data_2d = [[value] for value in column_data]
            
            range_label = f'{target_col_letter}{min_row}:{target_col_letter}{max_row}'
            print(f"📝 업데이트 범위: {range_label}")
            
            archive.batch_update([{
                'range': range_label,
                'values': column_data_2d
            }])
            
            print(f"✅ 컬럼 업데이트 완료: {min_row}~{max_row} 행")
            
        except Exception as e:
            print(f"❌ 컬럼 업데이트 실패: {str(e)}")
            raise


def main():
    """메인 실행 함수"""
    try:
        import sys
        
        def log(msg):
            print(f"🤖 {msg}")
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
            log("📋 DART 보고서 업데이트 시작...")
            updater.update_dart_reports()
            log("✅ DART 보고서 업데이트 완료")
            
            # Archive 시트 처리
            log("📊 Archive 시트 업데이트 시작...")
            archive = updater.workbook.worksheet('Dart_Archive')
            
            sheet_values = archive.get_all_values()
            if not sheet_values:
                raise ValueError("Dart_Archive 시트가 비어있습니다")
            
            last_col = len(sheet_values[0])
            control_value = archive.cell(1, last_col).value
            start_row = 10
            
            if control_value:
                last_col += 1
            
            log(f"Archive 처리: 시작행={start_row}, 대상열={last_col}")
            updater.process_archive_data(archive, start_row, last_col)
            log("✅ Archive 시트 업데이트 완료")
            
            log("🎉 전체 작업 완료!")
            
        except Exception as e:
            log(f"❌ 처리 중 오류 발생: {str(e)}")
            if 'updater' in locals():
                updater.send_telegram_message(
                    f"❌ XBRL DART 업데이트 실패\n\n"
                    f"• 종목: {COMPANY_INFO['name']} ({COMPANY_INFO['code']})\n"
                    f"• 오류: {str(e)}"
                )
            raise

    except Exception as e:
        print(f"💥 전체 작업 중 치명적 오류: {str(e)}")
        print(f"🔍 오류 타입: {type(e).__name__}")
        raise e

if __name__ == "__main__":
    main()
