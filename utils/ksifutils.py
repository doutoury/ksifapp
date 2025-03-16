
import requests
import json
import re

from datetime import datetime 
from dateutil.relativedelta import relativedelta

import zipfile
import xmltodict
from io import BytesIO

import pandas as pd
import numpy as np

import exchange_calendars as xcals
import yfinance as yf 
from pykrx import stock

import pdb


# 날짜 비교용 변수
today_dt = datetime.now().date()


### 유틸 functions 


# 자주 쓰이는 날짜 계산 함수. (위로 올리기)
def get_date_ago(date_str): 
    day_ago_dt = datetime.strptime(str(date_str), '%Y%m%d').date() - relativedelta(days=1) 
    return day_ago_dt.strftime('%Y%m%d') 


# 자주 쓰이는 날짜 계산 함수. (위로 올리기) 
def get_date_delta(sdt, edt): 
    last_dt = datetime.strptime(str(edt), '%Y%m%d').date() 
    start_dt = datetime.strptime(str(sdt), '%Y%m%d').date() 
    # sdt ~ edt 사이 남은 기간 계산
    delta = last_dt - start_dt 
    return delta 

# 자주 쓰이는 데이터프레임 생성 함수. 

def get_business_days(mkt, sdt, edt=None, ascending=True): 
    """
    "XKRX" : Korea Exchange
    "XNYS" : New York Stock Exchange
    "XCBF" : CBOE Futures
    "CMES" : Chicago Mercantile Exchange
    "IEPA" : ICE US
    ...
    https://github.com/gerrymanoim/exchange_calendars
    """
    
    if edt is None: 
        edt = today_dt
    
    XKRX = xcals.get_calendar(mkt)
    openday_df = pd.DataFrame(XKRX.schedule.loc[str(sdt):str(edt)])
    openday_df = openday_df.reset_index().sort_index(ascending=False)
    openday_df.rename(columns={'index': '영업일자'}, inplace=True)    # 'market_open' 가 'index' 로 바꼈나? 
    opendays = openday_df['영업일자'].apply(lambda x: x.strftime('%Y%m%d')) # 사용편의 위해 str 으로 변환
    if ascending == True: 
        opendays = opendays.sort_index(ascending=True)
    return opendays.values


# DART API 계정 인증키 (나중에 학교 계정으로 변경)
def get_dart_crtfc_key(): 
    _crtfc_key = '******'
    return _crtfc_key

# DART API 회사고유번호(corporate code) 조회
def get_dart_corpcode(tickers): 
    if type(tickers) is str: 
        tickers = [tickers]

    api = 'https://opendart.fss.or.kr/api/corpCode.xml'
    res = requests.get(api, params={
        'crtfc_key': get_dart_crtfc_key(), 
        })
    
    data_xml = zipfile.ZipFile(BytesIO(res.content))
    data_xml = data_xml.read('CORPCODE.xml').decode('utf-8')
    data_odict = xmltodict.parse(data_xml)
    data_dict = json.loads(json.dumps(data_odict))
    data = data_dict.get('result', {}).get('list')
    
    stock_list = []
    for item in data: 
        if item['stock_code'] in tickers: 
            stock_list.append(item)
    
    stock_list_df = pd.DataFrame(stock_list)
    stock_list_df = stock_list_df.rename(columns={'corp_code': '고유번호', 
                                                  'corp_name': '회사명', 
                                                  'stock_code': '종목코드', 
                                                  'modify_date': '변경일자'})
    return stock_list_df


def chg_dartnum_format(df: pd.DataFrame, list): 
    for col in list: 
        df[col] = df[col].apply(lambda x: np.NaN if x=='-' 
                                else float(x.replace(',', '')))


def chg_dartdate_format(df: pd.DataFrame, list): 
    for col in list: 
        df[col] = df[col].apply(lambda x: np.NaN if x=='-' 
                                else re.sub('[^0-9]', '', x)) 


# DART API 로 무상증자 (Capital increase with compensation) 정보 요청하는 함수 
def get_dart_fricdecsn(sdt, edt, tickers): 
    api = 'https://opendart.fss.or.kr/api/fricDecsn.json'
    res = requests.get(api, params={'crtfc_key': get_dart_crtfc_key(), 
                                    'corp_code' : get_dart_corpcode(tickers)['고유번호'], 
                                    'bgn_de' : str(sdt), 
                                    'end_de' : str(edt), 
                                    })
    
    col_names = {'rcept_no': '접수번호', 
                 'corp_cls': '법인구분',
                 'corp_code': '고유번호',
                 'corp_name': '회사명',
                 'nstk_ostk_cnt': '무상증자 신주수(보통주)', 
                 'nstk_estk_cnt': '무상증자 신주수(기타주)',
                 'fv_ps': '주당액면가액(원)',
                 'bfic_tisstk_ostk': '증자전 주식수(보통주)',
                 'bfic_tisstk_estk': '증자전 주식수(기타주)',
                 'nstk_asstd': '신주배정 기준일', 
                 'nstk_ascnt_ps_ostk': '주당 신주배정 주식수(보통주)', 
                 'nstk_ascnt_ps_estk': '주당 신주배정 주식수(기타주)',
                 'nstk_dividrk': '신주 배당기산일', 
                 'nstk_dlprd': '신주권교부 예정일',
                 'nstk_lstprd': '신주상장 예정일',
                 'bddd': '이사회결의일', 
                 'od_a_at_t': '사외이사 참석인원', 
                 'od_a_at_b': '사외이사 불참인원', 
                 'adt_a_atn': '감사위원 참석여부'}
    
    num_format_chg_list = ['무상증자 신주수(보통주)', 
                           '무상증자 신주수(기타주)', 
                           '증자전 주식수(보통주)', 
                           '증자전 주식수(기타주)', 
                           '주당 신주배정 주식수(보통주)', 
                           '주당 신주배정 주식수(기타주)', 
                           '주당액면가액(원)'] 
    date_format_chg_list = ['신주상장 예정일', 
                            '신주배정 기준일', 
                            '신주 배당기산일', 
                            '신주권교부 예정일', 
                            '이사회결의일'] 
    
    try: 
        data_df = pd.DataFrame(json.loads(res.text)['list'])
        data_df = data_df.rename(columns=col_names)
        chg_dartnum_format(data_df, num_format_chg_list)
        chg_dartdate_format(data_df, date_format_chg_list)
        new_data = data_df[['신주상장 예정일', '신주배정 기준일', '법인구분', '고유번호', '회사명', '주당 신주배정 주식수(보통주)', '무상증자 신주수(보통주)', '증자전 주식수(보통주)']]
        stock_list_df = get_dart_corpcode(tickers)
        joint_dataframe = new_data.merge(stock_list_df[['고유번호', '종목코드']], on=['고유번호'], how='left')
        # 무상증자 비율 = (신주의 종류와 수 'nstk_ostk_cnt' / 증자전 발행주식총수 'bfic_tisstk_ostk')
        # 무상주 입고수 = (신주의 종류와 수 'nstk_ostk_cnt' / 증자전 발행주식총수 'bfic_tisstk_ostk') * 보유고수  
        return joint_dataframe
    except: 
        new_data = pd.DataFrame(columns=['신주상장 예정일', '신주배정 기준일', '법인구분', '고유번호', '회사명', '주당 신주배정 주식수(보통주)', '무상증자 신주수(보통주)', '증자전 주식수(보통주)'])
        # data_df = data_df.rename(columns=col_names)
        # new_data = data_df[['신주상장 예정일', '신주배정 기준일', '법인구분', '고유번호', '회사명', '주당 신주배정 주식수(보통주)', '무상증자 신주수(보통주)', '증자전 주식수(보통주)']]
        stock_list_df = get_dart_corpcode(tickers)
        joint_dataframe = new_data.merge(stock_list_df[['고유번호', '종목코드']], on=['고유번호'], how='left')
        return joint_dataframe


# DART API 로 무상감자 (Capital reduction with compensation) 정보 요청하는 함수 
def get_dart_crdecsn(sdt, edt, tickers): 
    api = 'https://opendart.fss.or.kr/api/crDecsn.json'
    res = requests.get(api, params={'crtfc_key': get_dart_crtfc_key(), 
                                    'corp_code' : get_dart_corpcode(tickers)['고유번호'], 
                                    'bgn_de' : str(sdt), 
                                    'end_de' : str(edt), 
                                    })
    
    col_names = {'rcept_no': '접수번호', 
                 'corp_cls': '법인구분',
                 'corp_code': '고유번호',
                 'corp_name': '회사명',
                 'crstk_ostk_cnt': '무상감자 감소수(보통주)', 
                 'crstk_estk_cnt': '무상감자 감소수(기타주)',
                 'fv_ps': '주당액면가액(원)',
                 'bfcr_cpt': '감자전 자본금(원)', 
                 'atcr_cpt': '감자후 자본금(원)', 
                 'bfcr_tisstk_ostk': '감자전 주식수(보통주)',
                 'atcr_tisstk_ostk': '감자후 주식수(보통주)',
                 'bfcr_tisstk_estk': '감자전 주식수(기타주)', 
                 'atcr_tisstk_estk': '감자후 주식수(기타주)', 
                 'cr_rt_ostk': '감자비율%(보통주)', 
                 'cr_rt_estk': '감자비율%(기타주)', 
                 'cr_std': '감자 기준일', 
                 'cr_mth': '감자방법', 
                 'cr_rs': '감자사유', 
                 'crsc_gmtsck_prd': '주주총회 예정일', 
                 'crsc_trnmsppd': '명의개서 정지기간', 
                 'crsc_osprpd_bgd': '구주권 제출기간(시작일)', 
                 'crsc_osprpd_edd': '구주권 제출기간(종료일)', 
                 'crsc_trspprpd_bgd': '매매거래 정지예정기간(시작일)', 
                 'crsc_trspprpd_edd': '매매거래 정지예정기간(종료일)', 
                 'crsc_nstkdlprd': '신주권교부 예정일', 
                 'crsc_nstklstprd': '신주상장 예정일', 
                 'cdobprpd_bgd': '채권자 이의제출기간(시작일)',
                 'cdobprpd_edd': '채권자 이의제출기간(종료일)', 
                 'ospr_nstkdl_pl': '구주권제출 및 신주권교부장소', 
                 'ftc_stt_atn': '이사회 경의일', 
                 'od_a_at_t': '사외이사 참석인원', 
                 'od_a_at_b': '사외이사 불참인원', 
                 'adt_a_atn': '감사위원 참석여부', 
                 'bddd': '공정거래위원회 신고대상 여부'}

    num_format_chg_list = ['무상감자 감소수(보통주)', 
                           '무상감자 감소수(기타주)', 
                           '주당액면가액(원)', 
                           '감자전 자본금(원)', 
                           '감자후 자본금(원)', 
                           '감자전 주식수(보통주)', 
                           '감자후 주식수(보통주)', 
                           '감자전 주식수(기타주)', 
                           '감자후 주식수(기타주)', 
                           '감자비율%(보통주)', 
                           '감자비율%(기타주)']
    
    date_format_chg_list = ['감자 기준일', 
                            '주주총회 예정일', 
                            '명의개서 정지기간', 
                            '구주권 제출기간(시작일)', 
                            '구주권 제출기간(종료일)', 
                            '매매거래 정지예정기간(시작일)', 
                            '매매거래 정지예정기간(종료일)', 
                            '신주권교부 예정일', 
                            '신주상장 예정일', 
                            '채권자 이의제출기간(시작일)', 
                            '채권자 이의제출기간(종료일)', 
                            '이사회 경의일'] 

    try: 
        data_df = pd.DataFrame(json.loads(res.text)['list'])
        data_df = data_df.rename(columns=col_names)
        chg_dartnum_format(data_df, num_format_chg_list)
        chg_dartdate_format(data_df, date_format_chg_list)
        new_data = data_df[['신주상장 예정일', '감자 기준일', '법인구분', '고유번호', '회사명', '감자비율%(보통주)', '감자비율%(기타주)', '감자방법', '감자사유']] 
        stock_list_df = get_dart_corpcode(tickers)
        joint_dataframe = new_data.merge(stock_list_df[['고유번호', '종목코드']], on=['고유번호'], how='left')
        return joint_dataframe
    except: 
        new_data = pd.DataFrame(columns=['신주상장 예정일', '감자 기준일', '법인구분', '고유번호', '회사명', '감자비율%(보통주)', '감자비율%(기타주)', '감자방법', '감자사유'])
        stock_list_df = get_dart_corpcode(tickers)
        joint_dataframe = new_data.merge(stock_list_df[['고유번호', '종목코드']], on=['고유번호'], how='left')
        return joint_dataframe



# DART API 로 유상증자 (유상감자) 조회하면 데이터 없음 나옴. 


# KRX 에서 무상증자(bonus event)에 대해서만 조회 가능 (안쓰지만 함수 짜 놓음)
def krx_issue_events(sdt, edt, tickers): 
    issue_events_df = get_dart_fricdecsn(sdt, edt, tickers)
    issue_events_df.rename(columns={'신주상장 예정일': '영업일자', 
                                    '주당 신주배정 주식수(보통주)': '무상증자비율'}, 
                                    inplace=True)
    issue_events_df = issue_events_df.set_index(['종목코드', '영업일자'])[['신주배정 기준일', '무상증자비율']]
    # MultiIndex sorting 반영 고민 
    return issue_events_df



# 한국 
# 나중에 배당처리할 때 배당정보 어떻게 받아와서 처리할지 고민 필요 
def krx_split_events(tickers): 
    if type(tickers) is str: 
        tickers = [tickers]

    split_events_df = pd.DataFrame()
    for ticker in tickers: 
        event_df = stock.get_stock_major_changes(str(ticker))
        before_event = event_df['액면변경전'][event_df['액면변경전'] != 0].astype(float)
        after_event = event_df['액면변경후'][event_df['액면변경후'] != 0].astype(float)

        split_events = pd.DataFrame(before_event / after_event, columns=['액면분할비율'])
        split_events.index = split_events.index.date
        split_events.index = split_events.index.map(lambda x: x.strftime('%Y%m%d'))
        split_events.index = pd.MultiIndex.from_arrays([[ticker for i in range(split_events.shape[0])], split_events.index], names=('종목코드', '영업일자'))
        split_events_df = pd.concat([split_events_df, split_events])
    return split_events_df



# 미국 
# 그 외 국가 국가별로 Ticker 뒤에 나라코드 string 추가 처리 필요 
# 나중에 배당처리할 때 yf.Ticker(ticker).actions 의 ['Dividends'] 칼럼 활용 
def yf_split_event(ticker): 
    event_df = yf.Ticker(ticker).actions.rename(columns={'Dividends': '배당금', 'Stock Splits': '액면분할비율'})
    
    split_events = event_df.drop(event_df[(event_df['액면분할비율'] == 0)].index)[['액면분할비율']]
    split_events.index = split_events.index.date
    split_events.index = split_events.index.map(lambda x: x.strftime('%Y%m%d'))
    split_events.index = pd.MultiIndex.from_arrays([[ticker for i in range(split_events.shape[0])], split_events.index], names=('종목코드', '영업일자'))
    return split_events


# 환율 정보 가져오는 함수 
def yf_fxrate(sdt, edt=today_dt, ticker="KRW=X"): 
    fxrate_df = yf.download(ticker, start=sdt, end=edt) 
    fxrate_df = fxrate_df[['Close']]
    fxrate_df.columns = [ticker]
    fxrate_df.index.names = ['영업일자']
    return fxrate_df

# 한국수출입은행 환율 Open API 데이터 확인 ! (인증 Key 필요)
# https://www.koreaexim.go.kr/ir/HPHKIR020M01?apino=2&viewtype=C&searchselect=&searchword=

# KEB 하나은행 환율 Open API (인증 Key 불필요)
# https://www.hanafnapimarket.com/#/apis/guide?apiId=hbk00004 

# 한국은행 API 에서 환율 API 
# https://yenpa.tistory.com/93 