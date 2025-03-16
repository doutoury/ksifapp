# -*- coding: utf-8 -*-
"""
Created on Mon Apr 18 11:23:04 2022 
@author: KIS Developers 
"""

import time, copy
import yaml
import requests
import json
import pdb
import re

import pandas as pd
import numpy as np

from collections import namedtuple 
from datetime import datetime 
from dateutil.relativedelta import relativedelta 
import exchange_calendars as xcals 

# import pykrx 
# import yfinance as yf 

# import zipfile
# import xmltodict
# from io import BytesIO

# from KIS_OpenAPI.rest.kis_api_base import * 
# from KIS_OpenAPI.rest.kis_utils import * 
# Set 'ksifapp' as a name of the app folder.
from ksifapp.utils.ksifutils import *
from ksifapp.utils.kisapi import KISReq


# 시장 개장일/휴장일 확인용 xcals 객체 
XKRX = xcals.get_calendar("XKRX") # 한국 코드 
NYSE = xcals.get_calendar("NYSE") # 뉴욕증시 코드 



### 비즈니스 로직 파트

class KSIF(KISReq): 

    def __init__(self, team, asset='stock', svr='prod'): 
        
        self.team = team 
        self.asset = asset
        self.svr = svr

        super().__init__()
        super().auth(self.team, asset, svr)
    
    # 계좌 잔고를 DataFrame 으로 반환
    # Input: None (Option) rtCashFlag=True 면 예수금 총액을 반환하게 된다
    # Output: DataFrame (Option) rtCashFlag=True 면 예수금 총액을 반환하게 된다
    # 참고. 기존 있던 rtCashFlag 인자는 output2 에서 예수금 내용 확인용
    # 국내주식잔고조회 : 최대 50건 (이후 연속조회 필요) 
    # 해외주식잔고조회 : 최대 100건 (이후 연속조회 필요) 
    def get_acct_balance(self, output='output1'):
        url = '/uapi/domestic-stock/v1/trading/inquire-balance'
        tr_id = "TTTC8434R"

        # 당일계좌조회 (원본 코드)
        # 나중에 if 문 없애기 (오늘자 조회 / 과거 조회 코드 분리)
        # if date == None:

        params = {
            'CANO': super().getTREnv().my_acct[:8],         # 주식잔고조회API Request 의 Query Parameter 중 'CANO'로 계좌번호 앞자리 8자리만 들어가야되므로 [:8] 붙여서 수정
            'ACNT_PRDT_CD': '01', 
            'AFHR_FLPR_YN': 'N', 
            'FNCG_AMT_AUTO_RDPT_YN': 'N', 
            'FUND_STTL_ICLD_YN': 'N', 
            'INQR_DVSN': '01', 
            'OFL_YN': 'N', 
            'PRCS_DVSN': '01', 
            'UNPR_DVSN': '01', 
            'CTX_AREA_FK100': '', 
            'CTX_AREA_NK100': ''
            }

        t1 = super().url_fetch(url, tr_id, params)

        output1 = t1.getBody().output1
        output2 = t1.getBody().output2

        if t1.isOK() and len(output1) > 0:  # body 의 rt_cd 가 0 인 경우만 성공

            tdf1 = pd.DataFrame(output1)
            # tdf1.set_index('pdno', inplace=True)  
            cf1 = ['pdno', 'prdt_name','hldg_qty', 'ord_psbl_qty', 'pchs_avg_pric', 'evlu_pfls_rt', 'prpr', 'bfdy_cprs_icdc', 'fltt_rt']
            cf2 = ['종목코드', '상품명', '보유수량', '매도가능수량', '매입평균가격', '평가손익률', '현재가' ,'전일대비증감', '등락율']
            tdf1 = tdf1[cf1]
            tdf1[cf1[2:]] = tdf1[cf1[2:]].apply(pd.to_numeric)
            ren_dict = dict(zip(cf1, cf2))
            tdf1 = tdf1.rename(columns=ren_dict)

            tdf2 = pd.DataFrame(output2, index=[0])
            # tdf2.set_index('pdno', inplace=True)  
            cf1 = ['dnca_tot_amt', 'thdt_tlex_amt', 'scts_evlu_amt', 'tot_evlu_amt', 'nass_amt']
            cf2 = ['예수금', '제비용금액', '유가평가금액', '총평가금액', '순자산금액']
            tdf2 = tdf2[cf1]
            tdf2[cf1] = tdf2[cf1].apply(pd.to_numeric)
            ren_dict2 = dict(zip(cf1, cf2))
            tdf2 = tdf2.rename(columns=ren_dict2)

        else:
            t1.printError()
            return pd.DataFrame()

        # 종목코드를 index 할지는 메서드 활용 기능 보고 결정 !
        # tdf1.set_index('종목코드', inplace=True)

        if output == 'output1': 
            return tdf1 
        elif output == 'output2': 
            return tdf2 
        else: 
            print("Error: output 인자에 잘못된 값을 넣었습니다. ('output1' 또는 'output2' 입력)")


    # 해외주식잔고조회 (미국) 
    def get_acct_balance_us(self, output='output1'):
        url = '/uapi/overseas-stock/v1/trading/inquire-balance'
        tr_id = "TTTS3012R"

        params = {
            'CANO': super().getTREnv().my_acct[:8],         # 주식잔고조회API Request 의 Query Parameter 중 'CANO'로 계좌번호 앞자리 8자리만 들어가야되므로 [:8] 붙여서 수정
            'ACNT_PRDT_CD': '01', 
            'OVRS_EXCG_CD': 'NASD',     ### 해외거래소코드 
                                        ### [모의] NASD: 나스닥, NYSE: 뉴욕, AMEX: 아멕스 
                                        ### [실전] NASD: 미국전체, NAS: 나스닥, NYSE: 뉴욕, AMEX: 아멕스
            'TR_CRCY_CD': 'USD',        ### 거래통화코드
            'CTX_AREA_FK200': '',       ### CTX_AREA_FK100 -> CTX_AREA_FK200 
            'CTX_AREA_NK200': ''        ### CTX_AREA_NK100 -> CTX_AREA_NK200 
            }

        t1 = super().url_fetch(url, tr_id, params)
        # pdb.set_trace()
        output1 = t1.getBody().output1
        output2 = t1.getBody().output2

        if t1.isOK() and len(output1) > 0:  # body 의 rt_cd 가 0 인 경우만 성공

            tdf1 = pd.DataFrame(output1)
            # tdf1.set_index('pdno', inplace=True)  
            ### pdno -> ovrs_pdno, prdt_name -> ovrs_item_name, hldg_qty -> ovrs_cblc_qty, prpr -> now_pric2, bfdy_cprs_icdc -> ovrs_stck_evlu_amt, fltt_rt -> ovrs_excg_cd
            cf1 = ['ovrs_pdno', 'ovrs_item_name','ovrs_cblc_qty', 'ord_psbl_qty', 'pchs_avg_pric', 'evlu_pfls_rt', 'now_pric2', 'ovrs_stck_evlu_amt', 'ovrs_excg_cd']
            cf2 = ['종목코드', '상품명', '보유수량', '매도가능수량', '매입평균가격', '평가손익률', '현재가' ,'평가금액', '거래소구분코드']
            tdf1 = tdf1[cf1]
            tdf1[cf1[2:8]] = tdf1[cf1[2:8]].apply(pd.to_numeric)    # '거래소코드' 는 not numeric! 
            ren_dict = dict(zip(cf1, cf2))
            tdf1 = tdf1.rename(columns=ren_dict)

            tdf2 = pd.DataFrame(output2, index=[0])
            # tdf2.set_index('pdno', inplace=True)  
            ### output2 는 전부 다 바꿈 
            cf1 = ['frcr_pchs_amt1', 'ovrs_rlzt_pfls_amt', 'ovrs_tot_pfls', 'rlzt_erng_rt', 'tot_evlu_pfls_amt', 'tot_pftrt', 'frcr_buy_amt_smtl1', 'ovrs_rlzt_pfls_amt2', 'frcr_buy_amt_smtl2']
            cf2 = ['외화매입금액', '해외실현손익금액', '해외총손익', '실현수익율', '총평가손익금액', '총수익률', '외화매수금액합계1', '해외실현손익금액2', '외화매수금액합계2']
            tdf2 = tdf2[cf1]
            tdf2[cf1] = tdf2[cf1].apply(pd.to_numeric)
            ren_dict2 = dict(zip(cf1, cf2))
            tdf2 = tdf2.rename(columns=ren_dict2)

        else:
            t1.printError()
            return pd.DataFrame()

        # 종목코드를 index 할지는 메서드 활용 기능 보고 결정 !
        # tdf1.set_index('종목코드', inplace=True)

        if output == 'output1': 
            return tdf1 
        elif output == 'output2': 
            return tdf2 
        else: 
            print("Error: output 인자에 잘못된 값을 넣었습니다. ('output1' 또는 'output2' 입력)")



    # 해외주식잔고조회 (미국) 
    def get_acct_balance_us_2(self, output='output1'):
        url = '/uapi/overseas-stock/v1/trading/inquire-present-balance'
        tr_id = "CTRP6504R"

        params = {
            'CANO': super().getTREnv().my_acct[:8],         # 주식잔고조회API Request 의 Query Parameter 중 'CANO'로 계좌번호 앞자리 8자리만 들어가야되므로 [:8] 붙여서 수정
            'ACNT_PRDT_CD': '01', 
            'WCRC_FRCR_DVSN_CD': '01',  ### 원화외화구분코드 (01: 원화, 02: 외화) 
            'NATN_CD': '000',           ### 국가코드 (000: 전체)
            'TR_MKET_CD': '00',         ### 거래시장코드 (00: 전체)
            'INQR_DVSN_CD': '00',       ### 조회구분코드 (00: 전체)
            }

        t1 = super().url_fetch(url, tr_id, params)
        # pdb.set_trace()
        output1 = t1.getBody().output1
        output2 = t1.getBody().output2
        output3 = t1.getBody().output3

        if t1.isOK() and len(output1) > 0:  # body 의 rt_cd 가 0 인 경우만 성공

            # output 1 : 종목별 잔고 
            tdf1 = pd.DataFrame(output1)
            ### pdno, prdt_name, cblc_qty13, prpr -> now_pric2, bfdy_cprs_icdc -> ovrs_stck_evlu_amt, fltt_rt -> ovrs_excg_cd
            cf1 = ['pdno', 'prdt_name','cblc_qty13', 'ord_psbl_qty1', 'avg_unpr3', 'evlu_pfls_rt1', 'ovrs_now_pric1', 'frcr_evlu_amt2', 'ovrs_excg_cd']
            cf2 = ['종목코드', '상품명', '보유수량', '매도가능수량', '매입평균가격', '평가손익률', '현재가' ,'평가금액', '거래소구분코드']
            tdf1 = tdf1[cf1]
            tdf1[cf1[2:8]] = tdf1[cf1[2:8]].apply(pd.to_numeric)    # '거래소코드' 는 not numeric! 
            ren_dict = dict(zip(cf1, cf2))
            tdf1 = tdf1.rename(columns=ren_dict)

            # output 2 : 외화 기준 총잔고 ??? 
            tdf2 = pd.DataFrame(output2, index=[0])
            cf1 = ['crcy_cd', 'crcy_cd_name', 'frcr_dncl_amt_2', 'frcr_buy_mgn_amt', 'frcr_etc_mgna', 'frcr_drwg_psbl_amt_1', 'frcr_evlu_amt2']
            cf2 = ['통화코드', '통화코드명', '외화예수금', '외화매수증거금', '외화기타증거금', '외화출금가능금액', '원화출금가능금액']
            tdf2 = tdf2[cf1]
            tdf2[cf1[2:]] = tdf2[cf1[2:]].apply(pd.to_numeric)
            ren_dict2 = dict(zip(cf1, cf2))
            tdf2 = tdf2.rename(columns=ren_dict2)

            # output3 : 원화 기준 총잔고인가 ??? 
            tdf3 = pd.DataFrame(output3, index=[0])
            cf1 = ['evlu_amt_smtl', 'dncl_amt', 'tot_dncl_amt', 'mgna_tota', 'frcr_evlu_tota', 'tot_asst_amt', 'tot_frcr_cblc_smtl']
            cf2 = ['평가금액합계', '예수금액', '총예수금액', '증거금총액', '외화평가총액', '총자산금액', '총외화잔고합계']
            # cf1 = ['dnca_tot_amt', 'thdt_tlex_amt', 'scts_evlu_amt', 'tot_evlu_amt', 'nass_amt']
            # cf2 = ['예수금', '제비용금액', '유가평가금액', '총평가금액', '순자산금액']
            # cf1 = ['frcr_pchs_amt1', 'ovrs_rlzt_pfls_amt', 'ovrs_tot_pfls', 'rlzt_erng_rt', 'tot_evlu_pfls_amt', 'tot_pftrt', 'frcr_buy_amt_smtl1', 'ovrs_rlzt_pfls_amt2', 'frcr_buy_amt_smtl2']
            # cf2 = ['외화매입금액', '해외실현손익금액', '해외총손익', '실현수익율', '총평가손익금액', '총수익률', '외화매수금액합계1', '해외실현손익금액2', '외화매수금액합계2']
            tdf3 = tdf3[cf1]
            tdf3[cf1] = tdf3[cf1].apply(pd.to_numeric)
            ren_dict3 = dict(zip(cf1, cf2))
            tdf3 = tdf3.rename(columns=ren_dict3)

        else:
            t1.printError()
            return pd.DataFrame()

        # 종목코드를 index 할지는 메서드 활용 기능 보고 결정 !
        # tdf1.set_index('종목코드', inplace=True)

        if output == 'output1': 
            return tdf1 
        elif output == 'output2': 
            return tdf2 
        elif output == 'output3': 
            return tdf3
        else: 
            print("Error: output 인자에 잘못된 값을 넣었습니다. ('output1' 또는 'output2' 입력)")



    ### 필요없는 함수. 나중에 삭제 ### 
    ### 필요없는 함수. 나중에 삭제 ### 
    def get_fxrate(self, sdt, edt, output='output2'): 
        url = '/uapi/overseas-price/v1/quotations/inquire-daily-chartprice'
        tr_id = "FHKST03030100"

        params = {
            'FID_COND_MRKT_DIV_CODE': 'X',  # FID 조건 시장 분류 코드 (N: 해외지수, X 환율) 
            'FID_INPUT_ISCD': 'FX@KRW',     # 종목코드
                                            # 해외주식 마스터 코드 참조
                                            # (포럼 > FAQ > 종목정보 다운로드 > 해외주식)
            'FID_INPUT_DATE_1': sdt,        # FID 입력 날짜1 (시작일자(YYYYMMDD)
            'FID_INPUT_DATE_2': edt,        # FID 입력 날짜2 (종료일자(YYYYMMDD))
            'FID_PERIOD_DIV_CODE': 'D',     # FID 기간 분류 코드 (D:일, W:주, M:월, Y:년)
            }

        t1 = super().url_fetch(url, tr_id, params)
        # pdb.set_trace()
        output1 = t1.getBody().output1
        output2 = t1.getBody().output2

        if t1.isOK():  # body 의 rt_cd 가 0 인 경우만 성공

            tdf1 = pd.DataFrame(output1, index=[0])
            cf1 = ['stck_shrn_iscd', 'hts_kor_isnm', 'prdy_ctrt', 'ovrs_nmix_prdy_clpr', 'ovrs_nmix_prpr', 'ovrs_prod_oprc', 'ovrs_prod_hgpr', 'ovrs_prod_lwpr', 'ovrs_nmix_prdy_vrss', 'prdy_vrss_sign', 'acml_vol']
            cf2 = ['단축종목코드', 'HTS종목명', '전일대비율	', '전일종가', '현재가' , '시가', '최고가', '최저가', '전일대비', '전일대비부호', '누적거래량']
            tdf1 = tdf1[cf1]
            tdf1[cf1[2:8]] = tdf1[cf1[2:8]].apply(pd.to_numeric)    # '거래소코드' 는 not numeric! 
            ren_dict = dict(zip(cf1, cf2))
            tdf1 = tdf1.rename(columns=ren_dict)

            tdf2 = pd.DataFrame(output2)
            cf1 = ['stck_bsop_date', 'ovrs_nmix_prpr', 'ovrs_nmix_oprc', 'ovrs_nmix_hgpr', 'ovrs_nmix_lwpr', 'acml_vol', 'mod_yn']
            cf2 = ['영업일자', '현재가', '시가', '최고가', '최저가', '누적거래량', '변경여부']
            tdf2 = tdf2[cf1]
            tdf2[cf1[1:6]] = tdf2[cf1[1:6]].apply(pd.to_numeric)
            ren_dict2 = dict(zip(cf1, cf2))
            tdf2 = tdf2.rename(columns=ren_dict2)
            tdf2.set_index('영업일자', inplace=True) 

            # tdf2 = pd.DataFrame(output3, index=[0])

        else:
            t1.printError()
            return pd.DataFrame()

        # 종목코드를 index 할지는 메서드 활용 기능 보고 결정 !
        # tdf1.set_index('종목코드', inplace=True)

        if output == 'output2': 
            return tdf2 
        elif output == 'output1': 
            return tdf1 
        else: 
            print("Error: output 인자에 잘못된 값을 넣었습니다. ('output1' 또는 'output2' 입력)")
    ### 필요없는 함수. 나중에 삭제 ### 
    ### 필요없는 함수. 나중에 삭제 ### 



    # 내 계좌의 일별 주문 체결 조회
    # Input: 시작일, 종료일 (Option)지정하지 않으면 현재일
    # output: DataFrame

    """
    KIS 포럼 문의.
    주식일별주문체결조회 API 응답결과 output1 자료에서 
    [총체결금액 = 총체결수량 * 체결평균가] 관계식이 성립해야 하는데, 
    데이터 중 몇 개에서 미세한 금액차이가 존재함. (일단 무시)
    """

    # 계좌 잔고 기준 변동량
    # 실제 매매일 기준 +2 영업일 째에 반영됨
    # ex. QVI 20221031 매매
    def get_my_complete(self, sdt, edt, tr_id="TTTC8001R", output='output1', ascending=True):
        url = "/uapi/domestic-stock/v1/trading/inquire-daily-ccld"
        
        params = {
            "CANO": super().getTREnv().my_acct[:8],     # [:8] 추가하여 수정
            "ACNT_PRDT_CD": super().getTREnv().my_acct[9:],
            "INQR_STRT_DT": str(sdt),
            "INQR_END_DT": str(edt),
            "SLL_BUY_DVSN_CD": '00',
            "INQR_DVSN": '01' if (ascending == True) else '00',    # 역순 정순 선택 기능
            "PDNO": "",
            "CCLD_DVSN": "00",
            "ORD_GNO_BRNO": "",
            "ODNO":"",
            "INQR_DVSN_3": "00",
            "INQR_DVSN_1": "",
            "CTX_AREA_FK100": "",
            "CTX_AREA_NK100": ""
        }

        # output1_list = ['ord_dt','orgn_odno', 'sll_buy_dvsn_cd_name', 'pdno', 'ord_qty', 'ord_unpr', 'avg_prvs', 'cncl_yn','tot_ccld_amt','rmn_qty']
        # output2_list = ['tot_ccld_qty', 'pchs_avg_pric', 'tot_ccld_amt', 'prsm_tlex_smtl']
        # index_col = 'odno'

        cf1 = ['ord_dt', 'odno', 'orgn_odno', 'excg_dvsn_cd', 'prdt_type_cd', 'prdt_name', 'pdno', 'sll_buy_dvsn_cd_name', 'sll_buy_dvsn_cd', 'ord_qty', 'rmn_qty', 'tot_ccld_qty', 'avg_prvs', 'tot_ccld_amt']
        cf2 = ['영업일자', '주문번호', '원주문번호', '거래소구분코드', '상품유형코드', '상품명', '종목코드', '매매구분', '매매코드', '주문수량', '잔여수량', '체결수량', '평균가', '총체결금액']

        # step 2. output2 로 tdf2 저장
        cf1_out2 = ['tot_ord_qty', 'tot_ccld_qty', 'pchs_avg_pric', 'tot_ccld_amt', 'prsm_tlex_smtl'] 
        cf2_out2 = ['총주문수량', '총체결수량', '매입평균가격', '총체결금액', '추정제비용합계'] 


        tdf1, tdf2 = super().request_tr(url, tr_id, params, output1_keys=cf1, output2_keys=cf1_out2, appendHeaders={'tr_cont': ""})
        # tdf1.set_index('odno', inplace=True)
        tdf1 = tdf1[cf1]    # 혹시 res json 데이터의 key 값 변할까봐 넣은 코드 
        tdf2 = tdf2[cf1_out2[1:]]   # 혹시 res json 데이터의 key 값 변할까봐 넣은 코드 
                                    # 겸사겸사 필요없는 column 슬라이싱으로 제거 

        ### KIS API request tr 결과 받은 원본 데이터프레임 -> KSIF API 에서 원하는 결과로 데이터프레임 수정 
        ## tdf1.set_index('odno', inplace=True) 
        # question. index 로 숫자 or 종목명, 영업일자 중 뭐로 해야할지 고민 !
        # 원주문번호가 serial 해서 영업일자도 정렬되면 원래 방식인 주문번호 인덱싱이 사용자한테는 좋을 듯
        # database cardinality 입장에서는 중복이 많은 영업일자가 좋을 수도.
        rename_dict = dict(zip(cf1, cf2)) 
        tdf1[cf1[9:]] = tdf1[cf1[9:]].apply(pd.to_numeric)      # 숫자변환은 'ord_qty'('주문수량') 이후에만 적용! (연산필요한 값들만 적용)
        tdf1 = tdf1.rename(columns=rename_dict) 
        tdf1.set_index('영업일자', inplace=True) 
        # tdf1 = tdf1.sort_index(ascending=ascending)
        
        # 예수금변동액 column 추가 
        ### 
        sll_buy_sign = tdf1['매매코드'].replace({'01': -1, '02': 1}).astype(int)    # 01: 매도, 02: 매수
        acct_amt_chg = - sll_buy_sign * tdf1['총체결금액']
        hldg_qty_chg = sll_buy_sign * tdf1['체결수량']
        tdf1 = pd.concat([tdf1['주문번호'], tdf1['거래소구분코드'], tdf1['상품유형코드'], tdf1['상품명'], \
                        tdf1['종목코드'], tdf1['매매구분'], tdf1['체결수량'], tdf1['평균가'], tdf1['총체결금액'], \
                        pd.DataFrame(hldg_qty_chg, columns=['보유수변동량']), \
                        pd.DataFrame(acct_amt_chg, columns=['예수금변동액'])], \
                        axis=1)
        tdf1.set_index('주문번호', append=True, inplace=True) 

        # tdf2 데이터프레임 데이터 수정 
        rename_dict = dict(zip(cf1_out2, cf2_out2)) 
        tdf2 = tdf2.apply(pd.to_numeric) 
        tdf2 = tdf2.rename(columns=rename_dict) 
        # tdf2 = tdf2[['총체결수량', '매입평균가격', '총체결금액', '추정제비용합계']]

        ### 요청 tr 횟수 줄이기 위해 'all' 인자 포함 
        ### 사용시 명확성 늘리기 위해 'all' 인자 제거 (향후 서버 운영시 호출량 보고 판단)
        if output == 'output1':
            return tdf1
        elif output == 'output2': 
            return tdf2
        # elif output == 'all':
        #     return tdf1, tdf2, tdf0
        else: 
            print("Error: output 인자에 잘못된 값을 넣었습니다. ('output1' 또는 'output2' 입력)")



    # 계좌 잔고 기준 변동량 (미국) 
    # 미국 계좌 거래내역은 3개월 이전, 이후 구분 없으므로, get_my_complete() 코드 베이스로 완료! 
    # 실제 매매일 기준 +2 영업일 째에 반영됨
    # ex. DAR 20221031 매매
    def get_daily_completes_us(self, sdt, edt, tr_id="TTTS3035R", output='output', ascending=True):
        url = "/uapi/overseas-stock/v1/trading/inquire-ccnl"
        
        params = {
            "CANO": super().getTREnv().my_acct[:8],     # [:8] 추가하여 수정
            "ACNT_PRDT_CD": super().getTREnv().my_acct[9:],           
            "ORD_STRT_DT": str(sdt),   ### INQR_STRT_DT -> ORD_STRT_DT 
            "ORD_END_DT": str(edt),    ### INQR_END_DT -> ORD_END_DT 
            "SLL_BUY_DVSN": '00',    ### SLL_BUY_DVSN_CD -> SLL_BUY_DVSN 
            "SORT_SQN": 'DS' if (ascending == True) else 'AS',    # 역순 정순 선택 기능
                                        ### INQR_DVSN -> SORT_SQN ('DS': 정순, 'AS': 역순)
            "PDNO": "%",                ### ('%': 전종목)
            "CCLD_NCCS_DVSN": "00",     ### CCLD_DVSN -> CCLD_NCCS_DVSN 
            "OVRS_EXCG_CD": "%",        ### 해외거래소코드
            "ORD_DT": "" ,              ### 주문일자 
            "ORD_GNO_BRNO": "",
            "ODNO":"",
            "CTX_AREA_NK200": "",
            "CTX_AREA_FK200": ""
        }

        # output1_list = ['ord_dt','orgn_odno', 'sll_buy_dvsn_cd_name', 'pdno', 'ord_qty', 'ord_unpr', 'avg_prvs', 'cncl_yn','tot_ccld_amt','rmn_qty']
        # output2_list = ['tot_ccld_qty', 'pchs_avg_pric', 'tot_ccld_amt', 'prsm_tlex_smtl']
        # index_col = 'odno'

        ### 해외주식 거래조회  res 에는 상품유형코드 정보가 없네 (?) 
        # excg_dvsn_cd -> ovrs_excg_cd, prdt_type_cd -> tr_crcy_cd, ... 
        cf1 = ['ord_dt', 'odno', 'orgn_odno', 'ovrs_excg_cd', 'tr_crcy_cd', 'prdt_name', 'pdno', 'sll_buy_dvsn_cd_name', 'sll_buy_dvsn_cd', 'ft_ord_qty', 'nccs_qty', 'ft_ccld_qty', 'ft_ccld_unpr3', 'ft_ccld_amt3']
        cf2 = ['영업일자', '주문번호', '원주문번호', '거래소구분코드', '거래통화코드', '상품명', '종목코드', '매매구분', '매매코드', '주문수량', '잔여수량', '체결수량', '평균가', '총체결금액']

        ### 해외는 output2 없음 ! 
        ### cf1_out2 = ['tot_ord_qty', 'tot_ccld_qty', 'pchs_avg_pric', 'tot_ccld_amt', 'prsm_tlex_smtl'] 
        ### cf2_out2 = ['총주문수량', '총체결수량', '매입평균가격', '총체결금액', '추정제비용합계'] 

        tdf1 = super().request_tr(url, tr_id, params, output1_keys=cf1, appendHeaders={'tr_cont': ""})
        # tdf1.set_index('odno', inplace=True)
        tdf1 = tdf1[cf1]    # 혹시 res json 데이터의 key 값 변할까봐 넣은 코드 

        ### KIS API request tr 결과 받은 원본 데이터프레임 -> KSIF API 에서 원하는 결과로 데이터프레임 수정 
        ## tdf1.set_index('odno', inplace=True) 
        # question. index 로 숫자 or 종목명, 영업일자 중 뭐로 해야할지 고민 !
        # 원주문번호가 serial 해서 영업일자도 정렬되면 원래 방식인 주문번호 인덱싱이 사용자한테는 좋을 듯
        # database cardinality 입장에서는 중복이 많은 영업일자가 좋을 수도.
        rename_dict = dict(zip(cf1, cf2)) 
        tdf1[cf1[9:]] = tdf1[cf1[9:]].apply(pd.to_numeric)      # 숫자변환은 'ord_qty'('주문수량') 이후에만 적용! (연산필요한 값들만 적용)
        tdf1 = tdf1.rename(columns=rename_dict) 
        tdf1.set_index('영업일자', inplace=True) 
        # tdf1 = tdf1.sort_index(ascending=ascending)
        
        # 예수금변동액 column 추가 
        ### 
        sll_buy_sign = tdf1['매매코드'].replace({'01': -1, '02': 1}).astype(int)    # 01: 매도, 02: 매수
        acct_amt_chg = - sll_buy_sign * tdf1['총체결금액']
        hldg_qty_chg = sll_buy_sign * tdf1['체결수량']
        tdf1 = pd.concat([tdf1['주문번호'], tdf1['거래소구분코드'], tdf1['거래통화코드'], tdf1['상품명'], \
                        tdf1['종목코드'], tdf1['매매구분'], tdf1['체결수량'], tdf1['평균가'], tdf1['총체결금액'], \
                        pd.DataFrame(hldg_qty_chg, columns=['보유수변동량']), \
                        pd.DataFrame(acct_amt_chg, columns=['예수금변동액'])], \
                        axis=1)
        tdf1.set_index('주문번호', append=True, inplace=True) 

        # tdf2 데이터프레임 데이터 수정 
        # rename_dict = dict(zip(cf1_out2, cf2_out2)) 
        # tdf2 = tdf2.apply(pd.to_numeric) 
        # tdf2 = tdf2.rename(columns=rename_dict) 

        ### 요청 tr 횟수 줄이기 위해 'all' 인자 포함 
        ### 사용시 명확성 늘리기 위해 'all' 인자 제거 (향후 서버 운영시 호출량 보고 판단)
        if output == 'output':
            return tdf1
        # elif output == 'output2': 
        #     return tdf2
        # elif output == 'all':
        #     return tdf1, tdf2, tdf0
        else: 
            print("Error: output 인자에 잘못된 값을 넣었습니다. ('output' 입력)")



    def get_daily_completes(self, sdt, edt, output='output1', ascending=True):

        # 조회 범위 계산을 위한 변수들
        today_dt = datetime.now().date()
        thresh_dt = today_dt - relativedelta(months=3)
        sdt_dt = datetime.strptime(str(sdt), '%Y%m%d').date()
        edt_dt = datetime.strptime(str(edt), '%Y%m%d').date()
        
        # 3개월 이내 범위
        if sdt_dt >= thresh_dt:
            tr_id = "TTTC8001R"     # -3개월 이후 시점에 대한 tr_id 값 
            daily_completes_df = self.get_my_complete(sdt_dt.strftime("%Y%m%d"), edt_dt.strftime("%Y%m%d"), tr_id=tr_id, output=output, ascending=ascending)
            return daily_completes_df
        # 3개월 이후 ~ 3개월 이내 범위
        # 재귀적으로 짠 코드 
        elif sdt_dt < thresh_dt and edt_dt >= thresh_dt:
            
            # origin_sdt_dt = sdt_dt
            before_thresh_dt = thresh_dt - relativedelta(days=1)
            ### Error: get_my_complete() 함수가 하나의 객체로 저장므로, sdt_dt 변수가 덮어씌워지는 문제 !!! ### 
            ### 다른 base function 객체 만들어서 처리하던지 (추천), 객체로 처리하던지, deepcopy 같은 방법은? ### 
            tr_id = "TTTC8001R"     # -3개월 이후 시점에 대한 tr_id 값 
            # pdb.set_trace()
            innder_df = self.get_my_complete(thresh_dt.strftime("%Y%m%d"), edt_dt.strftime("%Y%m%d"), tr_id=tr_id, output=output, ascending=ascending)
            tr_id = "CTSC9115R"     # -3개월 이전 시점에 대한 tr_id 값
            outer_df = self.get_my_complete(sdt_dt.strftime("%Y%m%d"), before_thresh_dt.strftime("%Y%m%d"), tr_id=tr_id, output=output, ascending=ascending)

            if ascending == True:
                daily_completes_df = pd.concat([outer_df, innder_df])
            else:
                daily_completes_df = pd.concat([innder_df, outer_df])
            
            if output == 'output2':
                daily_completes_df = daily_completes_df.cumsum(axis=0).iloc[[-1]]
                daily_completes_df['매입평균가격'] = daily_completes_df['총체결금액'] / daily_completes_df['총체결수량']

            return daily_completes_df

        # 3개월 이후 범위
        elif sdt_dt < thresh_dt and edt_dt < thresh_dt:
            tr_id = "CTSC9115R"     # -3개월 이전 시점에 대한 tr_id 값
            daily_completes_df = self.get_my_complete(sdt_dt.strftime("%Y%m%d"), edt_dt.strftime("%Y%m%d"), tr_id=tr_id, output=output, ascending=ascending)
            return daily_completes_df



    # dates * stocks multi-index 전부 생성
    # 날짜 별로 stocks 변화량 전부 기록
    # .dropna() 적용시 당일 변동 종목만 남음
    def get_daily_acct_chgs(self, sdt, edt, ascending=True, dropna=False):

        daily_completes_kr = self.get_daily_completes(sdt, edt, output='output1', ascending=ascending).reset_index() 
        daily_completes_us = self.get_daily_completes_us(sdt, edt, output='output', ascending=ascending).reset_index() 
        daily_completes = daily_completes_kr.merge(daily_completes_us, how='outer')
        # pdb.set_trace() 
        acct_chgs = daily_completes.groupby(['영업일자', '종목코드', '거래소구분코드'], sort=False)[['보유수변동량', '예수금변동액']].sum()
        acct_chgs = acct_chgs.reset_index(level=2) 
        # acct_chgs_us = daily_completes_us.groupby(['영업일자', '종목코드', '거래소구분코드'], sort=False)['보유수변동량', '예수금변동액'].sum()
        # acct_chgs_us = acct_chgs_us.reset_index(level=2) 
        # acct_chgs = acct_chgs.join(acct_chgs_us, how='outer')

        ### 여기서부터 ###
        opendays_kr = get_business_days("XKRX", sdt, edt, ascending=ascending)     # 한국 주식 관련 메서드므로 주식시장 한국으로 고정 
        opendays_us = get_business_days("XNYS", sdt, edt, ascending=ascending) 
        opendays = np.union1d(opendays_kr, opendays_us)
        # opendays.merge(opendays_us, how='outer')

        changed_stocks = acct_chgs.reset_index()['종목코드'].unique()
        changed_stocks.sort()
        # changed_stocks_us = acct_chgs_us.reset_index()['종목코드'].unique()
        # changed_stocks_us.sort() 
        # changed_stocks = changed_stocks.join(changed_stocks_us, how='outer')

        dayily_stock_chgs_multiindex = pd.MultiIndex.from_product([opendays, changed_stocks], names=['영업일자', '종목코드'])
        dayily_stock_chgs_df = pd.DataFrame(index=dayily_stock_chgs_multiindex, 
                                            columns=['거래소구분코드', '보유수변동량', '예수금변동액']) 
        
        dayily_stock_chgs_df.update(acct_chgs)
        ### 여기까지 뭐하러 필요한거지? 그냥 stock_chgs 결과랑 똑같은것 같은데? ### 

        ### 여기서부터 ###

        # dayily_stock_chgs_us_multiindex = pd.MultiIndex.from_product([opendays_us, changed_stocks_us], names=['영업일자', '종목코드'])
        # dayily_stock_chgs_us_df = pd.DataFrame(index=dayily_stock_chgs_us_multiindex) 
        # daily_acct_chgs_us = dayily_stock_chgs_us_df.join(acct_chgs_us, how='left')
        # ### 여기까지 뭐하러 필요한거지? 그냥 stock_chgs 결과랑 똑같은것 같은데? ### 

        # daily_acct_chgs = daily_acct_chgs.join(daily_acct_chgs_us, how='outer')

        if dropna == True: 
            dayily_stock_chgs_df = dayily_stock_chgs_df.dropna() 

        return dayily_stock_chgs_df 



    # ascending=False 기준으로만 정상작동.
    # ascending=True 기준은 거래량이 음수로 누적되서 보유수량이 전부 마이너스로 나옴! 
    # if ascending=True 케이스 분기해서 수정필요
    def get_daily_stocks(self, sdt, edt, ascending=True, dropna=True): 

        # # utcnow_dt = datetime.utcnow().date()
        # # utcnow = utcnow_dt.strftime('%Y%m%d')
        # xkrx = xcals.get_calendar("XKRX")
        # curr_xdate = xkrx.date_to_session(datetime.now().date(), direction="previous")
        
        # curr_xstr = get_business_days("XKRX", datetime.now().date())[0]   # datetime.now() 가 휴일이면 에러 발생 ! 
        curr_xstr = str(datetime.now().date()).replace('-','')

        # 
        # curr_xstr_df = pd.DataFrame(columns=['영업일자'])
        # for i in range(len(curr_stocks_df)):
        #     curr_xstr_df = curr_xstr_df.append(pd.DataFrame([curr_xstr], columns=['영업일자']), ignore_index=True)
        # today_stocks_df = pd.concat([curr_xstr_df, curr_stocks_df], axis=1)

        # 현재 보유주식 (('영업일자', '종목코드')로 multiindexing)
        acct_stocks_df = self.get_acct_balance(output='output1')[['종목코드', '보유수량']]
        # curr_stocks_df = curr_stocks_df[['종목코드', '보유수량']]
        curr_xstr_df = pd.DataFrame([curr_xstr for i in range(0, acct_stocks_df.shape[0])], columns=['영업일자'])
        today_stocks_df = pd.concat([curr_xstr_df, acct_stocks_df], axis=1).set_index(['영업일자', '종목코드'])
        # pdb.set_trace()
        # today_stocks_df = today_stocks_df.fillna(curr_xstr_sr.values[0])
        # today_stocks_df = today_stocks_df.set_index(['영업일자', '종목코드'])

        # 과거 주식변동내역 
        daily_stock_chgs = self.get_daily_acct_chgs(sdt, curr_xstr, dropna=False, ascending=False)
        # daily_stocks_chgs_shifted = daily_stock_chgs.groupby(by='종목코드').shift(periods=-1).fillna(0)
        # daily_stocks_rev_shifted = daily_stocks_rev_shifted.shift(periods=1)
        # daily_stocks_rev_shifted = daily_stocks_rev_shifted.rename(columns={'보유수변동량': '보유수량'})

        idx_lv1 = daily_stock_chgs.index.get_level_values('영업일자').unique()
        idx_lv2 = daily_stock_chgs.index.get_level_values('종목코드').unique()

        daily_stock_chgs_adj = daily_stock_chgs.join(today_stocks_df, how='left')

        # 과거 주식분할내역 (=액면분할내역)
        # pykrx 패키지 그냥 사용 
        # for ... 
        # if 한국 종목 
        stock_split_df = krx_split_events(idx_lv2).swaplevel(i='영업일자', j='종목코드')
        stock_increase_df = get_dart_fricdecsn(sdt, edt, idx_lv2) 
        stock_increase_df.rename(columns={'신주상장 예정일': '영업일자', '주당 신주배정 주식수(보통주)': '주당신주수'}, inplace=True)
        stock_increase_df.set_index(['영업일자', '종목코드'], inplace=True)
        stock_increase_df = stock_increase_df[['주당신주수']]
        stock_decrease_df = get_dart_crdecsn(sdt, edt, idx_lv2) 
        stock_decrease_df.rename(columns={'신주상장 예정일': '영업일자', '감자비율%(보통주)': '감자비율'}, inplace=True)
        stock_decrease_df.set_index(['영업일자', '종목코드'], inplace=True)
        stock_decrease_df = stock_decrease_df[['감자비율']]
        # if 해외 종목 ...
        # pdb.set_trace()
        # daily_stock_chgs_adj = daily_stock_chgs.join(stock_split_df, how='left')
        # daily_stock_chgs_adj_shifted = daily_stock_chgs_adj.groupby(by='종목코드').shift(periods=1)
        init_stocks_df = daily_stock_chgs_adj.join([stock_split_df, stock_increase_df, stock_decrease_df], how='left')
        # pdb.set_trace()
        # idx_lv1 = init_stocks_df.index.get_level_values('영업일자').unique()
        # idx_lv2 = init_stocks_df.index.get_level_values('종목코드').unique()
        # columns = init_stocks_df.columns

        init_swapped_df = init_stocks_df.swaplevel(i='영업일자', j='종목코드').sort_index(level=0, sort_remaining=False)
        init_swapped_df = init_swapped_df.fillna(value={'보유수변동량': 0, '예수금변동액': 0, '액면분할비율': 1, '감자비율': 1, '주당신주수': 0, '보유수량': 0})
        # pdb.set_trace()

        for ticker in idx_lv2: 
            for i in range(0, len(idx_lv1)-1): 

                revcal_split = (init_swapped_df.loc[(ticker, idx_lv1[i]), '보유수량'] \
                                / init_swapped_df.loc[(ticker, idx_lv1[i]), '액면분할비율']) \
                               - init_swapped_df.loc[(ticker, idx_lv1[i]), '보유수변동량']
                init_swapped_df.loc[(ticker, idx_lv1[i+1]), '보유수량'] = 0 if (revcal_split < 0) else revcal_split

                revcal_decrease = (init_swapped_df.loc[(ticker, idx_lv1[i]), '보유수량'] \
                                   / init_swapped_df.loc[(ticker, idx_lv1[i]), '감자비율']) \
                                  - init_swapped_df.loc[(ticker, idx_lv1[i]), '보유수변동량']
                init_swapped_df.loc[(ticker, idx_lv1[i+1]), '보유수량'] = 0 if (revcal_decrease < 0) else revcal_decrease

                revcal_increase = init_swapped_df.loc[(ticker, idx_lv1[i]), '보유수량'] \
                                  - (init_swapped_df.loc[(ticker, idx_lv1[i]), '보유수량'] \
                                     * init_swapped_df.loc[(ticker, idx_lv1[i]), '주당신주수']) \
                                  - init_swapped_df.loc[(ticker, idx_lv1[i]), '보유수변동량']
                init_swapped_df.loc[(ticker, idx_lv1[i+1]), '보유수량'] = 0 if (revcal_increase < 0) else revcal_increase

                # if revcal_result < 0:   # 단수주 발생으로 인해 보유주식수가 negative 값 나오는 경우 처리 
                #     init_swapped_df.loc[(ticker, idx_lv1[i+1]), '보유수량'] = 0  # Nan 넣으면 revcal_result 계산 안됨!
                # else: 
                #     init_swapped_df.loc[(ticker, idx_lv1[i+1]), '보유수량'] = revcal_result

        init_swapped_df['보유수량'].replace(0, np.nan, inplace=True)    # replace nan 코드 쓸지 말지 고민 (추후 활용함수 고려)
        daily_acct_df = init_swapped_df.swaplevel(i='영업일자', j='종목코드').sort_index(ascending=True)

        if dropna == True:
            daily_acct_df = daily_acct_df[(daily_acct_df['보유수량'] != 0)].dropna()

        if ascending == True: 
            return daily_acct_df
        elif ascending == False: 
            return daily_acct_df.sort_index(ascending=False)




    def get_daily_deposits(self, sdt, edt, ascending=True):

        today_deposit = self.get_acct_balance(output='output2')['예수금']
        # today_d=eposit = curr_balance['예수금']

        acct_chgs = self.get_my_complete(sdt, edt, output='output1') 
        deposit_chgs = acct_chgs.groupby('영업일자')['예수금변동액'].sum()

        opendays_rev = get_business_days("XKRX", sdt, edt, ascending=False)     # 한국 주식 관련 메서드므로 주식시장 한국으로 고정 
        opendays_rev_df = pd.DataFrame(index=opendays_rev)
        # (sdt, edt) 기간 영업일에 대한 daily 예수금 계산
        daily_deposit_chgs = pd.concat([opendays_rev_df, deposit_chgs], axis=1)
        daily_deposit_chgs_rev = daily_deposit_chgs.sort_index(ascending=False)
        daily_deposit_chgs_rev['예수금변동액'].iloc[0] = today_deposit
        daily_deposits = daily_deposit_chgs_rev.expanding().sum()
        daily_deposits.rename(columns={'예수금변동액': '예수금'}, inplace=True)

        if ascending == True: 
            daily_deposits = daily_deposits.sort_index(ascending=True).astype(int)  # 앞에서 sorted 된 데이터 불러와서 처리할지, 마지막에 한번에 sorting 할지 나중에 고민.

        return daily_deposits 




    #종목의 주식, ETF, 선물/옵션 등의 구분값을 반환. 현재는 무조건 주식(J)만 반환
    def _getStockDiv(self, stock_no):
        return 'J'


    # 종목별 현재가를 dict 로 반환
    # Input: 종목코드
    # Output: 현재가 Info dictionary. 반환된 dict 가 len(dict) < 1 경우는 에러로 보면 됨

    def get_current_price(self, stock_no):
        url = "/uapi/domestic-stock/v1/quotations/inquire-price"
        tr_id = "FHKST01010100"

        params = {
            'FID_COND_MRKT_DIV_CODE': self._getStockDiv(stock_no), 
            'FID_INPUT_ISCD': stock_no
            }
        
        t1 = super().url_fetch(url, tr_id, params)
        
        if t1.isOK():
            return t1.getBody().output
        else:
            t1.printError()
            return dict()



    # 국내주식기간별시세 (일/주/월/년) [v1_국내주식-016]

    # 참고. 해외주식 기간별시세 [v1_해외주식-010] API 따로 작성 필요
        # 다음 내역 호출하는 코드 없음 => 내부적으로 호출 반복하도록 작성 필요 !
        # 다음 내역 호출이 있는 API 는 일별 여러 데이터가 호출되는 경우 !  
        # 다음 내역 호출이 없는 API 는 일별 하나의 데이터가 호출되는 경우 !

    # 참고. 종목별로 따로 기간별시세 호출해야 함 ...
        # 일별 자산구성과 일별 개별자산 정보간 데이터구조 어떻게 연관지을지 고민 !

    # Response <Body></Body> 내역 : 
        # output1 : 종목 현재 시가 및 기간내 상하한가, 거래량, 액면가, 상장수, 시총, PER/EPS/PBR, 융자잔고비율
        # output2 : 종목 과거 종가 및 최고저가, 누적거래량(대금), 분할여부, 분할비율, 락 구분 (권리락, 배당락, 분배락 등...)

    def get_daily_price(self, stock_no, sdt, edt=None, freq='D', adj=1, output='all', ascending=True): 
        url = "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
        tr_id = "FHKST03010100"

        # 조회 범위 계산을 위한 변수들
        today_dt = datetime.now().date()
        if (edt is None):
            ltdt = today_dt.strftime('%Y%m%d')
        else:
            ltdt = edt

        params = {
            'FID_COND_MRKT_DIV_CODE': self._getStockDiv(stock_no),   # 시장 분류 코드
            'FID_INPUT_ISCD': stock_no,                         # 종목번호 (6자리)
            'FID_INPUT_DATE_1': sdt,                            # 입력 날짜 (시작)
            'FID_INPUT_DATE_2': ltdt,                           # 입력 날짜 (종료)
            'FID_PERIOD_DIV_CODE': freq,                        # 기간분류코드 (D: 일봉, W: 주봉, M: 월봉, Y: 년봉)
            'FID_ORG_ADJ_PRC': adj,                             # 수정주가 원주가 가격 여부	(0: 수정주가, 1: 원주가)
        }

        cf1 = {
            'prdy_vrss': '전일 대비',
            'prdy_vrss_sign': '전일 대비 부호',
            'prdy_ctrt': '전일 대비율',
            'stck_prdy_clpr': '주식 전일 종가',
            'acml_vol': '누적 거래량',
            'acml_tr_pbmn': '누적 거래 대금',
            'hts_kor_isnm': 'HTS 한글 종목명',
            'stck_prpr': '주식 현재가',
            'stck_shrn_iscd': '주식 단축 종목코드',
            'prdy_vol': '전일 거래량',
            'stck_mxpr': '상한가',
            'stck_llam': '하한가',
            'stck_oprc': '시가',
            'stck_hgpr': '최고가',
            'stck_lwpr': '최저가',
            'stck_prdy_oprc': '주식 전일 시가',
            'stck_prdy_hgpr': '주식 전일 최고가',
            'stck_prdy_lwpr': '주식 전일 최저가',
            'askp': '매도호가',
            'bidp': '매수호가',
            'prdy_vrss_vol': '전일 대비 거래량',
            'vol_tnrt': '거래량 회전율',
            'stck_fcam': '주식 액면가',
            'lstn_stcn': '상장 주수',
            'cpfn': '자본금',
            'hts_avls': '시가총액',
            'per': 'PER',
            'eps': 'EPS',
            'pbr': 'PBR',
            'itewhol_loan_rmnd_ratem name': '전체 융자 잔고 비율',
        }

        cf2 = {
            'stck_bsop_date': '영업일자', 
            'stck_clpr': '주식 종가', 
            'stck_oprc': '주식 시가', 
            'stck_hgpr': '주식 최고가', 
            'stck_lwpr': '주식 최저가', 
            'acml_vol': '누적 거래량', 
            'acml_tr_pbmn': '누적 거래 대금', 
            'flng_cls_code': '락 구분 코드', 
            'prtt_rate': '분할 비율', 
            'mod_yn': '분할변경여부', 
            'prdy_vrss_sign': '전일 대비 부호', 
            'prdy_vrss': '전일 대비', 
            'revl_issu_reas': '재평가사유코드', 
        }
        
        t1 = super().url_fetch(url, tr_id, params)
        time.sleep(0.05)                            # REST API 초당 20건 호출 가능

        # 1회 호출 (tdf2 는 100 영업일 내역만 호출됨)
        if t1.isOK():
            tsr1 = pd.Series(t1.getBody().output1)
            tsr1 = tsr1.rename(index=cf1)               # pd.Series 자료형이므로 index 이름 변경
            tdf2 = pd.DataFrame(t1.getBody().output2)
            tdf2 = tdf2.rename(columns=cf2)             # pd.DataFrame 자료형이므로 columns 이름 변경
            # tdf2 = tdf2.set_index('영업일자')
        else:
            t1.printError()
            return pd.DataFrame(), pd.DataFrame()

        # 조회 시작일(std)이 개장일인지 여부
        is_opened = XKRX.is_session(str(sdt))  

        # tdf2 추가호출 및 병합 후 리턴 처리
        # tdf2.iloc[-1]['영업일자'] 값이 nan 이라서 생기는 문제 예외처리 
        cont = False if pd.isnull(tdf2.iloc[-1]['영업일자']) else True
        # if pd.isnull(tdf2.iloc[-1]['영업일자']):
        #     cont = False
        # else:
        #     cont = True

        while cont == True:

            # 추가호출 필요 여부 확인
            """
            sdt 가 주말인 경우, while 문 탈출에 문제 생기는 듯. 
            오류 찾아서 핵결 !
            """
            # ldt : 지금까지 조회해서 합친 tdf2 중 가장 마지막 일자
            # if ascending == True:
            ldt = tdf2.iloc[-1]['영업일자']
            # else: 
            #     ldt = tdf2.iloc[0]['영업일자']
            
            last_dt = datetime.strptime(str(ldt), '%Y%m%d').date()
            delta_days = get_date_delta(sdt, ldt).days
            # start_dt = datetime.strptime(str(sdt), '%Y%m%d').date()
            ## sdt ~ ldt 사이 남은 기간 계산
            # delta = last_dt - start_dt

            ## case 1. sdt가 개장일이고, 조회된 마지막 일자가 sdt 인 경우
            if is_opened == True and ldt == str(sdt):
                cont = False
                break
            # elif tdf2.iloc[-1]['영업일자'] is None:
            #     print('영업일자 데이터 null error')
            #     print(tdf2.iloc[-1]['영업일자'])
            #     break

            ## case 2. sdt가 폐장일이고, 조회된 마지막 일자가 sdt 아닌 경우 (마지막일자 ~ sdt 사이기간이 100일 미만일 때)
            ## sdt ~ 조회 마지막 일자 사잇날들이 모두 폐장일이면 cont = False 로 바꾸기 !
            elif is_opened == False and delta_days < 190:   # 최대 100건 호출이므로 일반일자 기준 190일 정도 체크
                # 사잇기간 is_opened 모두 False 여야 작동하는 코드
                # while 이나 for 문으로 하나라도 True 있으면 elif 절 pass 하도록 코드 작성
                # temp = False
                cont = False
                for d in range(1, delta_days):
                    check_dt = last_dt - relativedelta(days=d)
                    if XKRX.is_session(check_dt) == True:
                        cont_edt = ldt
                        cont = True
                        # temp = True
                        break
                    else:
                        continue
                # sdt ~ ldt 사이기간 체크했는데 조회할 영업일 없으면 while문 탈출
                if cont == False: 
                    break
                # if temp == False:
                    # cont = False
            ## case 3. 조회된 마지막 일자가 sdt 아니고, 마지막일자 ~ sdt 사이기간이 100 이상일 때
            else:
                cont_edt = ldt
                cont = True

            # 호출 edt 일자 갱신
            thresh_dt = datetime.strptime(str(cont_edt), '%Y%m%d').date()
            cont_dt = thresh_dt - relativedelta(days=1)
            params['FID_INPUT_DATE_2'] = cont_dt.strftime('%Y%m%d')

            # 다음 edt 일자에 대한 호출
            t1 = super().url_fetch(url, tr_id, params)
            time.sleep(0.05)                        # REST API 초당 20건 호출 가능

            if t1.isOK() and output=='all':

                # 갱신된 다음 100 영업일 이하 기간에 대해 호출 후 기존 tdf2와 병합
                next_tdf2 = pd.DataFrame(t1.getBody().output2)
                if len(next_tdf2) == 0:
                    break
                next_tdf2 = next_tdf2.rename(columns=cf2)   # pd.DataFrame 자료형이므로 columns 이름 변경

                # 국내주식기간별시세 API 경우 res 결과 항상 정순(오름차순)만 받음.
                # if ascending=False: 이용한 역순 pd.concat() 무의미! (마지막에 .sort_index 해줘야함)
                tdf2 = pd.concat([tdf2, next_tdf2])
                # pdb.set_trace()
            else:
                t1.printError()
                return pd.DataFrame(), pd.DataFrame()

        # tsr1 = tsr1.rename(index=cf1)
        tdf2 = tdf2.set_index('영업일자')
        # ascending 인자로 False (역순) 들어온 내용은 자료 다 합친 뒤 마지막에 수행.
        tdf2 = tdf2.sort_index(ascending=ascending)
        return tsr1, tdf2 



    def get_daily_price_us(self, xchg_cd, stock_no, sdt, freq='D', adj=1, output='all', ascending=True): 
        url = "/uapi/overseas-price/v1/quotations/dailyprice"
        tr_id = "HHDFS76240000"

        # 조회 범위 계산을 위한 변수들
        # today_dt = datetime.now().date()
        # if (edt is None):
        #     ltdt = today_dt.strftime('%Y%m%d')
        # else:
        #     ltdt = edt

        # freq 파라미터 한국과 달라서 수정 
        freq_us = {"D": '0', "W": '1', "M": '2'}
        freq_us = freq_us[freq]

        params = {
            'AUTH': "",                             # 사용자권한정보 ("": Null 값 설정)
            'EXCD': xchg_cd,                        # 거래소 코드 (해외 시장 분류 코드), self._getStockDiv(stock_no) 대신 외부 입력. 
            'SYMB': stock_no,                         # 종목번호 (6자리)
            'GUBN': freq_us,                        # 기간분류코드 (D: 일봉, W: 주봉, M: 월봉, Y: 년봉)
            'BYMD': sdt,                            # 입력 날짜 (시작)
            'MODP': adj,                             # 수정주가 원주가 가격 여부	(0: 수정주가, 1: 원주가)
        }

        cf1 = {
            'prdy_vrss': '전일 대비',
            'prdy_vrss_sign': '전일 대비 부호',
            'prdy_ctrt': '전일 대비율',
            'stck_prdy_clpr': '주식 전일 종가',
            'acml_vol': '누적 거래량',
            'acml_tr_pbmn': '누적 거래 대금',
            'hts_kor_isnm': 'HTS 한글 종목명',
            'stck_prpr': '주식 현재가',
            'stck_shrn_iscd': '주식 단축 종목코드',
            'prdy_vol': '전일 거래량',
            'stck_mxpr': '상한가',
            'stck_llam': '하한가',
            'stck_oprc': '시가',
            'stck_hgpr': '최고가',
            'stck_lwpr': '최저가',
            'stck_prdy_oprc': '주식 전일 시가',
            'stck_prdy_hgpr': '주식 전일 최고가',
            'stck_prdy_lwpr': '주식 전일 최저가',
            'askp': '매도호가',
            'bidp': '매수호가',
            'prdy_vrss_vol': '전일 대비 거래량',
            'vol_tnrt': '거래량 회전율',
            'stck_fcam': '주식 액면가',
            'lstn_stcn': '상장 주수',
            'cpfn': '자본금',
            'hts_avls': '시가총액',
            'per': 'PER',
            'eps': 'EPS',
            'pbr': 'PBR',
            'itewhol_loan_rmnd_ratem name': '전체 융자 잔고 비율',
        }

        cf2 = {
            'stck_bsop_date': '영업일자', 
            'stck_clpr': '주식 종가', 
            'stck_oprc': '주식 시가', 
            'stck_hgpr': '주식 최고가', 
            'stck_lwpr': '주식 최저가', 
            'acml_vol': '누적 거래량', 
            'acml_tr_pbmn': '누적 거래 대금', 
            'flng_cls_code': '락 구분 코드', 
            'prtt_rate': '분할 비율', 
            'mod_yn': '분할변경여부', 
            'prdy_vrss_sign': '전일 대비 부호', 
            'prdy_vrss': '전일 대비', 
            'revl_issu_reas': '재평가사유코드', 
        }
        
        t1 = super().url_fetch(url, tr_id, params)
        time.sleep(0.05)                            # REST API 초당 20건 호출 가능

        # 1회 호출 (tdf2 는 100 영업일 내역만 호출됨)
        if t1.isOK():
            tsr1 = pd.Series(t1.getBody().output1)
            tsr1 = tsr1.rename(index=cf1)               # pd.Series 자료형이므로 index 이름 변경
            tdf2 = pd.DataFrame(t1.getBody().output2)
            tdf2 = tdf2.rename(columns=cf2)             # pd.DataFrame 자료형이므로 columns 이름 변경
            # tdf2 = tdf2.set_index('영업일자')
        else:
            t1.printError()
            return pd.DataFrame(), pd.DataFrame()

        # 조회 시작일(std)이 개장일인지 여부
        is_opened = XKRX.is_session(str(sdt))  

        # tdf2 추가호출 및 병합 후 리턴 처리
        # tdf2.iloc[-1]['영업일자'] 값이 nan 이라서 생기는 문제 예외처리 
        cont = False if pd.isnull(tdf2.iloc[-1]['영업일자']) else True
        # if pd.isnull(tdf2.iloc[-1]['영업일자']):
        #     cont = False
        # else:
        #     cont = True

        while cont == True:

            # 추가호출 필요 여부 확인
            """
            sdt 가 주말인 경우, while 문 탈출에 문제 생기는 듯. 
            오류 찾아서 핵결 !
            """
            # ldt : 지금까지 조회해서 합친 tdf2 중 가장 마지막 일자
            # if ascending == True:
            ldt = tdf2.iloc[-1]['영업일자']
            # else: 
            #     ldt = tdf2.iloc[0]['영업일자']
            
            last_dt = datetime.strptime(str(ldt), '%Y%m%d').date()
            delta_days = get_date_delta(sdt, ldt).days
            # start_dt = datetime.strptime(str(sdt), '%Y%m%d').date()
            ## sdt ~ ldt 사이 남은 기간 계산
            # delta = last_dt - start_dt

            ## case 1. sdt가 개장일이고, 조회된 마지막 일자가 sdt 인 경우
            if is_opened == True and ldt == str(sdt):
                cont = False
                break
            # elif tdf2.iloc[-1]['영업일자'] is None:
            #     print('영업일자 데이터 null error')
            #     print(tdf2.iloc[-1]['영업일자'])
            #     break

            ## case 2. sdt가 폐장일이고, 조회된 마지막 일자가 sdt 아닌 경우 (마지막일자 ~ sdt 사이기간이 100일 미만일 때)
            ## sdt ~ 조회 마지막 일자 사잇날들이 모두 폐장일이면 cont = False 로 바꾸기 !
            elif is_opened == False and delta_days < 190:   # 최대 100건 호출이므로 일반일자 기준 190일 정도 체크
                # 사잇기간 is_opened 모두 False 여야 작동하는 코드
                # while 이나 for 문으로 하나라도 True 있으면 elif 절 pass 하도록 코드 작성
                # temp = False
                cont = False
                for d in range(1, delta_days):
                    check_dt = last_dt - relativedelta(days=d)
                    if XKRX.is_session(check_dt) == True:
                        cont_edt = ldt
                        cont = True
                        # temp = True
                        break
                    else:
                        continue
                # sdt ~ ldt 사이기간 체크했는데 조회할 영업일 없으면 while문 탈출
                if cont == False: 
                    break
                # if temp == False:
                    # cont = False
            ## case 3. 조회된 마지막 일자가 sdt 아니고, 마지막일자 ~ sdt 사이기간이 100 이상일 때
            else:
                cont_edt = ldt
                cont = True

            # 호출 edt 일자 갱신
            thresh_dt = datetime.strptime(str(cont_edt), '%Y%m%d').date()
            cont_dt = thresh_dt - relativedelta(days=1)
            params['FID_INPUT_DATE_2'] = cont_dt.strftime('%Y%m%d')

            # 다음 edt 일자에 대한 호출
            t1 = super().url_fetch(url, tr_id, params)
            time.sleep(0.05)                        # REST API 초당 20건 호출 가능

            if t1.isOK() and output=='all':

                # 갱신된 다음 100 영업일 이하 기간에 대해 호출 후 기존 tdf2와 병합
                next_tdf2 = pd.DataFrame(t1.getBody().output2)
                if len(next_tdf2) == 0:
                    break
                next_tdf2 = next_tdf2.rename(columns=cf2)   # pd.DataFrame 자료형이므로 columns 이름 변경

                # 국내주식기간별시세 API 경우 res 결과 항상 정순(오름차순)만 받음.
                # if ascending=False: 이용한 역순 pd.concat() 무의미! (마지막에 .sort_index 해줘야함)
                tdf2 = pd.concat([tdf2, next_tdf2])
                # pdb.set_trace()
            else:
                t1.printError()
                return pd.DataFrame(), pd.DataFrame()

        # tsr1 = tsr1.rename(index=cf1)
        tdf2 = tdf2.set_index('영업일자')
        # ascending 인자로 False (역순) 들어온 내용은 자료 다 합친 뒤 마지막에 수행.
        tdf2 = tdf2.sort_index(ascending=ascending)
        return tsr1, tdf2 




    # !!! 상폐종목 처리 수정 필요 !!!
    # 상폐종목 API 조회시 output1 으로 0 값, output2 으로 상폐 전 기간은 값 있고, 상폐 후 기간은 Nan 값 반환됨
    # get_daily_price() 함수에서 상폐 후 기간 Nan 값 -> 0 으로 변환(?) 
    def get_daily_prices(self, stock_no_list, sdt, edt=None, freq='D', adj=1, output='all', ascending=True):
        
        opendays = get_business_days("XKRX", sdt, edt, ascending=ascending)     # 한국 주식 관련 메서드므로 주식시장 한국으로 고정 
        daily_prices = pd.DataFrame(index=opendays)
        daily_price_list = []

        for stock_no in stock_no_list:
            daily_close_price = self.get_daily_price(stock_no, sdt=sdt, edt=edt, freq=freq, adj=adj, output=output, ascending=ascending)
            daily_close_price = daily_close_price[1][['주식 종가']].dropna()
            daily_close_price = daily_close_price.rename(columns={'주식 종가': stock_no})
            # if len(daily_price) is not 0:
            #     daily_price = daily_price[1][['주식 종가']]
            #     daily_price = daily_price.rename(columns={'주식 종가': stock_no})
            # else:
            #     print("%s 종목 get_daily_price() 결과값 없음." %stock_no)
            #     continue

            #  daily_price = daily_price.reset_index()
            daily_price_list.append(daily_close_price)
            ## daily_prices = pd.concat([daily_prices, daily_close_price], axis=1)

            # daily_price = get_daily_price(stock_no, sdt=sdt, edt=edt, freq=freq, output=output, ascending=ascending)
            # daily_price = daily_price[1][['주식 종가']].rename(columns={'주식 종가': stock_no})     # 일단 column 명은 '종목코드'로
            # daily_prices = daily_prices.append(daily_price)
            time.sleep(.05)
        
        daily_prices = pd.concat([daily_prices, *daily_price_list], axis=1)
        daily_prices = daily_prices.astype(float)

        if ascending == False:
            daily_prices = daily_prices.sort_index(ascending=ascending)
        
        return daily_prices



        
    # 주문 base function
    # Input: 종목코드, 주문수량, 주문가격, Buy Flag(If True, it's Buy order), order_type="00"(지정가)
    # Output: HTTP Response

    def do_order(self, stock_code, order_qty, order_price, prd_code="01", buy_flag=True, order_type="00"):

        url = "/uapi/domestic-stock/v1/trading/order-cash"

        if buy_flag:
            tr_id = "TTTC0802U"  #buy
        else:
            tr_id = "TTTC0801U"  #sell

        params = {
            'CANO': super().getTREnv().my_acct[:8],             # [:8] 추가하여 수정
            'ACNT_PRDT_CD': prd_code, 
            'PDNO': stock_code, 
            'ORD_DVSN': order_type, 
            'ORD_QTY': str(order_qty), 
            'ORD_UNPR': str(order_price), 
            'CTAC_TLNO': '', 
            'SLL_TYPE': '01', 
            'ALGO_NO': ''
            }
        
        t1 = super().url_fetch(url, tr_id, params, postFlag=True, hashFlag=True)
        
        if t1.isOK():
            return t1
        else:
            t1.printError()
            return None

    # 사자 주문. 내부적으로는 do_order 를 호출한다.
    # Input: 종목코드, 주문수량, 주문가격
    # Output: True, False

    def do_sell(self, stock_code, order_qty, order_price, prd_code="01", order_type="00"):
        t1 = self.do_order(stock_code, order_qty, order_price, buy_flag=False, order_type=order_type)
        return t1.isOK()

    # 팔자 주문. 내부적으로는 do_order 를 호출한다.
    # Input: 종목코드, 주문수량, 주문가격
    # Output: True, False

    def do_buy(self, stock_code, order_qty, order_price, prd_code="01", order_type="00"):
        t1 = self.do_order(stock_code, order_qty, order_price, buy_flag=True, order_type=order_type)
        return t1.isOK()

    # 정정취소 가능한 주문 목록을 DataFrame 으로 반환
    # Input: None
    # Output: DataFrame

    def get_orders(self, prd_code='01'):
        url = "/uapi/domestic-stock/v1/trading/inquire-psbl-rvsecncl"

        tr_id = "TTTC8036R"

        params = {
            "CANO": super().getTREnv().my_acct[:8],                 # [:8] 추가하여 수정
            "ACNT_PRDT_CD": prd_code,
            "CTX_AREA_FK100": '',
            "CTX_AREA_NK100": '',
            "INQR_DVSN_1": '0',
            "INQR_DVSN_2": '0'
            }

        t1 = super().url_fetch(url, tr_id, params)    
        if t1.isOK():  
            tdf = pd.DataFrame(t1.getBody().output)
            tdf.set_index('odno', inplace=True)   
            cf1 = ['pdno', 'ord_qty', 'ord_unpr', 'ord_tmd', 'ord_gno_brno','orgn_odno']
            cf2 = ['종목코드', '주문수량', '주문가격', '시간', '주문점', '원번호']
            tdf = tdf[cf1]
            ren_dict = dict(zip(cf1, cf2))

            return tdf.rename(columns=ren_dict)
            
        else:
            t1.printError()
            return pd.DataFrame()
        

    # 특정 주문 취소(01)/정정(02)
    # Input: 주문 번호(get_orders 를 호출하여 얻은 DataFrame 의 index  column 값이 취소 가능한 주문번호임)
    #       주문점(통상 06010), 주문수량, 주문가격, 상품코드(01), 주문유형(00), 정정구분(취소-02, 정정-01)
    # Output: APIResp object

    def _do_cancel_revise(self, order_no, order_branch, order_qty, order_price, prd_code, order_dv, cncl_dv, qty_all_yn):
        url = "/uapi/domestic-stock/v1/trading/order-rvsecncl"
        
        tr_id = "TTTC0803U"

        params = {
            "CANO": super().getTREnv().my_acct[:8],                 # [:8] 추가하여 수정
            "ACNT_PRDT_CD": prd_code,
            "KRX_FWDG_ORD_ORGNO": order_branch, 
            "ORGN_ODNO": order_no,
            "ORD_DVSN": order_dv,
            "RVSE_CNCL_DVSN_CD": cncl_dv, #취소(02)
            "ORD_QTY": str(order_qty),
            "ORD_UNPR": str(order_price),
            "QTY_ALL_ORD_YN": qty_all_yn
        }

        t1 = super().url_fetch(url, tr_id, params=params, postFlag=True)  
        
        if t1.isOK():
            return t1
        else:
            t1.printError()
            return None

    # 특정 주문 취소
    # 
    def do_cancel(self, order_no, order_qty, order_price="01", order_branch='06010', prd_code='01', order_dv='00', cncl_dv='02',qty_all_yn="Y"):
        return self._do_cancel_revise(order_no, order_branch, order_qty, order_price, prd_code, order_dv, cncl_dv, qty_all_yn)

    # 특정 주문 정정
    # 
    def do_revise(self, order_no, order_qty, order_price, order_branch='06010', prd_code='01', order_dv='00', cncl_dv='01', qty_all_yn="Y"):
        return self._do_cancel_revise(order_no, order_branch, order_qty, order_price, prd_code, order_dv, cncl_dv, qty_all_yn)

    # 모든 주문 취소
    # Input: None
    # Output: None

    def do_cancel_all(self):
        tdf = self.get_orders()
        od_list = tdf.index.to_list()
        qty_list = tdf['주문수량'].to_list()
        price_list = tdf['주문가격'].to_list()
        branch_list = tdf['주문점'].to_list()
        cnt = 0
        for x in od_list:
            ar = self.do_cancel(x, qty_list[cnt], price_list[cnt], branch_list[cnt])
            cnt += 1
            print(ar.getErrorCode(), ar.getErrorMessage())
            time.sleep(.2)



    # 매수 가능(현금) 조회
    # Input: None
    # Output: 매수 가능 현금 액수
    def get_buyable_cash(self, stock_code='', qry_price=0, prd_code='01'):
        url = "/uapi/domestic-stock/v1/trading/inquire-daily-ccld"
        tr_id = "TTTC8908R"

        params = {
            "CANO": super().getTREnv().my_acct[:8],                 # [:8] 추가하여 수정
            "ACNT_PRDT_CD": prd_code,
            "PDNO": stock_code,
            "ORD_UNPR": str(qry_price),
            "ORD_DVSN": "02", 
            "CMA_EVLU_AMT_ICLD_YN": "Y", #API 설명부분 수정 필요 (YN)
            "OVRS_ICLD_YN": "N"
        }

        t1 = super().url_fetch(url, tr_id, params)

        if t1.isOK():
            return int(t1.getBody().output['ord_psbl_cash'])
        else:
            t1.printError()
            return 0





    # 시세 Function

    # 주식현재가 체결 : 종목별 체결 Data
    # Input: 종목코드
    # Output: 체결 Data DataFrame
    # 주식체결시간, 주식현재가, 전일대비, 전일대비부호, 체결거래량, 당일 체결강도, 전일대비율
    def get_stock_completed(self, stock_no):
        url = "/uapi/domestic-stock/v1/quotations/inquire-ccnl"
        
        tr_id = "FHKST01010300"

        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": stock_no
        }

        t1 = super().url_fetch(url, tr_id, params)
        
        if t1.isOK():
            return pd.DataFrame(t1.getBody().output)
        else:
            t1.printError()
            return pd.DataFrame()

    # 종목별 history data (현재 기준 30개만 조회 가능)
    # Input: 종목코드, 구분(D, W, M 기본값은 D)
    # output: 시세 History DataFrame
    def get_stock_history(self, stock_no, gb_cd='D'): 
        url = "/uapi/domestic-stock/v1/quotations/inquire-daily-price"
        tr_id = "FHKST01010400"

        params = {
            "FID_COND_MRKT_DIV_CODE": self._getStockDiv(stock_no),
            "FID_INPUT_ISCD": stock_no,
            "FID_PERIOD_DIV_CODE": gb_cd, 
            "FID_ORG_ADJ_PRC": "0000000001"     # 0: 수정주가반영, 1: 수정주가미반영 (0 으로 설정 필요!)
        }

        t1 = super().url_fetch(url, tr_id, params)
        
        if t1.isOK():
            return pd.DataFrame(t1.getBody().output)
        else:
            t1.printError()
            return pd.DataFrame()

    # 종목별 history data 를 표준 OHLCV DataFrame 으로 반환
    # Input: 종목코드, 구분(D, W, M 기본값은 D), (Option)adVar 을 True 로 설정하면
    #        OHLCV 외에 inter_volatile 과 pct_change 를 추가로 반환한다.
    # output: 시세 History OHLCV DataFrame
    def get_stock_history_by_ohlcv(self, stock_no, gb_cd='D', adVar=False):
        hdf1 = self.get_stock_history(stock_no, gb_cd)
        
        chosend_fld = ['stck_bsop_date', 'stck_oprc', 'stck_hgpr', 'stck_lwpr', 'stck_clpr', 'acml_vol']
        renamed_fld = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
        
        hdf1 = hdf1[chosend_fld]
        ren_dict = dict()
        i = 0
        for x in chosend_fld:
            ren_dict[x] = renamed_fld[i]
            i += 1
        
        hdf1.rename(columns = ren_dict, inplace=True)
        hdf1[['Date']] = hdf1[['Date']].apply(pd.to_datetime)  
        hdf1[['Open','High','Low','Close','Volume']] = hdf1[['Open','High','Low','Close','Volume']].apply(pd.to_numeric)  
        hdf1.set_index('Date', inplace=True)
        
        if(adVar):
            hdf1['inter_volatile'] = (hdf1['High']-hdf1['Low'])/hdf1['Close'] 
            hdf1['pct_change'] = (hdf1['Close'] - hdf1['Close'].shift(-1))/hdf1['Close'].shift(-1) * 100

        
        return hdf1

    
    # 투자자별 매매 동향
    # Input: 종목코드
    # output: 매매 동향 History DataFrame (Date, PerBuy, ForBuy, OrgBuy) 30개 row를 반환
    def get_stock_investor(self, stock_no):
        url = "/uapi/domestic-stock/v1/quotations/inquire-investor"
        tr_id = "FHKST01010900"

        params = {
            "FID_COND_MRKT_DIV_CODE": self._getStockDiv(stock_no),
            "FID_INPUT_ISCD": stock_no
        }

        t1 = super().url_fetch(url, tr_id, params)
        
        if t1.isOK():
            hdf1 = pd.DataFrame(t1.getBody().output)
            
            chosend_fld = ['stck_bsop_date', 'prsn_ntby_qty', 'frgn_ntby_qty', 'orgn_ntby_qty']
            renamed_fld = ['Date', 'PerBuy', 'ForBuy', 'OrgBuy']
            
            hdf1 = hdf1[chosend_fld]
            ren_dict = dict()
            i = 0
            for x in chosend_fld:
                ren_dict[x] = renamed_fld[i]
                i += 1
            
            hdf1.rename(columns = ren_dict, inplace=True)
            hdf1[['Date']] = hdf1[['Date']].apply(pd.to_datetime)  
            hdf1[['PerBuy','ForBuy','OrgBuy']] = hdf1[['PerBuy','ForBuy','OrgBuy']].apply(pd.to_numeric) 
            hdf1['EtcBuy'] = (hdf1['PerBuy'] + hdf1['ForBuy'] + hdf1['OrgBuy']) * -1
            hdf1.set_index('Date', inplace=True)
            #sum을 맨 마지막에 추가하는 경우
            #tdf.append(tdf.sum(numeric_only=True), ignore_index=True) <- index를 없애고  만드는 경우
            #tdf.loc['Total'] = tdf.sum() <- index 에 Total 을 추가하는 경우
            return hdf1
        else:
            t1.printError()
            return pd.DataFrame()   

