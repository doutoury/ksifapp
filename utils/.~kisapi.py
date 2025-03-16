
import json
import yaml
import requests
import time, copy
from datetime import datetime
from collections import namedtuple

import pandas as pd


### KIS API 로직 파트 
class KIS(): 

    # yaml 오픈 경로 수정 
    """
    YAML 파일 사라짐. 
    KIS YAML 파일 기반으로 아래 _cfg 변수가 활용하는거에 맞춰서 YAML 파일 생성 및 open() 경로 설정. 
    내용 : prod, paper 계좌 분류, 계좌별 이름 분류 및 KEY, TOKEN 저장. 
    YAML open 주소 변경 : 
        'C:/Users/douto/Miniconda3/envs/64/Lib/site-packages/KIS_OpenAPI/rest/kisdev_vi.yaml' 에서 
        '/mnt/volume_01/workspace/ksifapp/kisdev_vi.yaml' 로
    """
    def __init__(self): 
        with open('/home/jovyan/work/ksifapp/kisdev_vi.yaml', encoding='UTF-8') as f:
            self._cfg = yaml.load(f, Loader=yaml.FullLoader)

        self._TRENV = tuple()
        self._last_auth_time = datetime.now()
        self._autoReAuth = False
        self._DEBUG = False
        self._isPaper = True
        self._base_headers = {"Content-Type": "application/json", 
                              "Accept": "text/plain", 
                              "charset": "UTF-8", 
                              'User-Agent': self._cfg['my_agent'] 
                              }


    def _getBaseHeader(self): 
        if self._autoReAuth: self.reAuth() 
        return copy.deepcopy(self._base_headers) 


    # 
    def _setTRENV(self, cfg): 
        nt1 = namedtuple('KISEnv', ['my_app','my_sec','my_acct', 'my_prod', 'my_token', 'my_url'])
        d = {
            'my_app': cfg['my_app'],
            'my_sec': cfg['my_sec'],
            'my_acct': cfg['my_acct'],
            'my_prod': cfg['my_prod'],
            'my_token': cfg['my_token'],
            'my_url' : cfg['my_url']
        }

        # global _TRENV     # 디버깅에서 _TRENV 확인 필요하면 global 변수로 변경 
                            # _TRENV 에 'my_sec', 'my_token' 들어가므로 global 변수로 변경하면 안됨! 
        self._TRENV = nt1(**d)

    def isPaperTrading(self):
        return self._isPaper


    # setAcct() 메서드 작성 -> changeTREnv() 내에서 사용 (나중에 상속)
    # kisdev_vi.yaml 내 Team Stock Accounts -> cfg: dict 변수의 my_accct 에 할당하는 함수 

    # setAcctKey() 메서드 작성 -> auth() 내에서 사용 (나중에 상속)
    # kisdev_vi.yaml 내 Team Keys -> cfg: dict 변수의 my_app, my_sec 에 할당하는 함수 

    # 고민. svr (prod, paper) 기준, team (DAR, LIM1, ...) 기준, product (stock, bond, ..) 기준 
    # 각 기준 한대 모아놓은 함수 짤지, 각각 짤지 
    # 계좌 str regex (보단 python str methods) 이용할지 ... 
    # 이 경우, yaml 에 다 저장할 필요 없고, 8자 + 2자 로 생성하면 됨. 

    def changeAcct(self, team, product='stock', svr='prod'): 
        if svr == 'prod': 
            self._cfg['my_acct_' + product] = self._cfg[team + '_acct'] + '-' + self._cfg[product]
        elif svr == 'vps': 
            self._cfg['my_paper_' + product] = self._cfg[team + '_paper'] + '-' + self._cfg[product]


    def changeAcctKey(self, team, svr='prod'): 
        # global _isPaper       # 호출 객체 별로 _isPaper 여부 확인하도록 instance 변수로 설정.
        if svr == 'prod': 
            self._cfg['my_app'] = self._cfg[team + '_app']
            self._cfg['my_sec'] = self._cfg[team + '_sec']
            self._isPaper = False
        elif svr == 'vps': 
            self._cfg['paper_app'] = self._cfg[team + '_paper_app']
            self._cfg['paper_sec'] = self._cfg[team + '_paper_sec']
            self._isPaper = True


    def changeTREnv(self, token_key, team, product='stock', svr='prod'):
        cfg = dict()

        self.changeAcctKey(team, svr)

        # global _isPaper
        # if svr == 'prod':
        #     ak1 = 'my_app'
        #     ak2 = 'my_sec'
        #     _isPaper = False
        # elif svr == 'vps':
        #     ak1 = 'paper_app'
        #     ak2 = 'paper_sec'
        #     _isPaper = True

        cfg['my_app'] = self._cfg['my_app'] 
        cfg['my_sec'] = self._cfg['my_sec'] 

        # if svr == 'prod' and product == '01':
        #     cfg['my_acct'] = _cfg['my_acct_stock']
        # elif svr == 'prod' and product == '03':
        #     cfg['my_acct'] = _cfg['my_acct_future']
        # elif svr == 'vps' and product == '01':        
        #     cfg['my_acct'] = _cfg['my_paper_stock']
        # elif svr == 'vps' and product == '03':        
        #     cfg['my_acct'] = _cfg['my_paper_future']

        self.changeAcct(team, product, svr='prod')
        cfg['my_acct'] = self._cfg['my_acct_' + product]

        cfg['my_prod'] = self._cfg[product]
        cfg['my_token'] = token_key
        cfg['my_url'] = self._cfg[svr] 

        self._setTRENV(cfg)


    def _getResultObject(self, json_data):
        _tc_ = namedtuple('res', json_data.keys())

        return _tc_(**json_data)

    def auth(self, team, product='stock', svr='prod'):

        p = {
            "grant_type": "client_credentials",
            }
        print(svr)
        # if svr == 'prod':
        #     ak1 = 'my_app'
        #     ak2 = 'my_sec'
        # elif svr == 'vps':
        #     ak1 = 'paper_app'
        #     ak2 = 'paper_sec'
        self.changeAcctKey(team, svr)

        p["appkey"] = self._cfg['my_app'] 
        p["appsecret"] = self._cfg['my_sec'] 


        url = f'{self._cfg[svr]}/oauth2/tokenP'

        res = requests.post(url, data=json.dumps(p), headers=self._getBaseHeader())
        rescode = res.status_code
        if rescode == 200:
            my_token = self._getResultObject(res.json()).access_token
        else:
            print('Get Authentification token fail!\nYou have to restart your app!!!')  
            return
    
        self.changeTREnv(f"Bearer {my_token}", team, product, svr)

        self._base_headers["authorization"] = self._TRENV.my_token
        self._base_headers["appkey"] = self._TRENV.my_app
        self._base_headers["appsecret"] = self._TRENV.my_sec

        # global _last_auth_time      # 호출 객체 별로 _isPaper 여부 확인하도록 instance 변수로 설정.
        self._last_auth_time = datetime.now()

        if (self._DEBUG):
            print(f'[{self._last_auth_time}] => get AUTH Key completed!')

    #end of initialize
    def reAuth(self, svr='prod', product='01'):
        n2 = datetime.now()
        if (n2 - self._last_auth_time).seconds >= 86400:
            self.auth(svr, product)     # self.auth(team, product='stock', svr='prod') 해야되지 않나 (?)

    def getEnv(self):
        return self._cfg
    def getTREnv(self):
        return self._TRENV

    #주문 API에서 사용할 hash key값을 받아 header에 설정해 주는 함수
    # Input: HTTP Header, HTTP post param
    # Output: None
    def set_order_hash_key(self, h, p):
    
        url = f"{self.getTREnv().my_url}/uapi/hashkey"
    
        res = requests.post(url, data=json.dumps(p), headers=h)
        rescode = res.status_code
        if rescode == 200:
            h['hashkey'] = self._getResultObject(res.json()).HASH
        else:
            print("Error:", rescode)


# KIS 클래스 상속 필요 없는 클래스 
# API 호출 결과로 응답받은 rep 데이터 저장하는 모델 객체 ! 
class APIResp:
    def __init__(self, resp):
        self._rescode = resp.status_code
        self._resp = resp
        self._header = self._setHeader()
        self._body = self._setBody()
        self._err_code = self._body.rt_cd
        self._err_message = self._body.msg1

    def getResCode(self):
        return self._rescode   

    def _setHeader(self):
        fld = dict()
        for x in self._resp.headers.keys():
            if x.islower():
                fld[x] = self._resp.headers.get(x)
        _th_ =  namedtuple('header', fld.keys())

        return _th_(**fld)

    def _setBody(self):
        _tb_ = namedtuple('body', self._resp.json().keys())

        return  _tb_(**self._resp.json())

    def getHeader(self):
        return self._header

    def getBody(self):
        return self._body

    def getResponse(self):
        return self._resp

    def isOK(self):
        try:
            if(self.getBody().rt_cd == '0'):
                return True
            else:
                return False
        except:
            return False

    def getErrorCode(self):
        return self._err_code

    def getErrorMessage(self):
        return self._err_message

    def printAll(self):
        print("<Header>")
        for x in self.getHeader()._fields:
            print(f'\t-{x}: {getattr(self.getHeader(), x)}')
        print("<Body>")
        for x in self.getBody()._fields:        
            print(f'\t-{x}: {getattr(self.getBody(), x)}')

    def printError(self):
        print('-------------------------------\nError in response: ', self.getResCode())
        print(self.getBody().rt_cd, self.getErrorCode(), self.getErrorMessage()) 
        print('-------------------------------')           

# end of class APIResp


########### API call wrapping
# KIS 상속 필요 ??? 
# 실제로 KIS API 요청에 사용하는 함수들 모음 ! 

class KISReq(KIS): 

    def __init__(self): 
        super().__init__()

    def url_fetch(self, api_url, ptr_id, params, appendHeaders=None, postFlag=False, hashFlag=True):
        url = f"{super().getTREnv().my_url}{api_url}"

        headers = super()._getBaseHeader()

        #추가 Header 설정
        tr_id = ptr_id
        if ptr_id[0] in ('T', 'J', 'C'):
            if super().isPaperTrading():
                tr_id = 'V' + ptr_id[1:]

        headers["tr_id"] = tr_id
        headers["custtype"] = "P"

        if appendHeaders is not None:
            if len(appendHeaders) > 0:
                for x in appendHeaders.keys():
                    headers[x] = appendHeaders.get(x)

        if(self._DEBUG): 
            print("< Sending Info >")
            print(f"URL: {url}, TR: {tr_id}")
            print(f"<header>\n{headers}")
            print(f"<body>\n{params}")

        if (postFlag):
            if(hashFlag): super().set_order_hash_key(headers, params)
            res = requests.post(url, headers=headers, data=json.dumps(params))
        else:
            res = requests.get(url, headers=headers, params=params)

        if res.status_code == 200:
            ar = APIResp(res)
            if (self._DEBUG): ar.printAll()
            return ar
        else:
            print("Error Code : " + str(res.status_code) + " | " + res.text)
            return None


    # tr response JSON -> DataFrame 변환 함수
    # request 보낼 tr 과 tr 에 함께보낼 params 정보 필요 
    # _url_fetch() 로 tr 보낼 때, appendHeaders={'tr_cont': ""} 인자 추가 필요 
    # 호출 응답 데이터가 없을 경우, 빈 데이터프레임 반환(?) 
    def request_tr(self, url, tr_id, params, output1_keys=None, output2_keys=None, appendHeaders={'tr_cont': ""}): 
        tr = self.url_fetch(url, tr_id, params, appendHeaders=appendHeaders)

        if tr.isOK() and ('output' in tr.getBody()._fields or 'output1' in tr.getBody()._fields): 

            # step 1. output1 로 tdf1 저장 (연속데이터 처리)
            # 연속거래여부 (rep: F or M - 다음데이터 있음, D or E - 마지막 데이터)
            tr_cont = tr._header.tr_cont

            if 'output' in tr.getBody()._fields: 
                tdf1 = pd.DataFrame(tr.getBody().output, columns=output1_keys)
            elif 'output1' in tr.getBody()._fields: 
                tdf1 = pd.DataFrame(tr.getBody().output1, columns=output1_keys)

            if tr_cont == 'D' or tr_cont == 'E':
                pass
            elif tr_cont == 'F' or tr_cont == 'M': 
                # step 2. 다음 reponse 결과 호출위해 params 에 'CTX_AREA_FK100' 과 'CTX_AREA_NK100' 값 저장
                # _url_fetch() 메서드 이용해 '다음 데이터 조회' header 로 요청!
                while tr_cont == 'F' or tr_cont == 'M': 
                    if "CTX_AREA_FK100" in params or "CTX_AREA_NK100" in params: 
                        params["CTX_AREA_FK100"] = tr._body.ctx_area_fk100
                        params["CTX_AREA_NK100"] = tr._body.ctx_area_nk100
                    elif "CTX_AREA_FK200" in params or "CTX_AREA_NK200" in params: 
                        params["CTX_AREA_FK200"] = tr._body.ctx_area_fk200
                        params["CTX_AREA_NK200"] = tr._body.ctx_area_nk200

                    tr = self.url_fetch(url, tr_id, params, appendHeaders={'tr_cont': "N"})
                    if tr.isOK() and 'output' in tr.getBody()._fields: 
                        next_tdf1 = pd.DataFrame(tr.getBody().output, columns=output1_keys)
                        tdf1 = pd.concat([tdf1, next_tdf1])
                        tr_cont = tr._header.tr_cont
                    elif tr.isOK() and 'output1' in tr.getBody()._fields: 
                        next_tdf1 = pd.DataFrame(tr.getBody().output1, columns=output1_keys)
                        tdf1 = pd.concat([tdf1, next_tdf1])
                        tr_cont = tr._header.tr_cont
                    else: 
                        tr.printError()
        else: 
            tr.printError()
            return pd.DataFrame()

        if tr.isOK() and 'output2' in tr.getBody()._fields: 
            tdf2 = pd.DataFrame(tr.getBody().output2, index=[0], columns=output2_keys)    # DataFrame 인덱스 처리
        else: 
            return tdf1

        return tdf1, tdf2
