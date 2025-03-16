import uvicorn
from fastapi import FastAPI
from fastapi import Response 

# Set 'ksifapp' as a name of the app folder.
from ksifapp.common.config import conf
from ksifapp.routers.ksifapi import KSIF


app = FastAPI() 


# root page 
@app.get("/")
async def res_index(): 
    """
    `INDEX` root 경로 페이지 \n 
    """
    return {"msg": "Hello, KSIF!"}


# session auth 
@app.get("/session/{team_id}")
async def init_session(team_id): 
    """
    `AUTH` 접근하려는 계좌용 객체 생성 (객체 1회 생성시 24시간 인증 유지) \n 
    """
    globals()[team_id] = KSIF(team_id, 'stock')
    return {"msg": f"{team_id} session is initiated"} 


# acct_balance 
@app.get("/acct_balance/{team_id}")
async def res_acct_balance(team_id, output='output1'): 
    """
    `ACCT_BALANCE` 계좌 현재잔고 조회 \n 
    :param output:
    """
    kis = globals()[team_id]
    result = kis.get_acct_balance(output=output).to_json() 
    return Response(result, media_type="application/json")


# daily_stocks 
@app.get("/daily_stocks/{team_id}")
async def res_daily_stocks(team_id, sdt, edt, ascending=True, dropna=True): 
    """
    `DAILY_STOCKS` 계좌 일별주식잔고 조회 \n 
    :param sdt:
    :param edt: 
    :param ascending: 
    :dropna:
    """
    kis = globals()[team_id] 
    # Pandas DataFrame 객체의 Multiindex 부분 JSON 포맷 변환이 안되므로 reset_index() 처리 
    result = kis.get_daily_stocks(sdt, edt, ascending=ascending, dropna=dropna).reset_index().to_json() 
    return Response(result, media_type="application/json")


# daily_completes 
@app.get("/daily_completes/{team_id}")
async def res_daily_completes(team_id, sdt, edt, output='output1', ascending=True): 
    """
    `DAILY_COMPLETES` 계좌 입출내역 조회 \n 
    :param sdt: 
    :param edt:
    :param output:
    :param ascending:
    """
    kis = globals()[team_id] 
    # Pandas DataFrame 객체의 Multiindex 부분 JSON 포맷 변환이 안되므로 reset_index() 처리 
    result = kis.get_daily_completes(sdt, edt, output=output, ascending=ascending).reset_index().to_json() 
    return Response(result, media_type="application/json")


# daily_completes_us 
@app.get("/daily_completes_us/{team_id}")
async def res_daily_completes_us(team_id, sdt, edt, output='output', ascending=True): 
    """
    `DAILY_COMPLETES (US)` 계좌 체결내역 조회 (미국) \n 
    :param sdt:
    :param edt:
    :param output:
    :param ascending:
    """
    kis = globals()[team_id] 
    # Pandas DataFrame 객체의 Multiindex 부분 JSON 포맷 변환이 안되므로 reset_index() 처리 
    result = kis.get_daily_completes_us(sdt, edt, output=output, ascending=ascending).reset_index().to_json() 
    return Response(result, media_type="application/json")


# daily_acct_chgs 
@app.get("/daily_acct_chgs/{team_id}")
async def res_daily_acct_chgs(team_id, sdt, edt, ascending=True, dropna=True): 
    """
    `DAILY_ACCT_CHGS` 계좌 보유고변동내역 조회 (미국) \n 
    :param sdt:
    :param edt:
    :param ascending:
    :param dropna:
    """
    kis = globals()[team_id] 
    # Pandas DataFrame 객체의 Multiindex 부분 JSON 포맷 변환이 안되므로 reset_index() 처리 
    result = kis.get_daily_acct_chgs(sdt, edt, ascending=ascending, dropna=dropna).reset_index().to_json() 
    return Response(result, media_type="application/json")


if __name__ == "__main__": 
    uvicorn.run("main:app", host="0.0.0.0", port=8080, reload=True)
