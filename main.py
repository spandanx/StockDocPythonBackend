from datetime import timedelta
from typing import List
from fastapi import FastAPI, Request, HTTPException, status, Depends, Response, Header
from pydantic import BaseModel

# from DataProcessing.TextSummarization import TextSummarizer
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from typing import Annotated

# from Security.OAuth2Security import
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm

from config.CommonVariables import get_settings
from config.ExtractProperties import Property
from src.mlProject.components.Chart.ChartData import ChartData
from src.mlProject.utils.StockUtils import StockUtils

from src.mlProject.components.SFTP.MLPrediction import SFTPMLPrediction
from Security.Encryption import AESCipher

origins = [
    "*"
]

properties = Property().get_property_data()

encrypter = AESCipher(get_settings().ENCODING_SALT)

sftpMLPrediction = SFTPMLPrediction(properties["sftp"]["host"], encrypter.decrypt(properties["sftp"]["username"]), encrypter.decrypt(properties["sftp"]["password"]))

chartData = ChartData(properties["stock"]["chart"]["baseurl"] + properties["stock"]["chart"]["charturl"])
stockUtils = StockUtils(properties["stock"]["chart"])

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)

auth_header = {
    "authorization": "enctoken zCBguB+DBiEFKnSu4Pwa8kUVECYBdF9x2R6/zNeh8Ann/FNSqBK/A4zw1J/Vu9V6JKJ2/rWMXywk92JPMPpDyRzHRYmcepOAK0PuEDJD+xPpdcrhylM7Fg=="
}

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

@app.get("/healthcheck-public")
async def root():
    return {"message": "I am Alive!"}

@app.get("/header-test")
async def header_test(auth: str | None = Header(default=None)):
    print("Called header_test")
    print(auth)
    return {"message": "I am Alive!"}


@app.get("/chart/")
# async def get_chart(stock_id: str, frequency: str, user_id: str, oi: str, from_date: str, to_date: str):
async def get_chart(request: Request):
    print("called get_chart()")
    stock_auth_header = {"Authorization": request.headers.get('stock_authorization')}
    print(stock_auth_header)
    query_params = request.query_params
    stock_id = query_params.get("stock_id")
    frequency = query_params.get("frequency")
    oi = query_params.get("oi")
    from_date = query_params.get("from_date")
    to_date = query_params.get("to_date")
    user_id = query_params.get("user_id")
    print(query_params)
    print(chartData.chart_base_url)
    try:
        chart_info = chartData.get_chart(stock_id=stock_id, frequency=frequency, user_id=user_id, oi=oi, from_date=from_date, to_date=to_date, auth_header=stock_auth_header)
        return chart_info#Response(content=chart_info, media_type="application/json")
    except Exception as e:
        print('Something went wrong while getting the chart')
        print(e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f'Error has occurred while getting the chart -  {e}'
        )

@app.get("/holdings/")
async def get_holdings(request: Request):
    print("called get_holdings()")
    try:
        stock_auth_header = {"Authorization": request.headers.get('stock_authorization')}
        holdings = stockUtils.get_holdings(stock_auth_header)
        print(holdings)
        return holdings
        # print(request.headers.get('stock_authorization'))
    except Exception as e:
        print('Something went wrong while getting the holdings')
        print(e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f'Error has occurred while getting the chart -  {e}'
        )

@app.get("/list-prediction-files/")
async def get_holdings(folder_path: str):
    print("called get_holdings()")
    try:
        return sftpMLPrediction.get_csv_files_in_folder(folder_path)
        # print(request.headers.get('stock_authorization'))
    except Exception as e:
        print('Something went wrong while getting the holdings')
        print(e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f'Error has occurred while getting the chart -  {e}'
        )

@app.get("/prediction/")
async def get_holdings(file_path: str):
    print("called get_holdings()")
    try:
        return Response(content=sftpMLPrediction.get_prediction(file_path), media_type='application/json')
        # print(request.headers.get('stock_authorization'))
    except Exception as e:
        print('Something went wrong while getting the holdings')
        print(e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f'Error has occurred while getting the chart -  {e}'
        )