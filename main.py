from datetime import timedelta
from typing import List
from fastapi import FastAPI, Request, HTTPException, status, Depends, Response
from pydantic import BaseModel

# from DataProcessing.TextSummarization import TextSummarizer
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from typing import Annotated

# from Security.OAuth2Security import
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm

from config.ExtractProperties import Property
from src.mlProject.components.Chart.ChartData import ChartData
from src.mlProject.utils.StockUtils import StockUtils

origins = [
    "*"
]

properties = Property().get_property_data()

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


@app.get("/chart/")
async def get_chart(stock_id: str, frequency: str, user_id: str, oi: str, from_date: str, to_date: str):
    print("called get_chart()")
    print(chartData.chart_base_url)
    try:
        chart_info = chartData.get_chart(stock_id=stock_id, frequency=frequency, user_id=user_id, oi=oi, from_date=from_date, to_date=to_date, auth_header=auth_header)
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