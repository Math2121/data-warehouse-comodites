#import
import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()
DB_HOST = os.getenv('DB_HOST_PROD')
DB_PORT = os.getenv('DB_PORT_PROD')
DB_USER = os.getenv('DB_USER_PROD')
DB_PASS = os.getenv('DB_PASSWORD_PROD')
DB_NAME = os.getenv('DB_NAME_PROD')
DB_SCHEMA = os.getenv('DB_SCHEMA_PROD')

print(DB_PORT)
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DB_HOST)

commodities = ['CL=F', 'GC=F','SI=F']
def searchCommoditiesData(symbol, period='5d',interval='1d'):
    ticker = yf.Ticker(symbol)
    data = ticker.history(period=period, interval=interval)[['Close']]

    data['symbol'] = symbol
    return data

def searchAllDataCommodities(commodities):
    all_data = []
    for symbol in commodities:
        dados = searchCommoditiesData(symbol)
        all_data.append(dados)
    return pd.concat(all_data)


def store(df, schema="public"):
    df.to_sql('commodities', engine, if_exists='replace', index=True, index_label='Date', schema=schema)
    
    
    
    
if __name__ == '__main__':
    data_concat = searchAllDataCommodities(commodities)
    store(data_concat, schema="public")