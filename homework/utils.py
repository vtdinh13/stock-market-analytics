import pandas as pd
import requests
from io import StringIO
import re
import numpy as np
import time
import yfinance as yf




def download_ipo_data(url:str) -> pd.DataFrame:
    """ Download IPO withdrawn data"""

    headers = {
        'User-Agent': (
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/58.0.3029.110 Safari/537.3'
        )
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()

        # Wrap HTML text in StringIO to avoid deprecation warning
        # "Passing literal html to 'read_html' is deprecated and will be removed in a future version. To read from a literal string, wrap it in a 'StringIO' object."
        html_io = StringIO(response.text)
        tables = pd.read_html(html_io)
        return tables[0]
        
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
    except ValueError as ve:
        print(f"Data error: {ve}")
    except Exception as ex:
        print(f"Unexpected error: {ex}")
    return pd.DataFrame()

def calculate_average_price(r):
    
    number_pattern = re.findall(r'\$?(\d+\.\d{2})', r)
    numbers = [float(n) for n in number_pattern]
    avg = np.mean(numbers) if numbers else 0
    
    return avg

map_types = {
    'inc.': 'Inc',
    'incoporated' : 'Inc',
    'group' : 'Group',
    'holdings' : 'Holdings',
    'ltd' : 'Ltd',
    'limited' : 'Ltd',
    'holdings limited' : 'Ltd',
    'group inc': 'Inc',
    'acquisition':'Acq.Corp'
}

def fetch_ticker_data(ticker_list: list) -> pd.DataFrame:
    frames = []
    for i, ticker in enumerate(ticker_list):
        ticker_object = yf.Ticker(ticker)
        hist = ticker_object.history(
            period='max', 
            interval='1d'
        )
        hist['Ticker'] = ticker
        hist['growth_252d'] = hist['Close'] / hist['Close'].shift(252)
        hist['volatility'] = hist['Close'].rolling(30).std() * np.sqrt(252)
        hist['Sharpe'] = (hist['growth_252d'] - 0.045) / hist['volatility']


        frames.append(hist)
        time.sleep(1)

    df_all = pd.concat(frames)
    return df_all