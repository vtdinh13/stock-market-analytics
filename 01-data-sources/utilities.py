import yfinance as yf
import time
import traceback
from tqdm import tqdm
import pandas as pd


def batch_generator(ticker_list, batch_size):
    """
    Iterate through a list and yield one batch of tickers at a time
    """
    for i in range(0, len(ticker_list), batch_size):
        yield ticker_list[i:i + batch_size]

def fetch_market_cap(ticker_list:list):
    """
    Connect to yfinance API and fetch marketcap info for S&P500 companies
    """
    results=[]
    for t in ticker_list:
            
        data = yf.Ticker(t).info
        results.append({
            'Ticker': t,
            'Sector': data.get('sector'),
            'MarketCap': data.get('marketCap')
        })
           
    return results

def download_batches_market_cap(ticker_list:list, batch_size:int):
    """
    Execute batch download and returns a dataframe
    """
    results = []
    for batch in tqdm(batch_generator(ticker_list, batch_size), total=len(ticker_list)/batch_size):
        retries=3
        for i in range(retries):
            try:
                result = fetch_market_cap(batch)
                results.extend(result)
                break
            except Exception as e:
                    print(f'Exception type: {type(e).__name__}')
                    print(f'Track back report: {traceback.print_exc()}')
                    time.sleep(2 ** i)
        
    return pd.DataFrame(results)

# Create color map to be used in all visualizations
color_map = {'Basic Materials' : "#1f77b4",
 'Communication Services' : "#ff7f0e",
 'Consumer Cyclical' : "#2ca02c",
 'Consumer Defensive' : "#d62728",
 'Energy' : "#9467bd",
 'Financial Services' : "#8c564b",
 'Healthcare' : "#e377c2",
 'Industrials' : "#7f7f7f",
 'Real Estate' :"#bcbd22",
 'Technology' : "#17becf",
 'Utilities': "#1f4e1f",
 }

# Map tickers to countries
map_country = {"^GSPC" : "GSPC (United States)",
"000001.SS" : "SSE (China)",
"^HSI" : "HSI (Hong Kong)",
"^AXJO" : "AXJO (Australia)",
"^NSEI" : "NSEI (India)",
"^GSPTSE" : "GSPTSE (Canada)",
"^GDAXI" : "GDAXI (Germany)",
"^FTSE" : "FTSE (United Kingdom)",
"^N225" : "N225 (Japan)",
"^MXX" : "MXX (Mexico)",
"^BVSP" : "BVSP (Brazil)"}
