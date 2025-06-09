import yfinance as yf
import time
import traceback
from tqdm import tqdm
import pandas as pd
from datetime import date


## Visualization ##

# Create color map to be used in all visualizations
color_map = {'Basic Materials' : '#1f77b4',
 'Communication Services' : '#ff7f0e',
 'Consumer Cyclical' : '#2ca02c',
 'Consumer Defensive' : '#d62728',
 'Energy' : '#9467bd',
 'Financial Services' : '#8c564b',
 'Healthcare' : '#e377c2',
 'Industrials' : '#7f7f7f',
 'Real Estate' :'#bcbd22',
 'Technology' : '#17becf',
 'Utilities': '#1f4e1f'
 }

color_map_countries = {
'GSPC (USA)' : '#1f77b4',
'SSE (CHN)' : '#ff7f0e',
'HSI (HKG)' : '#2ca02c',
'AXJO (AUS)' : '#d62728',
'NSEI (IND)' : '#9467bd',
'GSPTSE (CAN)' : '#8c564b',
'GDAXI (DEU)' : '#e377c2',
'FTSE (GBR)' : '#7f7f7f',
'N225 (JPN)' : '#bcbd22',
'MXX (MEX)' : '#17becf',
'BVSP (BRA)' : '#1f4e1f'
}

# Map tickers to countries
map_country = {
'^GSPC': 'GSPC (USA)',
'000001.SS': 'SSE (CHN)',
'^HSI': 'HSI (HKG)',
'^AXJO': 'AXJO (AUS)',
'^NSEI': 'NSEI (IND)',
'^GSPTSE': 'GSPTSE (CAN)',
'^GDAXI': 'GDAXI (DEU)',
'^FTSE': 'FTSE (GBR)',
'^N225': 'N225 (JPN)',
'^MXX': 'MXX (MEX)',
'^BVSP': 'BVSP (BRA)'
}

## SNP500 section ## 
def batch_generator(ticker_list, batch_size):
    """ Iterate through a list and yield one batch of tickers at a time """
    for i in range(0, len(ticker_list), batch_size):
        yield ticker_list[i:i + batch_size]

def fetch_market_cap(ticker_list:list) -> list:
    """ Connect to yfinance API and fetch marketcap info for S&P500 companies """
    results=[]
    for t in ticker_list:
            
        data = yf.Ticker(t).info
        results.append({
            'Ticker': t,
            'Sector': data.get('sector'),
            'MarketCap': data.get('marketCap')
        })
           
    return results

def download_batches_market_cap(ticker_list:list, batch_size:int) -> pd.DataFrame:
    """ Execute batch download """
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


## YTD returns and CAGR section ## 
def fetch_historical_data(ticker_list:list, start_date:date, end_date:date) -> pd.DataFrame :
    """ Make a call to yfinance to download historical data for the ticker list given """
    ticker_objects = {e: yf.Ticker(e) for e in ticker_list} # Dictionary to map ticker elements to (yfinance) ticker objects

    frames = []
    for i, obj in ticker_objects.items(): # Iterate through each ticker object and download history data for each index
        data_hist = obj.history(start=start_date, end=end_date)
        data_hist['Ticker'] = i # Add a ticker column to distinguish data among different tickers
        frames.append(data_hist)
    df = pd.concat(frames)
    df.reset_index(inplace=True) # Make data a column instead of index
    return df

def calculate_ytd_return(t1, t0):
    """ Calculate ytd return """
    return round(((t1-t0)/t0) * 100,3)

def calculate_cagr(t1, t0, n):
    """ Calculate CAGR or annualized return """
    return round((((t1/t0)**(1/n))-1)*100 ,3) 