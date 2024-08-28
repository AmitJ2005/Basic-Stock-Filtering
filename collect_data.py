import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine

# Create a connection to the SQLite database
engine = create_engine('sqlite:///financial_data.db')

def fetch_and_store_data(tickers):
    data = []
    for ticker in tickers:
        stock = yf.Ticker(ticker)
        try:
            # Fetch cash flow data
            cashflow = stock.cashflow.T  # Transpose to make it easier to work with

            # Print columns for debugging
            print(f"Columns in cash flow data for {ticker}: {cashflow.columns.tolist()}")

            # Fetch specific fields from the cash flow statement
            free_cash_flow = cashflow['Free Cash Flow'].values[0] if 'Free Cash Flow' in cashflow.columns else None
            operating_cash_flow = cashflow['Operating Cash Flow'].values[0] if 'Operating Cash Flow' in cashflow.columns else None
            
            if operating_cash_flow is None and 'Total Cash From Operating Activities' in cashflow.columns:
                operating_cash_flow = cashflow['Total Cash From Operating Activities'].values[0]
            
            # Append the data
            data.append({
                'ticker': ticker,
                'free_cash_flow': free_cash_flow,
                'operating_cash_flow': operating_cash_flow
            })
        except Exception as e:
            print(f"Failed to fetch data for {ticker}: {e}")
            continue

    # Convert to DataFrame and store in the database
    df = pd.DataFrame(data)
    df.to_sql('financials', engine, if_exists='replace', index=False)

if __name__ == "__main__":
    tickers = ['TATACHEM.NS', 'TATACOMM.NS', 'RELIANCE.NS', 'INFY.NS', 'TCS.NS', 'HDFCBANK.NS', 'ICICIBANK.NS', 'SBIN.NS', 'BAJFINANCE.NS', 'HINDUNILVR.NS']
    fetch_and_store_data(tickers)
    print("Data fetched and stored successfully.")
