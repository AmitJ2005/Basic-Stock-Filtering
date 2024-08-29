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
            cashflow = stock.cashflow.T 
            # Fetch balance sheet data
            balance_sheet = stock.balance_sheet.T
            # Fetch income statement data
            income_statement = stock.financials.T

            # Print columns for debugging
            print(f"Columns in cash flow data for {ticker}: {cashflow.columns.tolist()}")
            print(f"Columns in balance sheet data for {ticker}: {balance_sheet.columns.tolist()}")
            print(f"Columns in income statement data for {ticker}: {income_statement.columns.tolist()}")

            # Fetch specific fields from the cash flow statement
            free_cash_flow = cashflow['Free Cash Flow'].values[0] if 'Free Cash Flow' in cashflow.columns else None
            operating_cash_flow = cashflow['Operating Cash Flow'].values[0] if 'Operating Cash Flow' in cashflow.columns else None
            if operating_cash_flow is None and 'Total Cash From Operating Activities' in cashflow.columns:
                operating_cash_flow = cashflow['Total Cash From Operating Activities'].values[0]

            # Fetch specific fields from the balance sheet
            total_assets = balance_sheet['Total Assets'].values[0] if 'Total Assets' in balance_sheet.columns else None
            total_debt = balance_sheet['Total Debt'].values[0] if 'Total Debt' in balance_sheet.columns else None
            net_debt = balance_sheet['Net Debt'].values[0] if 'Net Debt' in balance_sheet.columns else None
            working_capital = balance_sheet['Working Capital'].values[0] if 'Working Capital' in balance_sheet.columns else None

            # Fetch specific fields from the income statement
            revenue = income_statement['Total Revenue'].values[0] if 'Total Revenue' in income_statement.columns else None
            net_income = income_statement['Net Income'].values[0] if 'Net Income' in income_statement.columns else None
            gross_profit = income_statement['Gross Profit'].values[0] if 'Gross Profit' in income_statement.columns else None
            ebit = income_statement['EBIT'].values[0] if 'EBIT' in income_statement.columns else None
            normalized_ebitda = income_statement['Normalized EBITDA'].values[0] if 'Normalized EBITDA' in income_statement.columns else None

            # Append the data
            data.append({
                'ticker': ticker,
                'free_cash_flow': free_cash_flow,
                'operating_cash_flow': operating_cash_flow,
                'total_assets': total_assets,
                'total_debt': total_debt,
                'net_debt': net_debt,
                'working_capital': working_capital,
                'revenue': revenue,
                'net_income': net_income,
                'gross_profit': gross_profit,
                'ebit': ebit,
                'normalized_ebitda': normalized_ebitda
            })
        except Exception as e:
            print(f"Failed to fetch data for {ticker}: {e}")
            continue

    # Convert to DataFrame and store in the database
    df = pd.DataFrame(data)

    # Log the DataFrame before dropping rows
    print("DataFrame before dropping rows with missing data:")
    print(df)

    # Drop rows with any missing data in the specified columns
    required_columns = [
        'free_cash_flow', 'operating_cash_flow',
        'total_assets', 'total_debt', 'net_debt', 'working_capital', 
        'revenue', 'net_income', 'gross_profit', 'ebit', 'normalized_ebitda'
    ]
    df.dropna(subset=required_columns, inplace=True)

    # Log the DataFrame after dropping rows
    print("DataFrame after dropping rows with missing data:")
    print(df)

    # Store in the database
    df.to_sql('financials', engine, if_exists='replace', index=False)

if __name__ == "__main__":
    tickers = ['TATACHEM.NS', 'TATACOMM.NS', 'RELIANCE.NS', 'INFY.NS', 'TCS.NS', 'HDFCBANK.NS', 'ICICIBANK.NS', 'SBIN.NS', 'BAJFINANCE.NS', 'HINDUNILVR.NS']
    fetch_and_store_data(tickers)
    print("Data fetched and stored successfully.")
