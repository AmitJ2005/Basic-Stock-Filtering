from sqlalchemy import create_engine, select, MetaData, Table
from sqlalchemy.sql import and_
import streamlit as st

# Connect to the SQLite database
engine = create_engine('sqlite:///financial_data.db')
metadata = MetaData()

# Reflect the existing database into a new model
financials_table = Table('financials', metadata, autoload_with=engine)

def filter_stocks(free_cash_flow_threshold, operating_cash_flow_threshold):
    with engine.connect() as conn:
        stmt = select(financials_table).where(
            and_(
                financials_table.c.free_cash_flow < free_cash_flow_threshold,
                financials_table.c.operating_cash_flow < operating_cash_flow_threshold
            )
        )
        result = conn.execute(stmt)

        # Access rows using _mapping to convert rows to dictionary-like objects
        filtered_stocks = [row._mapping for row in result]
        return filtered_stocks

# Streamlit interface
st.title('Stock Filtering Application')

# Displaying input fields, with integer values only
free_cash_flow_threshold = st.number_input('Free Cash Flow <', value=0, step=1)
operating_cash_flow_threshold = st.number_input('Operating Cash Flow <', value=0, step=1)

# Show data for debugging (minimized)
with st.expander("Show Database Contents"):
    with engine.connect() as conn:
        query = select(financials_table)
        result = conn.execute(query)
        # Convert result to list of dictionaries for debugging
        st.write("Database contents:", [dict(row._mapping) for row in result])

if st.button('Filter Stocks'):
    filtered_stocks = filter_stocks(free_cash_flow_threshold, operating_cash_flow_threshold)
    
    if filtered_stocks:
        st.markdown("### Stocks matching criteria:")
        # Prepare data for display
        display_data = [{
            'Ticker': stock['ticker'],
            'Free Cash Flow': f"{stock['free_cash_flow']:,}",
            'Operating Cash Flow': f"{stock['operating_cash_flow']:,}"
        } for stock in filtered_stocks]
        
        st.table(display_data)  # Display the filtered stocks in a table format
    else:
        st.write('No stocks match the given criteria.')