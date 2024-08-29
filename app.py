from sqlalchemy import create_engine, select, MetaData, Table
from sqlalchemy.sql import and_
import streamlit as st

# Connect to the SQLite database
engine = create_engine('sqlite:///financial_data.db')
metadata = MetaData()

# Reflect the existing database into a new model
financials_table = Table('financials', metadata, autoload_with=engine)

def filter_stocks(criteria):
    with engine.connect() as conn:
        conditions = []
        for column, (operator, value) in criteria.items():
            if operator == 'greater_than':
                conditions.append(financials_table.c[column] > value)
            elif operator == 'less_than':
                conditions.append(financials_table.c[column] < value)
        
        stmt = select(financials_table).where(and_(*conditions))
        result = conn.execute(stmt)

        # Access rows using _mapping to convert rows to dictionary-like objects
        filtered_stocks = [row._mapping for row in result]
        return filtered_stocks

# Streamlit interface
st.title('Stock Filtering Application')

# Custom CSS to increase the width of the page
st.markdown(
    """
    <style>
    .main .block-container {
        max-width: 90%;
        padding-left: 5%;
        padding-right: 5%;
    }
    </style>
    """,
    unsafe_allow_html=True
)

# Displaying input fields and comparison options
criteria = {}

# Define the input fields and their labels
fields = [
    ('Free Cash Flow', 'free_cash_flow'),
    ('Total Assets', 'total_assets'),
    ('Operating Cash Flow', 'operating_cash_flow'),
    ('Total Debt', 'total_debt'),
    ('Net Debt', 'net_debt'),
    ('Working Capital', 'working_capital'),
    ('Revenue', 'revenue'),
    ('Net Income', 'net_income'),
    ('Gross Profit', 'gross_profit'),
    ('EBIT', 'ebit'),
    ('Normalized EBITDA', 'normalized_ebitda')
]

# Organize input fields into four columns for better layout
cols = st.columns(4)

for i, (label, key) in enumerate(fields):
    with cols[i % 4]:
        threshold = st.number_input(label, value=0, step=1)
        operator = st.radio(label, ('greater_than', 'less_than'))
        criteria[key] = (operator, threshold)

# Show data for debugging (minimized)
with st.expander("Show Database Contents"):
    with engine.connect() as conn:
        query = select(financials_table)
        result = conn.execute(query)
        # Convert result to list of dictionaries for debugging
        st.write("Database contents:", [dict(row._mapping) for row in result])

if st.button('Filter Stocks'):
    filtered_stocks = filter_stocks(criteria)
    
    if filtered_stocks:
        st.markdown("### Stocks matching criteria:")
        # Prepare data for display
        display_data = [{
            'Ticker': stock['ticker'],
            'Free Cash Flow': f"{stock['free_cash_flow']:,}",
            'Operating Cash Flow': f"{stock['operating_cash_flow']:,}",
            'Total Assets': f"{stock['total_assets']:,}",
            'Total Debt': f"{stock['total_debt']:,}",
            'Net Debt': f"{stock['net_debt']:,}",
            'Working Capital': f"{stock['working_capital']:,}",
            'Revenue': f"{stock['revenue']:,}",
            'Net Income': f"{stock['net_income']:,}",
            'Gross Profit': f"{stock['gross_profit']:,}",
            'EBIT': f"{stock['ebit']:,}",
            'Normalized EBITDA': f"{stock['normalized_ebitda']:,}"
        } for stock in filtered_stocks]
        
        st.table(display_data)  # Display the filtered stocks in a table format
    else:
        st.write('No stocks match the given criteria.')