# imports
import duckdb
import pandas as pd
import os
from config import DATA_DIR
# Global Connection Holder

_CON = None

def get_connection():
    global _CON

    if _CON is None:
        # Initializae DuckDB connection
        _CON = duckdb.connect()

        # Register all tables
        _CON.register('sales', pd.read_csv(os.path.join(DATA_DIR, 'sales_cleaned.csv')))
        _CON.register('products', pd.read_csv(os.path.join(DATA_DIR, 'products_cleaned.csv')))
        _CON.register('category', pd.read_csv(os.path.join(DATA_DIR, 'category_cleaned.csv')))
        _CON.register('stores', pd.read_csv(os.path.join(DATA_DIR, 'stores_cleaned.csv')))
        _CON.register('warranty', pd.read_csv(os.path.join(DATA_DIR, 'warranty_cleaned.csv')))
    
    return _CON