# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
#     custom_cell_magics: kql
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.11.2
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %%
# Imports and Load sales data
import os
import pandas as pd
import duckdb as db

data_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'Data'))
def get_duckdb_connection():
    con = db.connect()
    con.register('sales', pd.read_csv(f'{data_path}/sales_cleaned.csv'))
    con.register('products', pd.read_csv(f'{data_path}/products_cleaned.csv'))
    con.register('category', pd.read_csv(f'{data_path}/category_cleaned.csv'))
    con.register('stores', pd.read_csv(f'{data_path}/stores_cleaned.csv'))
    con.register('warranty', pd.read_csv(f'{data_path}/warranty_cleaned.csv'))
    return con
# %%
# Top/Bottom Products by Revenue
def load_products_revenue(limit: int=10, sort_order: str = 'DESC'):
    con = get_duckdb_connection()
    df = con.execute(f"""
        SELECT 
        p.Product_Name,
        c.category_name,
        SUM(s.quantity * p.Price) as Total_Revenue,
        COUNT(*) as Units_Sold
        FROM sales s
        JOIN products p ON s.product_id = p.Product_ID
        JOIN category c ON p.Category_ID = c.category_id
        GROUP BY p.Product_ID, c.category_id, p.Product_Name, c.category_name
        ORDER BY Total_Revenue {sort_order}
        LIMIT {limit}
    """).fetchdf()
    con.close()
    return df

# %%
# Top/Bottom Products by units sold
def load_products_units(limit: int=10, sort_order: str = 'DESC'):
    con = get_duckdb_connection()
    df = con.execute(f"""
        SELECT 
            p.Product_Name,
            c.category_name,
            SUM(s.quantity * p.Price) as Total_Revenue,
            SUM(s.quantity) as Units_Sold
        FROM sales s
        JOIN products p ON s.product_id = p.Product_ID
        JOIN category c ON p.Category_ID = c.category_id
        GROUP BY p.Product_ID, c.category_id, p.Product_Name, c.category_name
        ORDER BY Units_Sold {sort_order}
        LIMIT {limit}
    """).fetchdf()
    con.close()
    return df

# %%
# Top/Bottom Stores by Revenue
def load_stores_revenue(limit: int=10, sort_order: str = 'DESC'):
    con = get_duckdb_connection()
    df = con.execute(f"""
        SELECT
        st.Store_ID,
        st.Store_Name,
        st.City,
        st.Country,
        SUM(s.quantity * p.Price) as Total_Revenue
        FROM stores st
        JOIN sales s ON s.store_id = st.Store_ID
        JOIN products p ON p.Product_ID = s.product_id
        GROUP BY st.Store_ID, st.Store_Name, st.City, st.Country
        ORDER BY Total_Revenue {sort_order}
        LIMIT {limit}
    """).fetchdf()
    con.close()
    return df
# %%
# Top/Bottom Countries by Revenue
def load_countries_revenue(limit: int=10, sort_order: str = 'DESC'):
    con = get_duckdb_connection()
    df = con.execute(f"""
        WITH Country_Revenue AS(
            SELECT
                st.Country,
                SUM(s.quantity * p.Price) as Country_Revenue
            FROM stores st
            JOIN sales s ON s.store_id = st.Store_ID
            JOIN products p ON p.Product_ID = s.product_id
            GROUP BY st.Country
        ),
        Total_Revenue AS (
            SELECT SUM(Country_Revenue) AS Total_Revenue
            FROM Country_Revenue
        ),
        Max_Revenue AS (
            SELECT MAX(Country_Revenue) AS Max_Revenue
            FROM Country_Revenue
        )                                 
        SELECT
            cr.Country,
            cr.Country_Revenue,
            CAST((cr.Country_Revenue * 100.0 / tr.Total_Revenue) AS DECIMAL(4,2)) AS Revenue_Percentage,
            CAST(mr.Max_Revenue - cr.Country_Revenue AS INT) AS Revenue_Difference
        FROM Country_Revenue cr
        CROSS JOIN Total_Revenue tr
        CROSS JOIN Max_Revenue mr
        ORDER BY cr.Country_Revenue {sort_order}
        LIMIT {limit}
    """).fetchdf()
    con.close()
    return df
# %%
# Most/Least Warranty Claims
def load_warranty_claims(limit: int=10,sort_order: str = 'DESC'):
    con = get_duckdb_connection()
    df = con.execute(f"""
        WITH Completed_Claims AS (
            SELECT 
                COUNT(w.claim_id) AS Completed_Claims,
                st.Country
            FROM warranty w
            JOIN sales s ON s.sale_id = w.sale_id
            JOIN stores st ON st.Store_ID = s.store_id
            WHERE w.repair_status = 'Completed'
            GROUP BY st.Country
        ),
        Pending_Claims AS (
            SELECT 
                COUNT(w.claim_id) AS Pending_Claims,
                st.Country
            FROM warranty w
            JOIN sales s ON s.sale_id = w.sale_id
            JOIN stores st ON st.Store_ID = s.store_id
            WHERE w.repair_status = 'Pending'
            GROUP BY st.Country                               
        ),
        IP_Claims AS (
            SELECT 
                COUNT(w.claim_id) AS IP_Claims,
                st.Country
            FROM warranty w
            JOIN sales s ON s.sale_id = w.sale_id
            JOIN stores st ON st.Store_ID = s.store_id
            WHERE w.repair_status = 'In Progress'
            GROUP BY st.Country                
        )
        SELECT
            COALESCE(cc.Country, pc.Country, ip.Country) as Country,
            COALESCE(cc.Completed_Claims, 0) AS Completed_Claims,
            COALESCE(pc.Pending_Claims, 0) AS Pending_Claims,
            COALESCE(ip.IP_Claims, 0) AS In_Progress_Claims,
            (COALESCE(cc.Completed_Claims, 0) + COALESCE(pc.Pending_Claims, 0) + COALESCE(ip.IP_Claims, 0)) AS Total_Claims
        FROM
            Completed_Claims cc
        FULL JOIN
            Pending_Claims pc USING (Country)
        FULL JOIN
        IP_Claims ip USING (Country)
        ORDER BY 
            Total_Claims {sort_order}
        LIMIT {limit}
    """).fetchdf()
    con.close()
    return df
# %%
# Top/Bottom Country Revenue
def load_Country_Monthly_Revenue(sort_order: str = 'DESC'):
    con = get_duckdb_connection()
    df = con.execute(f"""
        SELECT
            st.Country,
            SUM(s.quantity * p.Price) AS Monthly_Revenue,
            EXTRACT(YEAR FROM strptime(s.sale_date, '%d-%m-%Y')) AS Year,
            EXTRACT(MONTH FROM strptime(s.sale_date, '%d-%m-%Y')) AS Month
        FROM
            sales s
        JOIN products p ON s.product_id = p.Product_ID
        JOIN stores st ON s.store_id = st.Store_ID
        GROUP BY
            st.Country,
            EXTRACT(YEAR FROM strptime(s.sale_date, '%d-%m-%Y')),
            EXTRACT(MONTH FROM strptime(s.sale_date, '%d-%m-%Y'))
        ORDER BY
            Monthly_Revenue {sort_order},                              
            Year,
            Month
    """).fetchdf()
    con.close()
    return df
# %%
# Top/Bottom Store Revenue
def load_Stores_Monthly_Revenue(sort_order: str = 'DESC'):
    con = get_duckdb_connection()
    df = con.execute(f"""
        SELECT
            st.Store_ID,
            st.Store_Name,
            SUM(s.quantity * p.Price) AS Monthly_Revenue,
            EXTRACT(YEAR FROM strptime(s.sale_date, '%d-%m-%Y')) AS Year,
            EXTRACT(MONTH FROM strptime(s.sale_date, '%d-%m-%Y')) AS Month
        FROM
            sales s
        JOIN products p ON s.product_id = p.Product_ID
        JOIN stores st ON s.store_id = st.Store_ID
        GROUP BY
            st.Store_ID,
            st.Store_Name,
            EXTRACT(YEAR FROM strptime(s.sale_date, '%d-%m-%Y')),
            EXTRACT(MONTH FROM strptime(s.sale_date, '%d-%m-%Y'))
        ORDER BY
            Monthly_Revenue {sort_order},                              
            Year,
            Month
        """).fetchdf()
    con.close()
    return df
# %%
# Claims Rate by Product
def load_Claims_Rate_Product(sort_order: str = 'DESC'):
    con = get_duckdb_connection()
    df = con.execute(f"""
        WITH Claims_Rate AS (
        SELECT
            p.Product_ID,
            p.Product_Name,
            CAST(100 * COUNT(w.claim_id) / SUM(s.quantity) AS DECIMAL(4,2)) AS Claims_Rate
        FROM
            products p
        LEFT JOIN sales s ON p.Product_ID = s.product_id
        LEFT JOIN warranty w ON s.sale_id = w.sale_id
        GROUP BY
            p.Product_ID,
            p.Product_Name
        )
        SELECT
            cr.Product_ID,
            cr.Product_Name,
            cr.Claims_Rate 
        FROM
            Claims_Rate cr
        ORDER BY 
            cr.Claims_Rate {sort_order}
    """).fetchdf()
    con.close()
    return df

# %%
# Claims Rate by Store
def load_Claims_Rate_Store(sort_order: str = 'DESC'):
    con = get_duckdb_connection()
    df = con.execute(f"""
        WITH Claims_Rate AS (
        SELECT
            st.Store_ID,
            st.Store_Name,
            COUNT(w.claim_id) AS Claims_Count,
            CAST(100 * COUNT(w.claim_id) / SUM(s.quantity) AS DECIMAL(4,2)) AS Claims_Rate
        FROM
            stores st
        LEFT JOIN sales s ON st.Store_ID = s.store_id
        LEFT JOIN warranty w ON s.sale_id = w.sale_id
        GROUP BY
            st.Store_ID,
            st.Store_Name
        )
        SELECT
            cr.Store_ID,
            cr.Store_Name,
            cr.Claims_Count,
            cr.Claims_Rate, 
            st.Country
        FROM
            Claims_Rate cr
        JOIN stores st ON cr.Store_ID = st.Store_ID
        ORDER BY 
            cr.Claims_Rate {sort_order}
    """).fetchdf()
    con.close()
    return df

# %%
# Revenue by Category
def load_category_revenue_by_year():
    con = get_duckdb_connection()
    df = con.execute("""
        SELECT
            c.category_name,
            EXTRACT(YEAR FROM strptime(s.sale_date, '%d-%m-%Y')) AS year,
            SUM(s.quantity * p.price) AS revenue
        FROM sales s
        JOIN products p ON s.product_id = p.Product_ID
        JOIN category c ON p.Category_ID = c.category_id
        GROUP BY 
            c.category_name, 
            EXTRACT(YEAR FROM strptime(s.sale_date, '%d-%m-%Y'))
        ORDER BY
            c.category_name,
            year;            
    """).fetchdf()
    return df

# %%
# Monthly Claims vs. Revenue by Store
def load_monthly_claims_revenue_by_store():
    con = get_duckdb_connection()
    df = con.execute("""
        SELECT
            st.Store_ID,
            st.Store_Name,
            st.Country,
            EXTRACT(YEAR FROM strptime(s.sale_date, '%d-%m-%Y')) AS year,
            EXTRACT(MONTH FROM strptime(s.sale_date, '%d-%m-%Y')) AS month,
            SUM(s.quantity * p.price) AS revenue,
            SUM(s.quantity) AS units_sold,
            COUNT(w.claim_id) AS claim_count
        FROM sales s
        JOIN products p ON s.product_id = p.Product_ID
        JOIN stores st ON s.store_id = st.Store_ID
        LEFT JOIN warranty w ON s.sale_id = w.sale_id
        GROUP BY st.Store_ID, st.Store_Name, st.Country,
                EXTRACT(YEAR FROM strptime(s.sale_date, '%d-%m-%Y')),
                EXTRACT(MONTH FROM strptime(s.sale_date, '%d-%m-%Y'))
        ORDER BY 
            st.Store_ID,
            year,
            month
    """).fetchdf()
    con.close()
    return df