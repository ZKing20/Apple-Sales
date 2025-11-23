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
from db_connections import get_connection
con = get_connection()

# %%
# (1) Top N Products by Revenue
def load_products_revenue(limit: int, sort_order: str):
    df = con.execute(f"""
        SELECT 
            p.Product_Name,
            c.category_name,
            SUM(s.quantity * p.Price) as Total_Revenue,
            COUNT(*) as Units_Sold
        FROM 
            sales s
        JOIN 
            products p ON s.product_id = p.Product_ID
        JOIN 
            category c ON p.Category_ID = c.category_id
        GROUP BY
            p.Product_ID, c.category_id,
            p.Product_Name, 
            c.category_name
        ORDER BY
            Total_Revenue {sort_order}
        LIMIT {limit}
    """).fetchdf()
    return df

# %%
# (2) Top N Countries by Monthly Revenue
def load_country_monthly_revenue(limit:int, sort_order: str):
    df = con.execute(f"""
        WITH country_totals AS (
            SELECT
                st.Country,
                SUM(s.quantity * p.Price) AS Total_Revenue
            FROM
                sales s
            JOIN 
                products p ON s.product_id = p.Product_ID
            JOIN 
                stores st ON s.store_id = st.Store_ID
            GROUP BY 
                st.Country
            ORDER BY 
                Total_Revenue {sort_order}
            LIMIT {limit}
        )
        SELECT
            ct.Country,
            SUM(s.quantity * p.Price) AS Monthly_Revenue,
            strftime(strptime(s.sale_date, '%d-%m-%Y'), '%Y-%m') AS Year_Month
        FROM
            sales s
        JOIN 
            products p ON s.product_id = p.Product_ID
        JOIN 
            stores st ON s.store_id = st.Store_ID
        JOIN    
            country_totals ct ON st.Country = ct.Country
        GROUP BY
            ct.Country,
            Year_Month
        ORDER BY                             
            Year_Month
    """).fetchdf()
    return df

# %%
# (3) Revenue by Category
def load_category_revenue_by_year():
    df = con.execute("""
        SELECT
            c.category_name,
            EXTRACT(YEAR FROM strptime(s.sale_date, '%d-%m-%Y')) AS year,
            SUM(s.quantity * p.price) AS revenue
        FROM 
            sales s
        JOIN 
            products p ON s.product_id = p.Product_ID
        JOIN 
            category c ON p.Category_ID = c.category_id
        GROUP BY 
            c.category_name, 
            EXTRACT(YEAR FROM strptime(s.sale_date, '%d-%m-%Y'))
        ORDER BY
            c.category_name,
            year;            
    """).fetchdf()
    return df

# %%
# (3.5) Top N Stores by Monthly Revenue
def load_stores_monthly_revenue(limit: int, sort_order: str):
    df = con.execute(f"""
        WITH store_totals AS (
            SELECT
                st.Store_ID,
                st.Store_Name,
                SUM(s.quantity * p.Price) AS Total_Revenue
            FROM
                sales s
            JOIN 
                products p ON s.product_id = p.Product_ID
            JOIN
                stores st ON s.store_id = st.Store_ID
            GROUP BY 
                st.Store_ID,
                st.Store_Name
            ORDER BY Total_Revenue {sort_order}
            LIMIT {limit}         
        )
        SELECT
            stt.Store_ID,
            stt.Store_Name,
            SUM(s.quantity * p.Price) AS Monthly_Revenue,
            strftime(strptime(s.sale_date, '%d-%m-%Y'), '%Y-%m') AS Year_Month
        FROM
            sales s
        JOIN 
            products p ON s.product_id = p.Product_ID
        JOIN 
            stores st ON s.store_id = st.Store_ID
        JOIN 
            store_totals stt ON st.Store_ID = stt.Store_ID
        GROUP BY
            stt.Store_ID,
            stt.Store_Name,
            Year_Month
        ORDER BY                              
            Year_Month
        """).fetchdf()
    return df

#%%

# Missing (4) 

#%%
# (5) Claims Rate by Revenue and Store
def load_claims_vs_revenue_by_store(sort_order: str):
    df = con.execute(f"""
        SELECT
            st.Store_ID,
            st.Store_Name,
            st.Country,
            SUM(s.quantity * p.Price) AS Total_Revenue,
            SUM(s.quantity) AS Units_Sold,
            COUNT(w.claim_id) AS Claims_Count
        FROM
            stores st
        LEFT JOIN
            sales s ON st.Store_ID = s.store_id
        LEFT JOIN
            products p ON s.product_id = p.Product_ID
        LEFT JOIN
            warranty w ON s.sale_id = w.sale_id
        GROUP BY
            st.Store_ID,
            st.Store_Name,
            st.Country
        ORDER BY 
            Claims_Count {sort_order}                 
    """).fetchdf()
    return df

# %%
# (6) Claims Rate by Product
def load_claims_rate_product(limit: int, sort_order: str = 'DESC'):
    df = con.execute(f"""
        WITH Claims_Rate AS (
        SELECT
            p.Product_ID,
            p.Product_Name,
            c.category_name,
            CAST(100 * COUNT(w.claim_id) / SUM(s.quantity) AS DECIMAL(4,2)) AS Claims_Rate
        FROM
            products p
        JOIN
            category c ON p.Category_ID = c.category_id
        LEFT JOIN 
            sales s ON p.Product_ID = s.product_id
        LEFT JOIN
            warranty w ON s.sale_id = w.sale_id
        GROUP BY
            p.Product_ID,
            p.Product_Name,
            c.category_name
        )
        SELECT
            cr.Product_ID,
            cr.Product_Name,
            cr.Claims_Rate,
            cr.category_name 
        FROM
            Claims_Rate cr
        ORDER BY 
            cr.Claims_Rate {sort_order}
        LIMIT {limit}
    """).fetchdf()
    return df

#%%
#  (7) Claims Rate by Product and Store
def load_claims_rate_products_store(sort_order: str = 'ASC'):
    df = con.execute(f"""
        WITH Claims_Rate AS (
        SELECT
            st.Store_ID,
            st.Store_Name,
            p.product_ID,
            p.product_Name,
            COUNT(w.claim_id) AS Claims_Count,
            CAST(100 * COUNT(w.claim_id) / SUM(s.quantity) AS DECIMAL(4,2)) AS Claims_Rate
        FROM
            stores st
        LEFT JOIN 
            sales s ON st.Store_ID = s.store_id
        LEFT JOIN 
            products p ON s.product_id = p.Product_ID
        LEFT JOIN 
            warranty w ON s.sale_id = w.sale_id
        GROUP BY
            st.Store_ID,
            st.Store_Name,
            p.Product_ID,
            p.Product_Name
        )
        SELECT
            cr.Store_ID,
            cr.Store_Name,
            cr.Product_ID,
            cr.Product_Name,
            cr.Claims_Count,
            cr.Claims_Rate, 
            st.Country
        FROM
            Claims_Rate cr
        JOIN stores st ON cr.Store_ID = st.Store_ID
        ORDER BY 
            cr.Claims_Rate {sort_order}
    """).fetchdf()
    return df

# %%
# (8) Monthly Claims vs. Revenue by Store
def load_monthly_claims_revenue_by_store():
    df = con.execute("""
        SELECT
            st.Store_ID,
            st.Store_Name,
            st.Country,
            strftime(strptime(s.sale_date, '%d-%m-%Y'), '%Y-%m') AS Year_Month,
            SUM(s.quantity * p.price) AS revenue,
            SUM(s.quantity) AS units_sold,
            COUNT(w.claim_id) AS claim_count
        FROM 
            sales s
        JOIN 
            products p ON s.product_id = p.Product_ID
        JOIN 
            stores st ON s.store_id = st.Store_ID
        LEFT JOIN 
            warranty w ON s.sale_id = w.sale_id
        GROUP BY 
            st.Store_ID,
            st.Store_Name,
            st.Country,
            Year_Month    
        ORDER BY 
            st.Store_ID,
            Year_Month
    """).fetchdf()
    return df
