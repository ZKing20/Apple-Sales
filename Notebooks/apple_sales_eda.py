
# ---
# jupyter:
#   jupytext:
#     formats: py:percent,ipynb
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.18.1
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %%
import pandas as pd
import os
import duckdb as db

# List CSVs in Data
data_path = os.path.join(os.path.dirname(__file__), '..', 'Data')
csv_files = [f for f in os.listdir(data_path) if f.endswith('.csv')]

print("Your Apple Sales CSVs:")
if csv_files:    
    for f in csv_files:
        print(f"  - {f}")
else:
    print("No .csv files found")

#%%
# Preview CSVs
category_csv = csv_files[0] if csv_files else None
if category_csv:
    df_category = pd.read_csv(f'{data_path}/{category_csv}')
    print(f"\n {category_csv} Preview:")
    print("Shape:", df_category.shape)
    print("Columns:", df_category.columns.tolist())
    print(df_category.head())
else:
    print("No category CSV found!")


products_csv = csv_files[1] if csv_files else None
if products_csv:
    df_products = pd.read_csv(f'{data_path}/{products_csv}')
    print(f"\n {products_csv} Preview:")
    print("Shape:", df_products.shape)
    print("Columns:", df_products.columns.tolist())
    print(df_products.head())
else:
    print("No product CSV found!")

sales_csv = csv_files[2] if csv_files else None
if sales_csv:
    df_sales = pd.read_csv(f'{data_path}/{sales_csv}')
    print(f"\n {sales_csv} Preview:")
    print("Shape:", df_sales.shape)
    print("Columns:", df_sales.columns.tolist())
    print(df_sales.head())
else:
    print("No Sales CSV found!")

stores_csv = csv_files[3] if csv_files else None
if stores_csv:
    df_stores = pd.read_csv(f'{data_path}/{stores_csv}')
    print(f"\n {stores_csv} Preview:")
    print("Shape:", df_stores.shape)
    print("Columns:", df_stores.columns.tolist())
    print(df_stores.head())
else:
    print("No Stores CSV found!")

warranty_csv = csv_files[4] if csv_files else None
if warranty_csv:
    df_warranty = pd.read_csv(f'{data_path}/{warranty_csv}')
    print(f"\n {warranty_csv} Preview:")
    print("Shape:", df_warranty.shape)
    print("Columns:", df_warranty.columns.tolist())
    print(df_warranty.head())
else:
    print("No Warranty CSV found!")


# %%
# Load MAIN sales data
df_sales = pd.read_csv(f'{data_path}/sales.csv')
con = db.connect()
con.register('sales', df_sales)
con.register('products', pd.read_csv(f'{data_path}/products.csv'))
con.register('category', pd.read_csv(f'{data_path}/category.csv'))
con.register('stores', pd.read_csv(f'{data_path}/stores.csv'))
con.register('warranty', pd.read_csv(f'{data_path}/warranty.csv'))

# %%

# SQL: Top/Bottom Products
top_products_revenue = con.execute("""
    SELECT 
    p.Product_Name,
    c.category_name,
    SUM(s.quantity * p.Price) as Total_Revenue,
    COUNT(*) as Units_Sold
    FROM sales s
    JOIN products p ON s.product_id = p.Product_ID
    JOIN category c ON p.Category_ID = c.category_id
    GROUP BY p.Product_ID, c.category_id, p.Product_Name, c.category_name
    ORDER BY Total_Revenue DESC
    LIMIT 10
    """).fetchdf()

print("Top 10 Products by Revenue:")
top_products_revenue

# %%
bottom_products_revenue = con.execute("""
    SELECT 
    p.Product_Name,
    c.category_name,
    SUM(s.quantity * p.Price) as Total_Revenue,
    COUNT(*) as Units_Sold
    FROM sales s
    JOIN products p ON s.product_id = p.Product_ID
    JOIN category c ON p.Category_ID = c.category_id
    GROUP BY p.Product_ID, c.category_id, p.Product_Name, c.category_name
    ORDER BY Total_Revenue ASC
    LIMIT 10
    """).fetchdf()

print("Bottom 10 Products by Revenue:")
bottom_products_revenue
# %%
top_products_units = con.execute("""
    SELECT 
        p.Product_Name,
        c.category_name,
        SUM(s.quantity * p.Price) as Total_Revenue,
        SUM(s.quantity) as Units_Sold
    FROM sales s
    JOIN products p ON s.product_id = p.Product_ID
    JOIN category c ON p.Category_ID = c.category_id
    GROUP BY p.Product_ID, c.category_id, p.Product_Name, c.category_name
    ORDER BY Units_Sold DESC
    LIMIT 10
    """).fetchdf()

print("Top 10 Products by Units Sold:")
top_products_units

# %%
bottom_products_units = con.execute("""
    SELECT 
        p.Product_Name,
        c.category_name,
        SUM(s.quantity * p.Price) as Total_Revenue,
        SUM(s.quantity) as Units_Sold
    FROM sales s
    JOIN products p ON s.product_id = p.Product_ID
    JOIN category c ON p.Category_ID = c.category_id
    GROUP BY p.Product_ID, c.category_id, p.Product_Name, c.category_name
    ORDER BY Units_Sold ASC
    LIMIT 10
    """).fetchdf()

print("Bottom 10 Products by Units Sold:")
bottom_products_units
# %%
# SQL: Top/Bottom Stores
top_stores_revenue = con.execute("""
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
    ORDER BY Total_Revenue DESC
    LIMIT 10
    """).fetchdf()

print("Top 10 Stores by Revenue:")
top_stores_revenue

# %%
bottom_stores_revenue = con.execute("""
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
    ORDER BY Total_Revenue ASC
    LIMIT 10
    """).fetchdf()

print("Bottom 10 Stores by Revenue:")
bottom_stores_revenue
# %%
top_countries_revenue = con.execute("""
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
    ORDER BY cr.Country_Revenue DESC
    LIMIT 10
    """).fetchdf()

print("Top 10 Countries by Revenue:")
top_countries_revenue

# %%
bottom_countries_revenue = con.execute("""
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
    ORDER BY cr.Country_Revenue ASC
    LIMIT 10
    """).fetchdf()

print("Bottom 10 Countries by Revenue:")
bottom_countries_revenue

# %%
#SQL Most/Least Warranty Claims
most_warranty_claims = con.execute("""
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
        Total_Claims DESC
    LIMIT 10
    """).fetchdf()
print("Countries with the Most Amount of Warranty Claims:")
most_warranty_claims
# %%
least_warranty_claims = con.execute("""
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
        Total_Claims ASC
    LIMIT 10
    """).fetchdf()
print("Countries with the Least Amount of Warranty Claims:")
least_warranty_claims

# %%
# Time based queries
Country_Monthly_Revenue = con.execute("""
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
        Monthly_Revenue DESC,                              
        Year,
        Month
    """).fetchdf()
print("The Monthly Revenue by Country is:")
Country_Monthly_Revenue