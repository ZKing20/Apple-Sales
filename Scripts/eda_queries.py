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
# Revenue Performance

# %%
# (1.a/b) Top N Products by Global Revenue
def load_products_revenue(limit: int, sort_order: str):
    df = con.execute(f"""
        SELECT 
            p.Product_Name,
            c.category_name,
            SUM(s.quantity * p.Price) / 1000000 AS Total_Revenue_Millions,
            COUNT(*) AS Units_Sold
        FROM 
            sales s
        JOIN 
            products p ON s.product_id = p.Product_ID
        JOIN 
            category c ON p.Category_ID = c.category_id
        GROUP BY
            p.Product_ID, 
            c.category_id,
            p.Product_Name, 
            c.category_name
        ORDER BY
            Total_Revenue_Millions {sort_order}
        LIMIT {limit}
    """).fetchdf()
    return df

# %%
# (1.c) Top N Products by Revenue and Region
def load_products_revenue_region(limit: int, sort_order: str, region: str):
    df = con.execute(f"""
        SELECT 
            p.Product_Name,
            c.category_name,
            st.Region,
            SUM(s.quantity * p.Price) / 1000000 AS Total_Revenue_Millions,
            COUNT(*) as Units_Sold
        FROM 
            sales s
        JOIN 
            products p ON s.product_id = p.Product_ID
        JOIN 
            stores st ON s.store_id = st.Store_ID
        JOIN 
            category c ON p.Category_ID = c.category_id
        WHERE 
            st.Region = '{region}'
        GROUP BY
            p.Product_ID, 
            c.category_id,
            st.Region,
            p.Product_Name, 
            c.category_name
        ORDER BY
            Total_Revenue_Millions {sort_order}
        LIMIT {limit}
    """).fetchdf()
    return df

# %%
# (2.a) Top N Countries by Monthly Revenue
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

#%%
# (2.b) Top N stores by Monthly Revenue
def load_stores_monthly_revenue(limit:int, sort_order: str):
    df = con.execute(f"""
        WITH store_totals AS (
            SELECT
                st.Store_ID,
                st.Store_Name,
                st.Region,
                SUM(s.quantity * p.Price) AS Total_Revenue
            FROM
                sales s
            JOIN 
                products p ON s.product_id = p.Product_ID
            JOIN 
                stores st ON s.store_id = st.Store_ID
            GROUP BY 
                st.Store_ID,
                st.Store_Name,
                st.Region
            ORDER BY 
                Total_Revenue {sort_order}
            LIMIT {limit}
        )
        SELECT
            stt.Store_ID,
            stt.Store_Name,
            stt.Region,
            SUM(s.quantity * p.Price) AS Monthly_Revenue,
            strftime(strptime(s.sale_date, '%d-%m-%Y'), '%Y-%m') AS Year_Month
        FROM
            sales s
        LEFT JOIN 
            products p ON s.product_id = p.Product_ID
        LEFT JOIN 
            stores st ON s.store_id = st.Store_ID
        LEFT JOIN    
            store_totals stt ON st.Store_ID = stt.Store_ID
        GROUP BY
            stt.Store_ID,
            stt.Store_Name,
            stt.Region,
            Year_Month
        ORDER BY                             
            Year_Month
    """).fetchdf()
    return df

#%%
# (2.c) Regions by Monthly Revenue
def load_regions_monthly_revenue(sort_order: str):
    df = con.execute(f"""
        WITH region_totals AS (
            SELECT
                st.Region,
                SUM(s.quantity * p.Price) AS Total_Revenue
            FROM
                sales s
            LEFT JOIN 
                products p ON s.product_id = p.Product_ID
            LEFT JOIN
                stores st ON s.store_id = st.Store_ID
            GROUP BY 
                st.Region
            ORDER BY 
                Total_Revenue {sort_order}         
        )
        SELECT
            rt.Region,
            SUM(s.quantity * p.Price) AS Monthly_Revenue,
            strftime(strptime(s.sale_date, '%d-%m-%Y'), '%Y-%m') AS Year_Month
        FROM
            sales s
        LEFT JOIN 
            products p ON s.product_id = p.Product_ID
        LEFT JOIN 
            stores st ON s.store_id = st.Store_ID
        LEFT JOIN 
            region_totals rt ON st.Region = rt.Region
        GROUP BY
            rt.Region,
            Year_Month
        ORDER BY                              
            Year_Month
    """).fetchdf()
    return df

# %%
# (3.a) Revenue by Category

"""

DISCLAIMER:
This function DOES hard code in the years, which may not be generally
considered 'best practice'. I am aware of this, but have chosen to write
the function in this way solely because it is a static dataset that will
never recieve any updates for future years. Additionally, stylistically
this project tends to use Python mainly for plotting purposes, and uses
SQL to do all of the heavy lifting. I wanted to keep consistent with that
style, and avoid doing aggregations in Python when it could be done in
SQL.

""" 

def load_category_revenue_by_year(sort_order: str):
    df = con.execute(f"""
        WITH yearly AS (
            SELECT
                c.category_name,
                SUM(
                    CASE WHEN EXTRACT(YEAR FROM strptime(s.sale_date, '%d-%m-%Y')) = 2020
                    THEN s.quantity * p.Price ELSE 0 END
                ) / 1000000 AS revenue_2020,
                SUM(
                    CASE WHEN EXTRACT(YEAR FROM strptime(s.sale_date, '%d-%m-%Y')) = 2021
                    THEN s.quantity * p.Price ELSE 0 END
                ) / 1000000 AS revenue_2021,
                SUM(
                    CASE WHEN EXTRACT(YEAR FROM strptime(s.sale_date, '%d-%m-%Y')) = 2022
                    THEN s.quantity * p.Price ELSE 0 END
                ) / 1000000 AS revenue_2022,
                SUM(
                    CASE WHEN EXTRACT(YEAR FROM strptime(s.sale_date, '%d-%m-%Y')) = 2023
                    THEN s.quantity * p.Price ELSE 0 END
                ) / 1000000 AS revenue_2023,
                SUM(
                    CASE WHEN EXTRACT(YEAR FROM strptime(s.sale_date, '%d-%m-%Y')) = 2024
                    THEN s.quantity * p.Price ELSE 0 END
                ) / 1000000 AS revenue_2024
            FROM
                sales s
            JOIN 
                products p ON s.product_id = p.Product_ID
            JOIN
                category c ON p.Category_ID = c.category_id
            GROUP BY
                c.category_name
            )
        SELECT
            category_name,
            revenue_2020,
            revenue_2021,
            revenue_2022,
            revenue_2023,
            revenue_2024,
            (revenue_2020 + revenue_2021 + revenue_2022 + revenue_2023 + revenue_2024)
                AS Total_Revenue_Millions
        FROM 
            yearly
        ORDER BY
            Total_Revenue_Millions {sort_order};           
    """).fetchdf()
    return df

#%%
# (3.b) Category by Monthly Revenue
def load_category_monthly_revenue(sort_order: str):
    df = con.execute(f"""
        WITH category_totals AS (
            SELECT
                c.category_id,
                c.category_name,
                st.Country,
                SUM(s.quantity * p.Price) / 1000000 AS Total_Revenue_Millions
            FROM
                sales s
            LEFT JOIN 
                products p ON s.product_id = p.Product_ID
            LEFT JOIN 
                category c ON p.Category_ID = c.category_id
            LEFT JOIN 
                stores st ON s.store_id = st.Store_ID
            GROUP BY 
                c.category_id,
                c.category_name,
                st.Country
            ORDER BY 
                Total_Revenue_Millions {sort_order}
        )
        SELECT
            ct.category_id,
            ct.category_name,
            ct.Country,
            SUM(s.quantity * p.Price) / 1000000 AS Monthly_Revenue_Millions,
            strftime(strptime(s.sale_date, '%d-%m-%Y'), '%Y-%m') AS Year_Month
        FROM
            sales s
        LEFT JOIN 
            products p ON s.product_id = p.Product_ID
        LEFT JOIN 
            category c ON p.Category_ID= c.category_id
        LEFT JOIN    
            category_totals ct ON c.category_id = ct.category_id
        GROUP BY
            ct.category_id,
            ct.category_name,
            ct.Country,
            Year_Month
        ORDER BY                             
            Year_Month
    """).fetchdf()
    return df

#%%
# Warranty and Product Quality

#%%
# (4.a) Claims Rate by Country
def load_claims_rate_country(limit: int, sort_order: str):
    df = con.execute(f"""
        SELECT
            CAST(
                COALESCE(
                    COUNT(CASE WHEN w.repair_status = 'Completed' THEN 1 END) 
                     * 100 / NULLIF(SUM(s.quantity), 0), 
                    0
                ) AS DECIMAL(5,2)
            ) AS Claims_Rate,
            st.Region,
            st.Country
        FROM
            stores st
        LEFT JOIN
            sales s ON st.Store_ID = s.store_id
        LEFT JOIN
            warranty w ON s.sale_id = w.sale_id
        GROUP BY
            st.Region,
            st.Country
        ORDER BY
            Claims_Rate {sort_order}
        LIMIT {limit}
    """).fetchdf()
    return df

#%%
# (4.b) Claims Rate by Store
def load_claims_rate_store(limit: int, sort_order: str):
    df = con.execute(f"""
        SELECT
            CAST(
                COALESCE(
                    COUNT(CASE WHEN w.repair_status = 'Completed' THEN 1 END) 
                     * 100 / NULLIF(SUM(s.quantity), 0), 
                    0
                ) AS DECIMAL(5,2)
            ) AS Claims_Rate,
            st.Store_Name,
            st.Region
        FROM
            stores st
        LEFT JOIN
            sales s ON st.Store_ID = s.store_id
        LEFT JOIN
            warranty w ON s.sale_id = w.sale_id
        GROUP BY
            st.Store_Name,
            st.Region
        ORDER BY
            Claims_Rate {sort_order}
        LIMIT {limit}
    """).fetchdf()
    return df

# %%
# (5.a/b) Claims Rate by Product
def load_claims_rate_product(limit: int, sort_order: str):
    df = con.execute(f"""
        WITH Claims_Rate AS (
            SELECT
                p.Product_ID,
                p.Product_Name,
                c.category_name,
                SUM(s.quantity) AS Total_Sales,
                COUNT(CASE WHEN w.repair_status = 'Completed' THEN 1 END) AS Completed_Claims
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
            Product_ID,
            Product_Name,
            category_name,
            Completed_Claims,
            CAST(
                COALESCE(
                    (Completed_Claims * 100) / NULLIF(Total_Sales, 0),         
                    0
                ) AS DECIMAL(5,2)
            ) AS Claims_Rate_Decimal 
        FROM
            Claims_Rate
        ORDER BY 
            Claims_Rate_Decimal {sort_order}
        LIMIT {limit}
    """).fetchdf()
    return df

# %%
# (5.c) Monthly Claims vs. Revenue by Store
def load_claims_rate_vs_units():
    df = con.execute("""
        WITH Claims_Rate AS (
            SELECT
                p.Product_ID,
                p.Product_Name,
                c.category_name,
                SUM(s.quantity) AS Total_Sales,
                COUNT(CASE WHEN w.repair_status = 'Completed' THEN 1 END) AS Completed_Claims
            FROM
                products p
            LEFT JOIN
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
            Product_Name,
            category_name,
            Total_Sales,
            CAST(
                COALESCE(
                    (Completed_Claims * 100) / NULLIF(Total_Sales, 0),         
                    0
                ) AS DECIMAL(5,2)
            ) AS Claims_Rate_Decimal 
        FROM
            Claims_Rate
        ORDER BY 
            Claims_Rate_Decimal
    """).fetchdf()
    return df

#%%
#  (6.a) Claims Rate by Product and Store
def load_claims_rate_products_store(sort_order: str = 'ASC'):
    df = con.execute(f"""
        WITH Claims_Rate AS (
            SELECT
                st.Store_ID,
                st.Store_Name,
                st.Country,
                p.product_ID,
                p.product_Name,
                SUM(s.quantity) AS Total_Sales,
                COUNT(CASE WHEN w.repair_status = 'Completed' THEN 1 END) AS Completed_Claims
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
                st.Country,
                p.Product_ID,
                p.Product_Name
        )
        SELECT
            Store_ID,
            Store_Name,
            product_ID,
            product_Name,
            CAST(
                COALESCE(
                    (Completed_Claims * 100) / NULLIF(Total_Sales, 0),         
                    0
                ) AS DECIMAL(5,2)
            ) AS Claims_Rate_Decimal
        FROM
            Claims_Rate
        ORDER BY 
            Claims_Rate_Decimal {sort_order}
    """).fetchdf()
    return df

#%%
# (6.b) Outlier Table
def load_outliers(min_units: int, limit: int):
    df = con.execute(f"""
        SELECT
            st.Store_Name,
            p.Product_Name,
            st.Region,
            CAST(SUM(s.quantity) AS INT) AS Units_Sold,
            COUNT(CASE WHEN w.repair_status = 'Completed' THEN 1 END) AS Claims_Count,
            CAST(
                (COUNT(CASE WHEN w.repair_status = 'Completed' THEN 1 END) * 100.0) / SUM(s.quantity)
                AS DECIMAL(5,2)
                ) AS Claims_Rate_Decimal
        FROM
            sales s
        JOIN
            stores st ON s.store_id = st.Store_ID
        JOIN
            products p ON s.product_id = p.Product_ID
        LEFT JOIN
            warranty w ON s.sale_id = w.sale_id
        GROUP BY 
            st.Store_Name,
            p.Product_Name,
            st.Region
        HAVING
            Units_Sold >= {min_units}
        ORDER BY
            Claims_Rate_Decimal DESC
        LIMIT {limit}
    """).fetchdf()
    return df

#%%
# Revenue vs Quality

#%%
# (7.a) Claims Rate per Million by Store
def load_claims_vs_revenue_by_store(limit: int, sort_order: str):
    df = con.execute(f"""
        WITH claims_per_revenue AS (
            SELECT
                st.Store_ID,
                st.Store_Name,
                st.Country,
                st.Region,
                SUM(s.quantity * p.Price) AS Total_Revenue,
                COUNT(CASE WHEN w.repair_status = 'Completed' THEN 1 END) AS Completed_Claims
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
                st.Region,
                st.Country
        )
        SELECT
            Store_ID,
            Store_Name,
            Country,
            Region,
            Total_Revenue / 1000000 AS Total_Revenue,
            CAST(
                COALESCE(
                    (Completed_Claims * 1000000) / NULLIF(Total_Revenue, 0),         
                    0
                ) AS DECIMAL(6,2)
            ) AS claims_per_million
        FROM
            claims_per_revenue           
        ORDER BY 
            claims_per_million {sort_order}
        LIMIT {limit}         
    """).fetchdf()
    return df

# %%
# (7.b) Monthly Claims vs. Revenue by Store
def load_monthly_claims_revenue_by_store():
    df = con.execute("""
        SELECT
            st.Store_Name,
            st.Region,
            strftime(strptime(s.sale_date, '%d-%m-%Y'), '%Y') AS Year,
            strftime(strptime(s.sale_date, '%d-%m-%Y'), '%Y-%m') AS Year_Month,
            SUM(s.quantity * p.Price) / 1000000 AS revenue_millions,
            SUM(s.quantity) AS Total_Sales,
            CAST(
                1000000 * COUNT(CASE WHEN w.repair_status = 'Completed' THEN 1 END)
                     / NULLIF(SUM(s.quantity * p.Price), 0) AS DECIMAL(6,2)
                ) AS claims_per_million
        FROM 
            sales s
        JOIN 
            products p ON s.product_id = p.Product_ID
        JOIN 
            stores st ON s.store_id = st.Store_ID
        LEFT JOIN 
            warranty w ON s.sale_id = w.sale_id
        GROUP BY 
            st.Store_Name,
            st.Region,
            Year,
            Year_Month    
        ORDER BY 
            claims_per_million,
            Year,
            Year_Month
    """).fetchdf()
    return df

#%%
# Testing
if __name__ == '__main__':
    None