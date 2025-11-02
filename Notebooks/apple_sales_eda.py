
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
for f in csv_files:
    print(f"  - {f}")

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
    print("No CSVs found—add to Data/!")

warranty_csv = csv_files[4] if csv_files else None
if warranty_csv:
    df_warranty = pd.read_csv(f'{data_path}/{warranty_csv}')
    print(f"\n {warranty_csv} Preview:")
    print("Shape:", df_warranty.shape)
    print("Columns:", df_warranty.columns.tolist())
    print(df_warranty.head())
else:
    print("No CSVs found—add to Data/!")


# %%
# Load MAIN sales data
df_sales = pd.read_csv(f'{data_path}/sales.csv')
con = db.connect()
con.register('sales', df_sales)
con.register('products', pd.read_csv(f'{data_path}/products.csv'))
con.register('category', pd.read_csv(f'{data_path}/category.csv'))
con.register('stores', pd.read_csv(f'{data_path}/stores.csv'))

# SQL: Top Products
top_products = con.execute("""
SELECT 
  p.Product_Name,
  c.category_name,
  SUM(s.quantity * p.Price) as Total_Revenue,
  COUNT(*) as Units_Sold
FROM sales s
JOIN products p ON s.product_id = p.Product_ID
JOIN category c ON p.Category_ID = c.category_id
GROUP BY p.Product_Name, c.category_name
ORDER BY Total_Revenue DESC
LIMIT 10
""").fetchdf()

print("Top 10 Products by Revenue:")
top_products
# %%
