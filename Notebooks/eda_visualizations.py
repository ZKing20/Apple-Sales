
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
# Imports
import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

from eda_queries import *
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
# %%
# Bar Chart: Top 10 Products by Revenue
top_products_revenue = load_top_products_revenue()
plt.figure(figsize=(10,6))
sns.barplot(data=top_products_revenue, x='Product_Name', y='Total_Revenue')
plt.title('Top 10 Products by Revenue')
plt.xlabel('Product Name')
plt.ylabel('Total Revenue')
plt.xticks(rotation=45, ha='right')
plt.show()
# %%
# Line Graph: Top Store by Monthly Revenue
Top_Country_Monthly_Revenue = load_Top_Country_Monthly_Revenue()
country_totals = Top_Country_Monthly_Revenue.groupby('Country')['Monthly_Revenue'].sum().reset_index()
top_country = country_totals.loc[country_totals['Monthly_Revenue'].idxmax(), 'Country']
top_country_data = Top_Country_Monthly_Revenue[Top_Country_Monthly_Revenue['Country'] == top_country].copy()
top_country_data['Year_Month'] = top_country_data['Year'].astype(str) + '-' + top_country_data['Month'].astype(str).str.zfill(2)
top_country_data = top_country_data.sort_values('Year_Month')
plt.figure(figsize=(20,10))
sns.lineplot(data=top_country_data, x='Year_Month', y='Monthly_Revenue', marker = 'o')
plt.title(f'Monthly Revenue Trend for Top Country: {top_country}')
plt.xlabel('Year-Month')
plt.ylabel('Monthly Revenue')
plt.xticks(rotation=45, ha='right')
plt.grid(True)
plt.show()

# %%
# Top 5 countries
top_5_countries = country_totals['Country'].head(5).tolist()
top_5_country_data = Top_Country_Monthly_Revenue[Top_Country_Monthly_Revenue['Country'].isin(top_5_countries)].copy()
top_5_country_data['Year_Month'] =top_5_country_data['Year'].astype(str) + '-' + top_5_country_data['Month'].astype(str).str.zfill(2)
top_5_country_data = top_5_country_data.sort_values('Year_Month')
plt.figure(figsize=(20,10))
sns.lineplot(data=top_5_country_data, x='Year_Month', y='Monthly_Revenue', hue = 'Country', marker = 'o')
plt.title('Monthly Revenue Trend for Top 5 Stores')
plt.xlabel('Year-Month')
plt.ylabel('Monthly Revenue')
plt.xticks(rotation=45, ha='right')
plt.grid(True)
plt.legend(title='Country', bbox_to_anchor=(1.05, 1), loc='upper left')
plt.show()

# %%
# Top 5 Stores
Top_Stores_Monthly_Revenue = load_Top_Stores_Monthly_Revenue()
store_totals = Top_Stores_Monthly_Revenue.groupby('Store_Name')['Monthly_Revenue'].sum().reset_index()
store_totals = store_totals.rename(columns={'Monthly_Revenue': 'Total_Revenue'})
top_5_stores = store_totals['Store_Name'].head(5).tolist()
top_5_store_data = Top_Stores_Monthly_Revenue[Top_Stores_Monthly_Revenue['Store_Name'].isin(top_5_stores)].copy()
top_5_store_data['Year_Month'] = top_5_store_data['Year'].astype(str) + '-' + top_5_store_data['Month'].astype(str).str.zfill(2)
top_5_store_data = top_5_store_data.sort_values('Year_Month')
plt.figure(figsize=(20,10))
sns.lineplot(data = top_5_store_data, x='Year_Month', y='Monthly_Revenue', hue = 'Store_Name', marker = 'o')
plt.title('Monthly Revenue Trend for Top 5 Stores')
plt.xlabel('Year-Month')
plt.ylabel('Monthly Revenue')
plt.xticks(rotation=45, ha='right')
plt.grid(True)
plt.legend(title='Store', bbox_to_anchor=(1.05, 1), loc='upper left')
plt.show()

# %%
# Claims Rate vs. Revenue
Claims_Rate_Store = load_Claims_Rate_Store()
claims_rate_store_total = (pd.merge(
    Claims_Rate_Store, store_totals, 
    on='Store_Name', how='left')
    .drop_duplicates(subset=['Store_Name'])
)
claims_rate_store_total['Claims_Per_Revenue'] = (
    claims_rate_store_total['Claims_Count'] / claims_rate_store_total['Total_Revenue'] * 1000)
claims_rate_store_total['Claims_Per_Revenue'] = claims_rate_store_total['Claims_Per_Revenue'].fillna(0)
claims_rate_store_total = claims_rate_store_total.sort_values(
    by='Claims_Per_Revenue', ascending=False
)
plt.figure(figsize=(20, 10))
sns.barplot(data=claims_rate_store_total, x='Store_Name', y='Claims_Per_Revenue', hue='Country', errorbar=None, dodge=False, palette='tab20')
plt.title('Claims per $1000 by Store')
plt.xlabel('Store Name')
plt.ylabel('Claims Per $1000')
plt.xticks(rotation=45, ha='right')
plt.legend(title='Country', bbox_to_anchor=(1.05, 1), loc='upper left')
plt.show()