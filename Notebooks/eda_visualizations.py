
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
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '..', 'Scripts')))

from eda_queries import *
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
# %%
# Bar Chart: Top 10 Products by Revenue
top_products_revenue = load_products_revenue()
plt.figure(figsize=(10,6))
sns.barplot(data=top_products_revenue, x='Product_Name', y='Total_Revenue')
plt.title('Top 10 Products by Revenue')
plt.xlabel('Product Name')
plt.ylabel('Total Revenue')
plt.xticks(rotation=45, ha='right')
plt.show()
# %%
# Bar Chart: Claims Rate vs. Revenue
Top_Stores_Monthly_Revenue = load_Stores_Monthly_Revenue()
store_totals = Top_Stores_Monthly_Revenue.groupby('Store_Name')['Monthly_Revenue'].sum().reset_index()
store_totals = store_totals.rename(columns={'Monthly_Revenue': 'Total_Revenue'})
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

# %%
# Scatterplot: Claims Rate vs. Total Revenue
df = load_monthly_claims_revenue_by_store()
df['Year_Month'] = df['year'].astype(int).astype(str) + '-' + df['month'].astype(int).astype(str).str.zfill(2)
df['Claims_Per_1000'] = df.apply(lambda r: (r['claim_count'] / r['revenue'] * 1000) if r['revenue'] > 0 else 0, axis = 1)
df['Claims_Per_Unit'] = df.apply(lambda r: (r['claim_count'] / r['units_sold']) if r['units_sold'] > 0 else 0, axis = 1)
snapshot = (df.groupby(['Store_ID', 'Store_Name', 'Country'], as_index=False)
                .agg({'revenue':'sum', 'claim_count':'sum', 'units_sold': 'sum'}))
snapshot['Claims_Per_1000'] = snapshot.apply(lambda r: (r['claim_count'] / r['revenue'] * 1000) if r['revenue'] > 0 else 0, axis = 1)
plt.figure(figsize=(20,10))
sns.scatterplot(data=snapshot, x='revenue', y='Claims_Per_1000',
                size='units_sold', sizes=(20,300),
                hue='Country')
plt.xlabel('Total Revenue')
plt.ylabel('Claims per $1000')
plt.title('Claims per $1000 vs Revenue by Store')
plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
plt.show()

# %%
# Line Graph: Top Store by Monthly Revenue
Top_Country_Monthly_Revenue = load_Country_Monthly_Revenue()
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
# Line Graph: Top 5 countries
top_5_countries = country_totals['Country'].head(5).tolist()
top_5_country_data = Top_Country_Monthly_Revenue[Top_Country_Monthly_Revenue['Country'].isin(top_5_countries)].copy()
top_5_country_data['Year_Month'] =top_5_country_data['Year'].astype(str) + '-' + top_5_country_data['Month'].astype(str).str.zfill(2)
top_5_country_data = top_5_country_data.sort_values('Year_Month')
plt.figure(figsize=(20,10))
sns.lineplot(data=top_5_country_data, x='Year_Month', y='Monthly_Revenue', hue = 'Country', marker = 'o')
plt.title('Monthly Revenue Trend for Top 5 Countries')
plt.xlabel('Year-Month')
plt.ylabel('Monthly Revenue')
plt.xticks(rotation=45, ha='right')
plt.grid(True)
plt.legend(title='Country', bbox_to_anchor=(1.05, 1), loc='upper left')
plt.show()

# %%
# Line Graph: Top 5 Stores
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

#%%
# Horizontal Bar Chart: Revenue by Product Type
yearly_category_revenue = load_category_revenue_by_year()
yearly_category_revenue = yearly_category_revenue.pivot(index='category_name', columns='year', values='revenue').fillna(0)
yearly_category_revenue['Total_Revenue'] = yearly_category_revenue.sum(axis=1)
yearly_category_revenue = yearly_category_revenue.sort_values('Total_Revenue', ascending=True)
yearly_category_revenue = yearly_category_revenue.drop(columns=['Total_Revenue'])
yearly_category_revenue.plot(
    kind='barh',
    stacked=True,
    figsize=(20,10)
)
plt.title('Revenue of Products by Categroy', fontsize=20, pad=20)
plt.xlabel('Total Revenue', fontsize=18)
plt.ylabel('Category', fontsize=18)
plt.grid(axis='x')
plt.legend(title='Year', bbox_to_anchor=(1.05, 1), loc='upper left')
plt.tight_layout()
plt.show()

#%%
# Heatmap: Claims Rate by Product and Store
def plot_top_10_claims_heatmap(claims_product_store):
    top_stores = (
        claims_product_store.groupby("Store_Name")["Claims_Rate"]
        .mean()
        .nlargest(10)
        .index
    )

    top_products = (
        claims_product_store.groupby("Product_Name")["Claims_Rate"]
        .mean()
        .nlargest(10)
        .index
    )
    
    filtered = claims_product_store[
        claims_product_store["Store_Name"].isin(top_stores) &
        claims_product_store["Product_Name"].isin(top_products)
    ]
    
    claims_product_store = filtered.pivot_table(
        index = 'Store_Name',
        columns = 'Product_Name',
        values = 'Claims_Rate',
        aggfunc = 'mean'
    )

    plt.figure(figsize=(20,10))

    sns.heatmap(
        claims_product_store,
        annot = True,
        fmt = '.2f',
        cmap = 'Reds',
        linewidths = .5,
        cbar_kws = {'label': 'Claims Rate (%)'}
    )

    plt.title("Claims Rate by Store and Product")
    plt.xlabel("Product")
    plt.ylabel("Store")
    plt.tight_layout()
    plt.show
claims_product_store = load_Claims_Rate_Products_Store(sort_order = 'DESC')
plot_top_10_claims_heatmap(claims_product_store)
#%%
# Testing
if __name__ =='__main__':
    None
