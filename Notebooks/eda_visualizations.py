
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
sys.path.append(os.path.abspath
        (os.path.join
            (os.getcwd(), '..', 'Scripts')
        )
)
from eda_queries import *
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# %%
# Revenue Performance

#%%
# (1) Bar Chart: Top N Products by Revenue
limit = 10
sort_order = 'DESC'

def plot_top_products_revenue(top_products_revenue):
    plt.figure(figsize=(10,6))
    sns.barplot(
        data = top_products_revenue,
        x = 'Product_Name',
        y = 'Total_Revenue'
    )
    plt.title(f'Top {limit} Products by Revenue')
    plt.xlabel('Product Name')
    plt.ylabel('Total Revenue')
    plt.xticks(rotation = 45, ha = 'right')
    plt.show()

top_products_revenue = load_products_revenue(limit, sort_order)
plot_top_products_revenue(top_products_revenue)

# %%
# (2) Line Graph: Top N Countries by Monthly Revenue
limit = 5
sort_order = 'DESC'
def plot_top_country_monthly_revenue(top_country_data):
    plt.figure(figsize=(20,10))
    sns.lineplot(
        data=top_country_data,
        x='Year_Month',
        y='Monthly_Revenue',
        hue = 'Country',
        marker = 'o'
    )
    plt.title(f'Monthly Revenue Trend for Top {limit} Countries')
    plt.xlabel('Year-Month')
    plt.ylabel('Monthly Revenue')
    plt.xticks(rotation=45, ha='right')
    plt.grid(True)
    plt.show()

top_country_data = load_country_monthly_revenue(limit, sort_order)
plot_top_country_monthly_revenue(top_country_data)

#%%
# (3) Horizontal Bar Chart: Revenue by Product Type
def plot_category_revenue_by_year(category_revenue):
    
    # Pivot so each category is a row and each year is a column
    category_revenue = (
        category_revenue
        .pivot(index='category_name', columns='year', values='revenue')
        .fillna(0)
    )

    # Compute total revenue per category to sort by total
    category_revenue['Total_Revenue'] = category_revenue.sum(axis=1)
    category_revenue = category_revenue.sort_values('Total_Revenue', ascending=True)
    category_revenue = category_revenue.drop(columns=['Total_Revenue'])

    # Plot
    category_revenue.plot(
    kind='barh',
    stacked=True,
    figsize=(20,10)
    )

    plt.title('Revenue of Products by Category', fontsize=20, pad=20)
    plt.xlabel('Total Revenue', fontsize=18)
    plt.ylabel('Category', fontsize=18)
    plt.grid(axis='x')
    plt.legend(title='Year', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.show()

category_revenue = load_category_revenue_by_year()
plot_category_revenue_by_year(category_revenue)

#%%
# (3.5) Line Graph: Top N Stores by Monthly Revenue
limit = 10
sort_order = 'DESC'
def plot_top_stores_monthly_revenue(top_stores_data):
    plt.figure(figsize=(20,10))
    sns.lineplot(
        data=top_stores_data,
        x='Year_Month',
        y='Monthly_Revenue',
        hue = 'Store_Name',
        marker = 'o'
    )
    plt.title(f'Monthly Revenue Trend for Top {limit} Stores')
    plt.xlabel('Year-Month')
    plt.ylabel('Monthly Revenue')
    plt.xticks(rotation=45, ha='right')
    plt.grid(True)
    plt.show()

    plt.figure(figsize=(20,10))
    sns.barplot(
        data=top_stores_data,
        x='Year_Month',
        y='Monthly_Revenue',
        hue = 'Store_Name',
    )
    plt.title(f'Monthly Revenue Trend for Top {limit} Stores')
    plt.xlabel('Year-Month')
    plt.ylabel('Monthly Revenue')
    plt.xticks(rotation=45, ha='right')
    plt.grid(True)
    plt.show()

top_stores_data = load_stores_monthly_revenue(limit, sort_order)
plot_top_stores_monthly_revenue(top_stores_data)

#%%
# Warranty and Product Quality

#%%
# (4) Missing query that counts claims by country

# %%
# (5) Bar Chart: Claims Rate vs. Revenue
limit = 10
sort_order = 'DESC'

def plot_claims_rate_vs_revenue(claims_rate_store_revenue):
    claims_rate_store_revenue['Claims_Per_1000'] = claims_rate_store_revenue.apply(
        lambda r: (r['Claims_Count'] / r['Total_Revenue'] * 1000)
        if r['Total_Revenue'] > 0 else 0,
        axis = 1
    )

    claims_rate_store_revenue = claims_rate_store_revenue.sort_values(by = 'Claims_Per_1000', ascending = False)

    plt.figure(figsize=(20,10))
    sns.barplot(
        data=claims_rate_store_revenue,
        x='Store_Name',
        y='Claims_Per_1000',
        hue='Country',
        errorbar=None,
        dodge=False,
        palette='tab20'
    )
    plt.title('Claims per $1000 by Store')
    plt.xlabel('Store Name')
    plt.ylabel('Claims Per $1000')
    plt.xticks(rotation=45, ha='right')
    plt.legend(title='Country', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.show()

claims_rate_store_revenue = load_claims_vs_revenue_by_store(limit, sort_order)
plot_claims_rate_vs_revenue(claims_rate_store_revenue)

# %%
# (6) Bar Chart: Claims Rate by Product
limit = 60
sort_order = 'DESC'

def plot_claims_rate_by_product(claims_rate_product):
    plt.figure(figsize=(20,10))
    sns.barplot(
        data = claims_rate_product,
        x = 'Product_Name',
        y = 'Claims_Rate',
        hue = 'category_name',
        errorbar = None,
        dodge = False,
        palette = 'tab10'
    )
    plt.title(f'Top {limit} Products by Claims Rate')
    plt.xlabel('Product Name')
    plt.ylabel('Claims Rate')
    plt.xticks(rotation = 45, ha = 'right')
    plt.show()

claims_rate_product = load_claims_rate_product(limit, sort_order)
plot_claims_rate_by_product(claims_rate_product)

#%%
# (7) Heatmap: Claims Rate by Product and Store
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
claims_product_store = load_claims_rate_products_store(sort_order = 'DESC')
plot_top_10_claims_heatmap(claims_product_store)

#%%
# Revenue vs Quality

# %%
# (8) Scatterplot: Claims Rate vs. Total Revenue
def plot_claims_vs_revenue_by_store(monthly_claims_revenue_by_store):
    # Aggregate by store
    monthly_claims_revenue_by_store = (
        monthly_claims_revenue_by_store.groupby(['Store_ID', 'Store_Name', 'Country'], as_index = False)
        .agg({
            'revenue': 'sum',
            'claim_count': 'sum',
            'units_sold': 'sum'
        })
    )
    # Compute claims per $1000 revenue
    monthly_claims_revenue_by_store['Claims_Per_1000'] = monthly_claims_revenue_by_store.apply(
        lambda r: (r['claim_count'] / r['revenue'] * 1000) if r['revenue'] > 0 else 0,
        axis = 1
    )
    # Plot the scatterplot
    plt.figure(figsize=(20,10))
    sns.scatterplot(
        data=monthly_claims_revenue_by_store,
        x='revenue',
        y='Claims_Per_1000',
        size='units_sold',
        sizes=(20,300),
        hue='Country'
    )
    plt.xlabel('Total Revenue')
    plt.ylabel('Claims per $1000')
    plt.title('Claims per $1000 vs Revenue by Store')
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.show()

monthly_claims_revenue_by_store = load_monthly_claims_revenue_by_store()
plot_claims_vs_revenue_by_store(monthly_claims_revenue_by_store)

#%%
# Testing
if __name__ =='__main__':
    None
