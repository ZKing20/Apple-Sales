
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

"""

Key Question (1):
Which products generate the highest revenue, and how consistent is this 
performance across regions?

Key Question (2):
Which stores and countries contribute the most to overall revenue, and 
how do their monthly trends compare?

Key Question (3):
Which product categories show strong growth or decline over time?

"""
#%%
# (1.a) Bar Chart: Top N Products by Global Revenue (Small Scope)
consistent_categroy_palette = {
    'Accessories': '#1f77b4',             # Blue
    'Audio': '#2ca02c',                   # Green
    'Desktop': '#bcbd22',                 # Olive
    'Laptop': '#ff7f0e',                  # Orange
    'Smartphone': '#d62728',              # Red
    'Tablet': '#9467bd',                  # Purple
    'Subscription Service': "#17becf",    # Cyan
    'Wearable': '#8c564b',                # Brown
    'Streaming Device': '#e377c2',        # Pink
    'Smart Speaker': '#7f7f7f',           # Gray
}
limit = 20
sort_order = 'DESC'

def plot_top_products_revenue(top_products_revenue):
    plt.figure(figsize=(10,6))
    sns.barplot(
        data = top_products_revenue,
        x = 'Product_Name',
        y = 'Total_Revenue_Millions',
        hue = 'category_name',
        palette = consistent_categroy_palette
    )
    plt.title(f'Top {limit} Products by Revenue')
    plt.xlabel('Product Name')
    plt.ylabel('Total Revenue (in Millions)')
    plt.xticks(rotation = 45, ha = 'right')
    plt.legend(title='Category', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.show()

top_products_revenue = load_products_revenue(limit, sort_order)
plot_top_products_revenue(top_products_revenue)

"""

DESCRIPTION:
This code makes a visualization that shows the top N products 
by Revenue. This directly answers the first part of Key Question (1),
and gives us insight into what products generate the most revenue.

INSIGHT:
The visualization shows that Apple Music is actually the highest
earning product that Apple sells, and other top earners are spread
out across multiple product categories. This indicates that customers 
may be buying more into the ecosystem than individual products.

"""

#%%
# (1.b) Bar Chart: Top N Products by Global Revenue (Wide Scope)
limit = 100
sort_order = 'DESC'

def plot_top_products_revenue(top_products_revenue):
    plt.figure(figsize=(10,6))
    ax = sns.barplot(
        data = top_products_revenue,
        x = 'Product_Name',
        y = 'Total_Revenue_Millions',
        hue = 'category_name',
        palette = consistent_categroy_palette
    )
    ax.set_xlabel('')
    ax.set_xticklabels([])
    plt.title(f'Top {limit} Products by Revenue')
    plt.ylabel('Total Revenue (in Millions)')
    plt.legend(title='Category', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.show()

top_products_revenue = load_products_revenue(limit, sort_order)
plot_top_products_revenue(top_products_revenue)

"""

DESCRIPTION:
This code makes a visualization that shows the same thing as the
previous visualization, but just zoomed out wider. 

INSIGHT:
This visualization shows that there are no clear bands along category 
lines, and that product revenue generally decreases at a steady rate. 
No large jumps downwards in revenue indicates that there is no large gap
in any product(s) perceived quality by customers.

"""

#%%
# (1.c) Bar Chart: Top N Products by Revenue and Region
Regions = ['North America', 'South America', 'Europe', 'Asia', 'Middle East', 'Oceania']
limit = 20
sort_order = 'DESC'

def plot_top_products_revenue_region(top_products_revenue_region):
    plt.figure(figsize=(10,6))
    sns.barplot(
        data = top_products_revenue_region,
        x = 'Product_Name',
        y = 'Total_Revenue_Millions',
        hue = 'category_name',
        errorbar = None,
        palette = consistent_categroy_palette
    )
    plt.title(f'Top {limit} Products by Revenue ({region})')
    plt.xlabel('Product Name')
    plt.ylabel('Total Revenue (in Millions)')
    plt.xticks(rotation = 45, ha = 'right')
    plt.legend(title='Category', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.show()

for region in Regions:
    top_products_revenue_region = load_products_revenue_region(limit, sort_order, region)
    plot_top_products_revenue_region(top_products_revenue_region)

"""

DESCRIPTION:
This code produces 6 visualizations, each showing the same graph
but filtered by region. This directly answers the second part of 
Key Question (1), with regards to consistency of top revenue earners
across regions. 

INSIGHT:
These visualizations show that the top revenue generator is Apple Music 
in 3 of 6 regions, and is in the top 3 for all regions. Other standout 
products across regions include: iMac (27-inch), 
iPad Mini (9th generation), iPad (5th generation), and Beats Fit Pro.
This goes to show that (with the exception of the Beats Fit Pro), 
the top revenue generating products fit into the Apple ecosystem in 
different ways, and this is what the customers are buying.

"""

# %%
# (2.a) Line Graph: Top N Countries by Monthly Revenue
limit = 5
sort_order = 'DESC'

def plot_top_country_monthly_revenue(top_country_data):
    plt.figure(figsize=(20,10))
    sns.lineplot(
        data = top_country_data,
        x = 'Year_Month',
        y = 'Monthly_Revenue',
        hue = 'Country',
        marker = 'o'
    )
    plt.title(f'Monthly Revenue Trend for Top {limit} Countries')
    plt.xlabel('Year-Month')
    plt.ylabel('Monthly Revenue')
    plt.xticks(rotation=45, ha='right')
    plt.legend(title='Country', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.grid(True)
    plt.show()

top_country_data = load_country_monthly_revenue(limit, sort_order)
plot_top_country_monthly_revenue(top_country_data)

"""

DESCRIPTION:
This code generates a visualization that directly answers Key Question (2).
By showing monthly revenue of the top countries, this gives a clear 
picture of the trends of the top revenue generating countries.

INSIGHT:
The United States is far and away the most dominant revenue driver for 
the company.The other countries in the top 5 are all much closer 
together in this respect, but are still only half or less than half of 
the revenue generated by the U.S. China and Japan both appearing in the 
number 2 and 3 spots indicates that while the market in the U.S. may be 
saturated, there is potential room for growth in Asia.

"""

#%%
# (2.b) Top N stores by Monthly Revenue (Color coded by Region)
limit = 5
sort_order = 'DESC'

def plot_top_store_monthly_revenue(top_store_data):
    plt.figure(figsize=(20,10))
    sns.lineplot(
        data = top_store_data,
        x = 'Year_Month',
        y = 'Monthly_Revenue',
        hue = 'Region',
        marker = 'o',
        errorbar = None
    )
    plt.title(f'Monthly Revenue Trend for Top {limit} Stores')
    plt.xlabel('Year-Month')
    plt.ylabel('Monthly Revenue')
    plt.xticks(rotation=45, ha='right')
    plt.legend(title='Store', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.grid(True)
    plt.show()

top_store_data = load_stores_monthly_revenue(limit, sort_order)
plot_top_store_monthly_revenue(top_store_data)

"""

DESCRIPTION:
This code generates a visualization that directly answers Key Question (2).
By showing monthly revenue of the top stores, this gives a clear 
picture of the trends of the top revenue generating stores.

INSIGHT:
This shows that each store has fairly similar revenue, but the trends 
are not very cleanly defined; a lot of static, seemingly brownian trend 
lines indicates fairly consistent revenue but with unpredictable minor
differences on a month to month basis aside from two noticeable trends.
Revenue seems to slightly spike in every store around the holiday season
(November - December). On the other hand, revenue seems to consistently
fall in the Month of February for most if not all stores, most if not all 
years. Keep reaping the benefits of selling a lot during the holiday 
season, but maybe try a few targeted add campaigns for Valentine's Day
to offset predictably slow months.

"""
#%%
# (3.a) Horizontal Bar Chart: Revenue by Category
sort_order = 'ASC'
def plot_category_revenue_by_year(category_revenue):
    # Drop the Total_Revenue column so it's not included in the bar
    if "Total_Revenue" in category_revenue.columns:
        category_revenue = category_revenue.drop(columns = ["Total_Revenue"])
    
    # Set category names as index so they're used for y-axis
    category_revenue = category_revenue.set_index("category_name")
    
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

category_revenue = load_category_revenue_by_year(sort_order)
plot_category_revenue_by_year(category_revenue)

#%%
# (3.b) Line Graph: Top N Regions by Monthly Revenue
limit = 10
sort_order = 'DESC'
def plot_top_regions_monthly_revenue(top_regions_data):
    plt.figure(figsize=(20,10))
    sns.lineplot(
        data = top_regions_data,
        x = 'Year_Month',
        y = 'Monthly_Revenue',
        hue = 'Region',
        marker = 'o',
        errorbar = None
    )
    plt.title(f'Monthly Revenue Trend for Top {limit} Regions')
    plt.xlabel('Year-Month')
    plt.ylabel('Monthly Revenue')
    plt.xticks(rotation=45, ha='right')
    plt.legend(title='Region', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.grid(True)
    plt.show()

top_regions_data = load_regions_monthly_revenue(limit, sort_order)
plot_top_regions_monthly_revenue(top_regions_data)

#%%
# (3.c) Line Graph: Top N Stores by Monthly Revenue
limit = 10
sort_order = 'DESC'
def plot_top_stores_monthly_revenue(top_stores_data):
    plt.figure(figsize=(20,10))
    sns.lineplot(
        data = top_stores_data,
        x = 'Year_Month',
        y = 'Monthly_Revenue',
        hue = 'Store_Name',
        marker = 'o'
    )
    plt.title(f'Monthly Revenue Trend for Top {limit} Stores')
    plt.xlabel('Year-Month')
    plt.ylabel('Monthly Revenue')
    plt.xticks(rotation=45, ha='right')
    plt.legend(title='Store', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.grid(True)
    plt.show()

top_stores_data = load_stores_monthly_revenue(limit, sort_order)
plot_top_stores_monthly_revenue(top_stores_data)

#%%
# Warranty and Product Quality

"""

Key Question (4):
Which stores and regions have the highest warranty claim counts or rates?

Key Question (5):
Which products experience the highest claim rates, and how does this 
relate to their sales volume?

Key Question (6):
Are there specific product-store combinations that show unusually 
high claim activity?

"""

#%%
# (4) Bar Chart: Claims Rate by Country 
limit = 19 # Only have 19 countries
sort_order = 'DESC'

def plot_claims_rate_by_country(claims_rate_country):
    plt.figure(figsize=(20,10))
    sns.barplot(
        data = claims_rate_country,
        x = 'Country',
        y = 'Claims_Rate',
        hue = 'Region'
    )
    plt.title('Claims Rate by Country')
    plt.xlabel('Country')
    plt.ylabel('Claims Rate')
    plt.xticks(rotation=45, ha='right')
    plt.legend(title='Region', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.show()    

claims_rate_country = load_claims_rate_country(limit, sort_order)
plot_claims_rate_by_country(claims_rate_country)

# %%
# (5.a) Bar Chart: Claims Rate per Million by Store
limit = 10
sort_order = 'DESC'

def plot_claims_rate_vs_revenue(claims_rate_store_revenue):
    plt.figure(figsize=(20,10))
    sns.barplot(
        data = claims_rate_store_revenue,
        x = 'Store_Name',
        y = 'claims_per_million',
        hue = 'Region',
        errorbar = None,
        dodge = False,
        palette = 'tab10'
    )
    plt.title('Claims per $1,000,000 by Store')
    plt.xlabel('Store Name')
    plt.ylabel('Claims Per Million')
    plt.xticks(rotation=45, ha='right')
    plt.legend(title='Region', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.show()

claims_rate_store_revenue = load_claims_vs_revenue_by_store(limit, sort_order)
plot_claims_rate_vs_revenue(claims_rate_store_revenue)

# %%
# (5) Bar Chart: Claims Rate by Product
limit = 25
sort_order = 'DESC'

def plot_claims_rate_by_product(claims_rate_product):
    plt.figure(figsize=(20,10))
    sns.barplot(
        data = claims_rate_product,
        x = 'Product_Name',
        y = 'Claims_Rate_Decimal',
        hue = 'category_name',
        errorbar = None,
        dodge = False,
        palette = 'tab10'
    )
    plt.title(f'Top {limit} Products by Claims Rate')
    plt.xlabel('Product Name')
    plt.ylabel('Claims Rate (%)')
    plt.xticks(rotation = 45, ha = 'right')
    plt.legend(title='Product Category', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.show()

claims_rate_product = load_claims_rate_product(limit, sort_order)
plot_claims_rate_by_product(claims_rate_product)

#%%
# (6) Heatmap: Claims Rate by Product and Store
def plot_top_10_claims_heatmap(claims_product_store):
    top_stores = (
        claims_product_store.groupby("Store_Name")["Claims_Rate_Decimal"]
        .mean()
        .nlargest(10)
        .index
    )

    top_products = (
        claims_product_store.groupby("Product_Name")["Claims_Rate_Decimal"]
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
        values = 'Claims_Rate_Decimal',
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

"""

Key Question (7):
Is there a relationship between a store's revenue and its 
warranty claim rate?

Key Question (8):
Which stores generate strong revenue while maintaining low warranty 
claims, and what sets them apart?

"""

# %%
# (7) Bar Chart: Claims Rate per Million by Store
limit = 10
sort_order = 'DESC'

def plot_claims_rate_vs_revenue(claims_rate_store_revenue):
    plt.figure(figsize=(20,10))
    sns.barplot(
        data = claims_rate_store_revenue,
        x = 'Store_Name',
        y = 'claims_per_million',
        hue = 'Region',
        errorbar = None,
        dodge = False,
        palette = 'tab10'
    )
    plt.title('Claims per $1,000,000 by Store')
    plt.xlabel('Store Name')
    plt.ylabel('Claims Per Million')
    plt.xticks(rotation=45, ha='right')
    plt.legend(title='Region', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.show()

claims_rate_store_revenue = load_claims_vs_revenue_by_store(limit, sort_order)
plot_claims_rate_vs_revenue(claims_rate_store_revenue)

# %%
# (8) Scatterplot: Claims Rate vs. Total Revenue
def plot_monthly_claims_revenue_by_store(monthly_claims_revenue_by_store):
        plt.figure(figsize=(20,10))
        sns.scatterplot(
            data = monthly_claims_revenue_by_store,
            x = 'revenue',
            y = 'claims_per_thousand',
            size = 'units_sold',
            sizes = (20, 300),
            hue = 'Region'
        )
        plt.xlabel('Total Revenue')
        plt.ylabel('Claims per $1000')
        plt.title('Claims per $1000 vs Revenue by Store')
        plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.show()

monthly_claims_revenue_by_store = load_monthly_claims_revenue_by_store()
plot_monthly_claims_revenue_by_store(monthly_claims_revenue_by_store)

#%%
# Testing
if __name__ == '__main__':
    None