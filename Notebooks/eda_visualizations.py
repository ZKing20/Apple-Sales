
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
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# Definitions used frequently
Regions = ['North America', 'South America', 'Europe', 'Asia', 'Middle East', 'Oceania']

consistent_category_palette = {             # Color palette matched to tab10 palette"
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

consistent_region_palette = {
    'North America': '#1f77b4',           # Blue
    'South America': '#2ca02c',           # Green
    'Europe': '#ff7f0e',                  # Orange
    'Asia': '#d62728',                    # Red
    'Middle East': '#9467bd',             # Purple
    'Oceania': '#bcbd22',                 # Olive
}

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
limit = 20
sort_order = 'DESC'

def plot_top_products_revenue(top_products_revenue):
    plt.figure(figsize=(10,6))
    sns.barplot(
        data = top_products_revenue,
        x = 'Product_Name',
        y = 'Total_Revenue_Millions',
        hue = 'category_name',
        palette = consistent_category_palette
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
        palette = consistent_category_palette
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
        palette = consistent_category_palette
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
# (2.b) Line Graph: Top N stores by Monthly Revenue
limit = 5
sort_order = 'DESC'

def plot_top_store_monthly_revenue(top_store_data):
    plt.figure(figsize=(20,10))
    sns.lineplot(
        data = top_store_data,
        x = 'Year_Month',
        y = 'Monthly_Revenue',
        hue = 'Region',
        palette = consistent_region_palette,
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
are not very cleanly defined; a lot of static, very noisy lines indicates
fairly consistent revenue but with unpredictable minor differences on a 
month to month basis aside from two noticeable trends. Revenue seems to
slightly spike in every store around the holiday season (November - December).
On the other hand, revenue seems to consistently fall in the Month of February
for most if not all stores, most if not all years. Keep reaping the benefits
of selling a lot during the holiday season, but maybe try a few targeted add
campaigns for Valentine's Day to offset predictably slow months.

"""

#%%
# (2.c) Line Graph: Regions by Monthly Revenue
sort_order = 'DESC'
def plot_regions_monthly_revenue(regions_data):
    plt.figure(figsize=(20,10))
    sns.lineplot(
        data = regions_data,
        x = 'Year_Month',
        y = 'Monthly_Revenue',
        hue = 'Region',
        palette = consistent_region_palette,
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

regions_data = load_regions_monthly_revenue(sort_order)
plot_regions_monthly_revenue(regions_data)

"""

DESCRIPTION:
This code generates a visualization that shows the Regions ranked by 
monthly revenue, helping give more a more well rounded answer to
Key Question (2). By showing the regions themselves, this helps give
more context about the overall trends of our top stores.

INSIGHT:
Contradicting our initial assumptions from (2.a), we see that 
North America and Asia are actually quite similar performers in terms of
revenue, and are clearly at or near market saturation. It would therefore
make more sense to focus strategies on expanding the market specifically
in the Middle East, sa it is an area with higher income countries that 
have well performing stores, but an overall low market share for Apple's
overall revenue. 

"""
#%%
# (3.a) Horizontal Bar Chart: Revenue by Category
sort_order = 'ASC'
def plot_category_revenue_by_year(category_revenue):
    # Drop the Total_Revenue column so it's not included in the bar
    category_revenue = category_revenue.drop(columns = ["Total_Revenue_Millions"])
    category_revenue = category_revenue.rename(columns = {
        'category_name': 'Category Name',
        'revenue_2020': '2020',
        'revenue_2021': '2021',
        'revenue_2022': '2022',
        'revenue_2023': '2023',
        'revenue_2024': '2024',
        'Total_Revenue_Millions': 'Total Revenue (Millions)'
    })

    # Set category names as index so they're used for y-axis
    category_revenue = category_revenue.set_index("Category Name")
    
    # Plot
    category_revenue.plot(
    kind='barh',
    stacked=True,
    figsize=(20,10)
    )

    plt.title('Revenue of Products by Category', fontsize=20, pad=20)
    plt.xlabel('Total Revenue (in Millions)', fontsize=18)
    plt.ylabel('Category', fontsize=18)
    plt.grid(axis='x')
    plt.legend(title='Year', bbox_to_anchor=(1.05, 1, ), loc='upper left', fontsize = 'large')
    plt.tight_layout()
    plt.show()

category_revenue = load_category_revenue_by_year(sort_order)
plot_category_revenue_by_year(category_revenue)

"""

DESCRIPTION:
This code generates a visualization that shows Revenue by Category type.
This aims to help provide groundwork to get a well rounded answer to
Key Question (3), by giving a baseline of what global revenue looks like
when broken up by Category.

INSIGHT:
This shows that the actual hardware makes up the vast bulk of the revenue 
for the company. Coupled with findings from Key Question (1), we get more reason
to focus efforts to expanding more efforts to boost revenue from subscription services.
Key Question (1) revealed that globally, subscription services are very popular, but
they don't make up very much of the overall revenue percentage. This indicates that they
are undersaturated in the market at the moment, despite their popularity.

"""

#%%
# (3.b) Line Graph: Category by Monthly Revenue
sort_order = 'DESC'

def plot_category_monthly_revenue(category_data):
    plt.figure(figsize=(20,10))
    sns.lineplot(
        data = category_data,
        x = 'Year_Month',
        y = 'Monthly_Revenue_Millions',
        hue = 'category_name',
        palette = consistent_category_palette,
        marker = 'o',
        errorbar = None
    )
    plt.title(f'Monthly Revenue Trend by Category')
    plt.xlabel('Year-Month')
    plt.ylabel('Monthly Revenue (in Millions)')
    plt.xticks(rotation=45, ha='right')
    plt.legend(title='Category', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.grid(True)
    plt.show()

category_data = load_category_monthly_revenue(sort_order)
plot_category_monthly_revenue(category_data)

"""

DESCRIPTION:
This code generates a visualization to show Monthly Revenue by Category type,
helping to directly answer Key Question (3), by giving an idea of trends for product
categories over time.

INSIGHT:
This shows even more evidence that February consistently has dips in generated
revenue. Additionally, it shows that there are only 3 categories pulling in
less than $7.5M per month: Subscription Services, Streaming Devices, and
Smart Speakers. Smart Speakers and Streaming Devices are much more in the
category of luxury items, especially when compared to the other underperforming
category--Subscription Services. Therefore, it seems more likely that Subscription
Services have the highest opportunity for growth out of the 3 low performers,
especially when combined with earlier findings. This gives more credence to 
the idea of bundling subscription services with other products, in order to get
more customers into the entire Apple ecosystem.

"""

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
# (4.a) Bar Chart: Claims Rate by Country 
limit = 19 # Only have 19 countries
sort_order = 'DESC'

def plot_claims_rate_by_country(claims_rate_country):
    plt.figure(figsize=(20,10))
    sns.barplot(
        data = claims_rate_country,
        x = 'Country',
        y = 'Claims_Rate',
        hue = 'Region',
        palette = consistent_region_palette
    )
    plt.title('Claims Rate by Country')
    plt.xlabel('Country')
    plt.ylabel('Claims Rate (%)')
    plt.xticks(rotation=45, ha='right')
    plt.legend(title='Region', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.show()    

claims_rate_country = load_claims_rate_country(limit, sort_order)
plot_claims_rate_by_country(claims_rate_country)

"""

DESCRIPTION:
This code generates a visualization that directly helps to answer
Key Question (4) by showing a graph of the claims rate for every country
we have data for, color coded by region to help see if regional trends
are present.

INSIGHT:
What we see from the graph is that all of the countries have a claims
rate around 0.12% - 0.14% (fairly negligible), and that none of the
regions seem to be clumped together in an unusual way, just normal noise.
This seems to show that no country in the data has particularly good
or poor warranty claims.

"""

#%%
# (4.b) Claims Rate by Store
limit = 100
sort_order = 'DESC'

def plot_claims_rate_by_store(claims_rate_store):
    plt.figure(figsize=(20,10))
    ax = sns.barplot(
        data = claims_rate_store,
        x = 'Store_Name',
        y = 'Claims_Rate',
        hue = 'Region',
        palette = consistent_region_palette
    )
    ax.set_xlabel('')
    ax.set_xticklabels([])
    plt.title('Claims Rate by Store')
    plt.xlabel('Store')
    plt.ylabel('Claims Rate (%)')
    plt.xticks(rotation=45, ha='right')
    plt.legend(title='Region', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.show()    

claims_rate_store = load_claims_rate_store(limit, sort_order)
plot_claims_rate_by_store(claims_rate_store)

"""

DESCRIPTION:
This code generates a visualizations that directly helps to answer 
Key Question (4) by showing the exact claims rate for the stores. 

INSIGHT:
Even when going for a fairly large scope (n = 100) of stores, we see 
that the claims rate is around 0.11% - 0.17% (also fairly negligible).
With this claims rate being similar to the claims rate per country,
we have pretty good reason to suspect that most of the reason for the
claims Apple does have are due to issues at the manufacturing level,
not due to any particular practices by any store or country.

"""

# %%
# (5.a) Bar Chart: Claims Rate by Product (Small Scope)
limit = 20
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
        palette = consistent_category_palette
    )
    plt.title(f'Top {limit} Products by Claims Rate')
    plt.xlabel('Product Name')
    plt.ylabel('Claims Rate (%)')
    plt.xticks(rotation = 45, ha = 'right')
    plt.legend(title='Category', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.show()

claims_rate_product = load_claims_rate_product(limit, sort_order)
plot_claims_rate_by_product(claims_rate_product)

"""

DESCRIPTION:
This code generates a visualization that directly answers part of
Key Question 5, by showing the top 20 products by claims rates.

INSIGHT:
The direct answer to the question is that the Mac Pro (Rack) hsa the
highest claims rate, followed by the MacBook (read note in README about
data integrity for more information about the vague naming), and
iPhone 13 Pro. The overall bigger takeaway however, is that the products
are, just like our stores and regions, fairly noisy.

"""

# %%
# (5.b) Bar Chart: Claims Rate by Product (Wide Scope)
limit = 100
sort_order = 'DESC'

def plot_claims_rate_by_product(claims_rate_product):
    plt.figure(figsize=(20,10))
    ax = sns.barplot(
        data = claims_rate_product,
        x = 'Product_Name',
        y = 'Claims_Rate_Decimal',
        hue = 'category_name',
        errorbar = None,
        dodge = False,
        palette = consistent_category_palette
    )
    ax.set_xlabel('')
    ax.set_xticklabels([])
    plt.title(f'Top {limit} Products by Claims Rate')
    plt.xlabel('Product Name')
    plt.ylabel('Claims Rate (%)')
    plt.xticks(rotation = 45, ha = 'right')
    plt.legend(title='Category', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.show()

claims_rate_product = load_claims_rate_product(limit, sort_order)
plot_claims_rate_by_product(claims_rate_product)

"""

DESCRIPTION:
This code generates a visualization that helps to flesh out the previous
answer to Key Question 5, by providing a larger scope to show a more full
range of the claims rates the products have.

INSIGHT:
To be specific, most products fall in the 0.10% - 0.16% range, which
seems to indicate that the small amount of claims we do see are due to
manufacturing issues that occur across products.

"""

# %%
# (5.c) Scatterplot: Claims Rate vs. Total Units Sold
def plot_claims_rate_vs_units(claims_rate_vs_units):
        claims_rate_vs_units = claims_rate_vs_units.rename(columns = {
            'Product_Name': 'Product Name',
            'category_name': 'Category',
            'Total_Sales': 'Total Sales',
            'Claims_Rate_Decimal': 'Claims Rate (%)'
        })
        plt.figure(figsize=(20,10))
        sns.scatterplot(
            data = claims_rate_vs_units,
            x = 'Total Sales',
            y = 'Claims Rate (%)',
            size = 'Total Sales',
            sizes = (20, 300),
            hue = 'Category',
            palette = consistent_category_palette
        )
        plt.xlabel('Total Units Sold')
        plt.ylabel('Claims Rate (%)')
        plt.title('Claims Rate vs Units Sold')
        plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', ncol=2)
        plt.show()

claims_rate_vs_units = load_claims_rate_vs_units()
plot_claims_rate_vs_units(claims_rate_vs_units)

"""

DESCRIPTION:
This code generates a visualization that directly answers part of
Key Question 5, by showing a scatterplot relating claims rate to
sales volume.

INSIGHT:
This also goes to show that there is a great amount of noise, with no
clear patterns arising within any product categories, or even any
particular product. Again, we see that the overall claims rate is between
0.9% - 0.17%, which is similar to the range we have been seeing across
other categories. This again gives more evidence to the theory that
warranty issues are arising at the manufacturer level at a rate of around
0.1% - 0.2%.

"""


#%%
# (6.a) Heatmap: Claims Rate by Product and Store
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
    plt.show()
claims_product_store = load_claims_rate_products_store(sort_order = 'DESC')
plot_top_10_claims_heatmap(claims_product_store)

"""

DESCRIPTION:
This code generates a visualization that directly answers Key Question 6.
It shows a heat map that gives the top 10 stores and products by warranty claims.

INSIGHT:
This visualization shows exactly what we suspected based on the insights gained from
other visualizations. The claims rates seems to be sporadic, with no discernible patterns
being immediately obvious. This again help support the hypothesis that the root cause for
warranty issues lies at the manufacturing level.

"""

#%%
#(6.b) Outlier Table
limit = 10
min_units = 50


def plot_outlier_table(outlier_table):
    outlier_table = outlier_table.rename(columns = {
            'Store_Name': 'Store Name',
            'Product_Name': 'Product Name',
            'Units_Sold': 'Total Units Sold',
            'Claims_Count': 'Total Claims',
            'Claims_Rate_Decimal': 'Claims Rate (%)'
        })
    fig, ax = plt.subplots(figsize=(20,3))
    ax.axis('tight')
    ax.axis('off')

    table = ax.table(
        cellText=outlier_table.values,
        colLabels=outlier_table.columns,
        cellLoc='center',
        loc='center'
    )

    table.auto_set_font_size(False)
    table.set_fontsize(12)
    table.scale(1.2, 1.2)

    for (row, _), cell in table.get_celld().items():
        if row == 0:
            cell.set_text_props(weight='bold', color='white', verticalalignment='center')
            cell.set_facecolor('#40466e')
        else:
            cell.set_facecolor('#f5f5f5')
    plt.title(f"Outliers: Top {limit} Stores by Claims Rate (Minimum {min_units} Sales)", weight='bold', pad=5, fontsize=32)
    fig.tight_layout()
    plt.show()

outlier_table = load_outliers(min_units, limit)
plot_outlier_table(outlier_table)

"""

DESCRIPTION:
This code generates a visualization that directly answers Key Question 6.
It shows a simple chart that gives the top 10 stores and products based
on individual product/store warranty claims to look for outliers that may
be hiding in the averages of the top stores from the noisy heatmap.

INSIGHT:
What we see is that even the highest claims rate product in the highest
claims rate store has less ghan a 1% claims rate (0.81%). In raw numbers,
there were 7 claims for the Mac Pro (Tower), with 865 units sold. Even
the statistical outliers do not seem to have such significant claims rate
to warrant any belief besides regularly expected noise in data.

"""

#%%
# Revenue vs Quality

"""

Key Question (7):
Is there a relationship between a store's revenue and its 
warranty claim rate, and which stores manage to generate strong
revenue while maintaining low warranty claims?

"""

# %%
# (7.a) Scatterplot: Claims Rate per Million by Store
limit = 100
sort_order = 'DESC'

def plot_claims_rate_vs_revenue(claims_rate_store_revenue):
    claims_rate_store_revenue = claims_rate_store_revenue.rename(columns = {
            'Total_Revenue': 'Total Revenue'
        })
    plt.figure(figsize=(20,10))
    sns.scatterplot(
        data = claims_rate_store_revenue,
        x = 'Total Revenue',
        y = 'claims_per_million',
        size = 'Total Revenue',
        sizes = (20,300),
        hue = 'Region',
        palette = consistent_region_palette
    )
    plt.title('Claims per $1,000,000 by Store')
    plt.xlabel('Total Revenue (in Millions)')
    plt.ylabel('Claims Per Million')
    plt.xticks(rotation=45, ha='right')
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', ncol=2)
    plt.grid(True)
    plt.show()

claims_rate_store_revenue = load_claims_vs_revenue_by_store(limit, sort_order)
plot_claims_rate_vs_revenue(claims_rate_store_revenue)

"""

DESCRIPTION:
This code generates a visualization that directly answers Key Question 7
by showing a scatterplot relating claims rate to revenue by store sales
volume.

INSIGHT:
This data shows that there are clearly two tiers of stores. Tier 1 Stores,
earning around $80M - $85M per year, and Tier 2 stores, earning more than 
$160M per year. There is also no trend line to be drawn between higher
earning stores having higher claims rates, or vice versa. This suggests
that store operations scale efficiently, with high-volume stores maintaining
the same low claims rate as smaller stores.

"""

# %%
# (7.b) Scatterplot: Monthly Claims Rate vs. Total Revenue by Region
def plot_monthly_claims_revenue_by_store(monthly_claims_revenue_by_store):
        # Data 
        plt.figure(figsize=(20,10))
        monthly_claims_revenue_by_store = monthly_claims_revenue_by_store.rename(columns = {
            'Total_Sales': 'Total Sales'
        })
        sns.scatterplot(
            data = monthly_claims_revenue_by_store,
            x = 'revenue_millions',
            y = 'claims_per_million',
            size = 'Total Sales',
            sizes = (20, 300),
            hue = 'Region',
            palette = consistent_region_palette
        )
        # Y = N/x
        line_x = np.linspace(1, 3.5, 100)
        for n in range (0, 10):
            line_y = n / line_x
            plt.plot(line_x, line_y, 'r--')
            n +=1
        plt.xlabel('Total Revenue (in Millions)')
        plt.ylabel('Claims per $1M')
        plt.title('Claims per $1M vs Revenue by Store')
        plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', ncol=2)
        plt.show()

monthly_claims_revenue_by_store = load_monthly_claims_revenue_by_store()
plot_monthly_claims_revenue_by_store(monthly_claims_revenue_by_store)

"""

DESCRIPTION:
This code generates a visualization that directly answers Key Question 7
by showing more detail about the stores caims rate by showing one dot for
each store's claims rate for each month, color coded by Region

INSIGHT:
We clearly see from this visualization more evidence to support the idea
of two tiers of stores. We can see a large gap between stores earning less
than roughly $1.7M per month, and stores earning between $2.5M-$3.5M per
month. At first glance, this data may seem to have some kind of structure
to it that may indicate some type of structure that would reflect an anomaly,
namely the lines having noticeable 'stripes'. However, this effect can simply
be explained by the underlyingm mathematics behind the graph. The Y-axis
shows claims per million, while the x-axis is showing revnue in millions.
So, since Y = Claims/Revenue, as we see an increase in X = Revenue, Y
decreases along the function Y = N / x, where N is the number of claims each
store has that particular month. This is verified by the red lines that are
overlayed on the graph as well.

"""

#%%
# (7.c) Scatterplot: Monthly Claims Rate vs. Total Revenue by Year
def plot_monthly_claims_revenue_by_store(monthly_claims_revenue_by_store):
        plt.figure(figsize=(20,10))
        monthly_claims_revenue_by_store = monthly_claims_revenue_by_store.rename(columns = {
            'Total_Sales': 'Total Sales'
        })
        sns.scatterplot(
            data = monthly_claims_revenue_by_store,
            x = 'revenue_millions',
            y = 'claims_per_million',
            size = 'Total Sales',
            sizes = (20, 300),
            hue = 'Year'
        )
        plt.xlabel('Total Revenue (in Millions)')
        plt.ylabel('Claims per $1M')
        plt.title('Claims per $1M vs Revenue by Store')
        plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', ncol=2)
        plt.show()

monthly_claims_revenue_by_store = load_monthly_claims_revenue_by_store()
plot_monthly_claims_revenue_by_store(monthly_claims_revenue_by_store)

"""

DESCRIPTION:
This code generates a visualization that directly answers Key Question 7
by showing more detail about the stores caims rate by showing one dot for
each store's claims rate for each month, color coded by Year.

INSIGHT:
This visualization shows a very similar picture to the last one, but also
reveals that there is a large cluster of stores with between 0-3 claims per
month in the year 2024. This indicates that things have improved in the most
recent year in terms of warranty claims. Investigate further to see if this is
due to company policy changes (i.e. changes in training, hiring of managers, etc.),
or if this was due to issues being resolved at the manufacturing level. Either way,
find the culprit and ensure best practices are being upheld.

"""

#%%
# Testing
if __name__ == '__main__':
    None