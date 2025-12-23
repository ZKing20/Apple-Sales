# Apple Retail Sales Analysis

## Objective
Analyze Apple sales performance and warranty outcomes across products, stores, and regions to identify patterns, risks, and opportunities related to revenue performance and product quality.

## Tools Used
- Python (pandas, duckdb, matplotlib/plotly)
- Jupyter Notebook
- SQL (DuckDB)
- Kaggle retail dataset

##  Key Questions
### **Revenue Performance**
1. **Which products generate the highest revenue, and how consistent is this performance across regions?**
2. **Which stores and countries contribute the most to overall revenue, and how do their monthly trends compare?**
3. **Which product categories show strong growth or decline over time?**

### **Warranty & Product Quality**
4. **Which stores and regions have the highest warranty claim counts or rates?**
5. **Which products experience the highest claim rates, and how does this relate to their sales volume?**
6. **Are there specific product–store combinations that show unusually high claim activity?**

### **Revenue vs. Quality**
7. **Is there a relationship between a store’s revenue and its warranty claim rate, and which stores manage to generate strong revenue while maintaining low warranty claims?**

## Results
### Summary
- **Revenue Drivers:** The US market is dominant but likely saturated or near saturation. Hardware drives the bulk of revenue, but Subscription Services (particularly Apple Music) show high volume with low total revenue share, indicating a key growth opportunity.
- **Operational Efficiency:** Store performance is bifurcated into two distinct "tiers". High-revenue stores demonstrate superior operational scalability, maintaining lower warranty claims rates even at high volumes.
- **Product Quality:** Warranty claims are low (roughly 0.10% - 0.17%) and uniform across regions, stores and products. The data strongly suggests that claim root causes are likely manufacturing-based rather than store-specific operational failures.

### Key Insights and Visualizations
- **Revenue Trends and Seasonality**
  - **Global Leaders:** The United States generates nearly double the revenue of the next closest competitor (China). However, similar performance between North America and Asia suggests the US market may be nearing saturation.
  - **The "February Dip":** Across all regions, revenue consistently drops in February following holiday spikes.
  - **Product Mix:** While hardware (Laptops, Phones) drives total revenue, Apple Music is the top individual product by transaction count. This highlights that customers are buying into the ecosystem, not just devices.

<img src="Images/Key Question 2/2.c_regions_monthly_revenue.png" width="700">
(Figure 1: Monthly Revenue trends showing the consistent post-holiday dip across all major regions)

- **Warranty and Quality Assurance**
  - **The "Noise" Factor:** Analysis of warranty claims by Store, Region, and Product reveals a consistently low claim rate (0.10% - 0.17%) with no distinct outliers.
  - **Root Cause:** The lack of regional or store-specific spikes suggests that warranty issues are likely stemming from baseline manufacturing defects rather than shipping damage, storage conditions, or local handling.

<img src="Images/Key Question 4/4.b_claims_rate_by_store.png" width="700">
(Figure 2: Claims rates are flat scross stores, indicating a stable baseline manufacturing defect rate, rather than operational failures)

- **Operational Scalbility (Revenue vs. Quality)**
  - **Two Tiers of Stores:** The data reveals two distinct clusters of stores: "Tier 1" (roughly $1.2M/mo) and "Tier 2" (roughly $2.8M/mo).
  - **Economy of Scale:** As store revenue increases, the warranty claim rate does not increase proportionally. In fact, "Tier 2" stores show lower volatility in claims per million. This proves that high-volume locations are successfully scaling operations without sacrificing service quality.

<img src="Images/Key Question 7/7.b_monthly_claims_rate_per_store_vs_total_revenue_by_region.png" width="700">
(Figure 3: Claims per $1M vs Total Revenue. Note the two distinct store clusters and the tightening variance in the high-revenue cluster)

### Recommendations
Based on the data, I propose the following strategies:
1. **Bundle Subscriptions:** Since hardware is the revenue driver, but Supscription Services are the volume driver, create aggressive bundles (e.g., "3 Months Free Apple Music with iPad Mini") to increase Lifetime Value of hardware buyers.
2.  **Stabilize Q1 Revenue:** To combat the predicted "February Dip", launch targeted marketing campaigns or "Valentine's Day" accessory specials in mid-January
3.  **Invest in Middle East Expansion:** With North Africa and Asia near saturation, and high disposable income in the Middle East, this regions represents the most logical target for physical store expansion.


## Repository Structure
├── **Data/**                      (CSV files)

├── **Notebooks/**                 (Jupyter Notebooks for EDA and visualization)

│   ├── eda_visualizations.py  (Plotting logic)

├── **Scripts/**                   (Python scripts for SQL logic and database connection)

│   ├── __init__.py            (Allows this folder to be imported by other files)

│   ├── eda_queries.py         (SQL queries separating logic from presentation)

│   ├── db_connections.py      (DuckDB connection handling)

│   ├── config.py              (Defines variables used in db_connections.py)

├── **Images/**                    (Exported charts and graphs used in this README, separated by Key Question)

├── **.vscode/**                   (Files needed to replicate this project using vscode)

│   ├── extensions.json        (packages and modules needed to replicate)

│   ├── settings.json          (various necessary setings)

├── .gitignore   

├── .gitattributes

├── **README.md**                  (Project documentation)

## Notes
### Data Integrity
I identified a product simply labeled 'MacBook' (ID: P-1) with a unique price point ($1149) and release date. While the name is rather vague, the product accounts for $73M in revenue (1.2%) of total. To preserve data integrity and financial accuracy, I decided that it made the most sense for this product to be included in the dataset despite the ambiguous naming. Note that this dataset is synthetic data gathered from Kaggle, so it isn't as simple as just looking up which product released on the given release date.

## Status
Very nearly complete; Finishing final polishing.

All of the data was gathered on Kaggle at https://www.kaggle.com/datasets/amangarg08/apple-retail-sales-dataset. It does NOT represent real Apple Sales data.

Any and all feedback is greatly appreciated
