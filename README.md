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
7. **Is there a relationship between a store’s revenue and its warranty claim rate?**
8. **Which stores generate strong revenue while maintaining low warranty claims, and what sets them apart?**

## Results
(Will fill in after Notebook Completion, and when insights are finalized)

## Repository Structure
├── Data/                      (CSV files)

├── Notebooks/                 (Jupyter Notebooks for EDA and visualization)

│   ├── eda_visualizations.py  (Plotting logic)

├── Scripts/                   (Python scripts for SQL logic and database connection)

│   ├── __init__.py            (Allows this folder to be imported by other files)

│   ├── eda_queries.py         (SQL queries separating logic from presentation)

│   ├── db_connections.py      (DuckDB connection handling)

│   ├── config.py              (Defines variables used in db_connections.py)

├── Images/                    (Exported charts and graphs used in this README, separated by Key Question)

├── .vscode/                   (Files needed to replicate this project using vscode)

│   ├── extensions.json        (packages and modules needed to replicate)

│   ├── settions.json          (various necessary setings)

├── .gitignore   

├── .gitattributes

├── README.md                  (Project documentation)

## Status
Work in progress - visualization and final insights in progress.

All of the data was gathered on Kaggle at https://www.kaggle.com/datasets/amangarg08/apple-retail-sales-dataset. It does NOT represent real Apple Sales data.

Any and all feedback is greatly appreciated
