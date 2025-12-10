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

## Notes
### Data Integrity
I identified a product simply labeled 'MacBook' (ID: P-1) with a unique price point ($1149) and release date. While the name is rather vague, the product accounts for $73M in revenue (1.2%) of total. To preserve data integrity and financial accuracy, I decided that it made the most sense for this product to be included in the dataset despite the ambiguous naming. Note that this dataset is synthetic data gathered from Kaggle, so it isn't as simple as just looking up which product released on the given release date.

### Security
This project does write SQL in a way that DOES allow for SQL injections to occur. The reason I have decided to allow this is simpmly to make the SQL more readable, as security is not an issue with this data, given that it is sythetic and does not represent anything from the real world.

## Status
Work in progress - 5 of 8 Key Questions Answered

All of the data was gathered on Kaggle at https://www.kaggle.com/datasets/amangarg08/apple-retail-sales-dataset. It does NOT represent real Apple Sales data.

Any and all feedback is greatly appreciated
