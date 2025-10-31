
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

# List CSVs in Data/
data_path = os.path.join(os.path.dirname(__file__), '..', 'Data')
csv_files = [f for f in os.listdir(data_path) if f.endswith('.csv')]
print("Your Apple Sales CSVs:")
for f in csv_files:
    print(f"  - {f}")

# Preview first CSV (adjust name below if needed)
first_csv = csv_files[0] if csv_files else None
if first_csv:
    df = pd.read_csv(f'{data_path}/{first_csv}')
    print(f"\n {first_csv} Preview:")
    print("Shape:", df.shape)
    print("Columns:", df.columns.tolist())
    print(df.head())
else:
    print("No CSVs found—add to Data/!")

# %%
