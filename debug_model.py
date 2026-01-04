import pandas as pd
import sqlite3

# Connect to database
conn = sqlite3.connect('database/supply_chain_dw.db')

# Load relevant columns
query = """
SELECT 
    shipping_mode,
    days_scheduled,
    days_real,
    late_delivery_risk
FROM fact_orders
"""
df = pd.read_sql(query, conn)

# 1. Calculate ACTUAL Late Risk by Shipping Mode
print("\n--- ACTUAL DATA (Historical) ---")
print(df.groupby('shipping_mode')['late_delivery_risk'].mean().sort_values(ascending=False))

# 2. Check the "Tight Deadline" Hypothesis
print("\n--- Why is it late? (Avg Days) ---")
print(df.groupby('shipping_mode')[['days_scheduled', 'days_real']].mean())