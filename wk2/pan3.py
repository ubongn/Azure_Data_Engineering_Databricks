import pandas as pd
import numpy as np


# df = pd.read_csv("orders.csv")
# df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")

# total_by_region = df.groupby("region")["amount"].sum()
# summary = df.groupby("region", dropna=False).agg(
#     order_count=("order_id", "count"),
#     total_revenue=("amount", "sum"),
#     avg_order=("amount", "mean"),
#     unique_customers=("customer_id", "nunique")
# ).reset_index()



# df["region_avg"] = df.groupby("region")["amount"].transform("mean")
# df["pic_of_region_avg"] = df["amount"] / df["region_avg"]



# print(summary)

# Group by region and calculate total amount per region
# Group by region AND product and calculate count + sum
# Use .transform("mean") to add a region_avg column, then calculate pct_of_region_avg

# class work
# df = pd.DataFrame({
#     "region": ["EMEA", "NA", "EMEA", "APAC", "NA", "EMEA"],
#     "product": ["Widget", "Widget", "Gadget", "Widget", "Gadget", "Widget"],
#     "amount": [100, 200, 150, 80, 300, 120],
# })

# 1) Total amount per region
# region_totals = df.groupby("region", dropna=False)["amount"].sum().reset_index(name="total_amount")

# 2) Group by region and product with order count and total amount
# region_product_summary = df.groupby(["region", "product"]).agg(
#     order_count=("amount", "count"),
#     total_amount=("amount", "sum"),
# ).reset_index()

# 3) Add region_avg and pct_of_region_avg to each row
# df["region_avg"] = df.groupby("region")["amount"].transform("mean")
# df["pct_of_region_avg"] = df["amount"] / df["region_avg"]

# print("Region totals:\n", region_totals, "\n")
# print("Region + product summary:\n", region_product_summary, "\n")
# print("Rows with region averages and pct_of_region_avg:\n", df)







# orders = pd.DataFrame({
#     "order_id": [1, 2, 3, 4],
#     "customer_id": [101, 102, 103, 999],
#     "amount": [150, 300, 80, 200],
# })

# customers = pd.DataFrame({
#     "customer_id": [101, 102, 103, 104],
#     "name": ["Alice", "Bob", "Carol", "Dave"],
#     "region": ["EMEA", "NA", "APAC", "EMEA"],
# })

# result = pd.merge(orders, customers, on="customer_id", how="left" )

# merged = pd.merge(orders, customers, on="customer_id", how="left", indicator=True)
# orphans = merged[merged["_merge"] == "left_only"]

# print(orphans[["order_id", "customer_id", "amount"]])


# Using the orders and customers DataFrames above:

# Do an inner join — how many rows?

# inner_join = pd.merge(orders, customers, on="customer_id", how="inner")
# print("Inner join rows:", len(inner_join))
# print(inner_join)
# print()

# Do a left join — how many rows? Which row has NaN values?

# left_join = pd.merge(orders, customers, on="customer_id", how="left", indicator=True)
# print("Left join rows:", len(left_join))
# print(left_join)
# print()
# print("Rows with missing customer info in left join:")
# print(left_join[left_join["_merge"] == "left_only"])
# print()

# Find customers who have NO orders

# customers_no_orders = pd.merge(customers, orders, on="customer_id", how="left", indicator=True)
# customers_no_orders = customers_no_orders[customers_no_orders["_merge"] == "left_only"]
# print("Customers with no orders:")
# print(customers_no_orders[["customer_id", "name", "region"]])






# pivot = df.pivot_table(
#     values="amount",
#     index="region",
#     columns=df["order_date"].dt.quarter,
#     aggfunc="sum",
#     fill_value=0,
# )


# print(pivot)



# df = df.sort_values(["customer_id", "order_date"])
# df["running_total"] = df.groupby("customer_id")["amount"].cumsum()
# df["rank_in_region"] = df.groupby("region")["amount"].rank(ascending=False)
# df["prev_amount"] = df.groupby("customer_id")["amount"].shift(1)
# df["amount_change"] = df["amount"] - df["prev_amount"]
# df["rolling_avg_3"] = df.groupby("customer_id")["amount"].transform(
#     lambda x: x.rolling(3, min_periods=1).mean()
# )

df = pd.DataFrame({
    "customer": ["A", "A", "A", "A", "B", "B", "B"],
    "month": [1, 2, 3, 4, 1, 2, 3],
    "amount": [100, 150, 200, 180, 50, 80, 120],
})

# Add a running_total column per customer
# Add a prev_amount column using .shift(1)
# Add a growth column: current amount minus previous amount

df = df.sort_values(["customer", "month"])
df["running_total"] = df.groupby("customer")["amount"].cumsum()
df["prev_amount"] = df.groupby("customer")["amount"].shift(1)
df["growth"] = df["amount"] - df["prev_amount"]

print(df)


# A Real Enrichment Pipeline


# Step 1: Filter to completed orders only
completed = orders.query("status == 'completed'").copy()

# Step 2: Join with customer table to get regions
enriched = pd.merge(completed, customers, on="customer_id", how="left")

# Step 3: Group by region and month
monthly = enriched.groupby(
    [enriched["region"], enriched["order_date"].dt.to_period("M")]
).agg(
    revenue=("amount", "sum"),
    orders=("order_id", "count"),
).reset_index()

# Step 4: Add month-over-month growth
monthly = monthly.sort_values(["region", "order_date"])
monthly["prev_revenue"] = monthly.groupby("region")["revenue"].shift(1)
monthly["growth_pct"] = ((monthly["revenue"] - monthly["prev_revenue"])
                          / monthly["prev_revenue"] * 100).round(1)