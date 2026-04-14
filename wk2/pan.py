import pandas as pd
import numpy as np


# df = pd.read_csv("orders.csv", dtype={"zip_code": str, "order_id": str}, 
#                 parse_dates=["order_date"], na_values=["", "NULL", "N/A"])
# df = pd.read_csv("orders.csv", parse_dates=["order_date"])


# Step 1: Inspect Your Data


# print(df.shape)
# print(df.info())
# print(df.head())
# print(df.tail(3))
# print(df.sample(5))
# print(df.dtypes)

# print(df.isnull().sum())

# print(df["status"].value_counts())

# print(f"Memory Usage: {df.memory_usage(deep=True).sum() / 1024**2:.1f} MB")


# Step 2: Filter Your Data


# Single condition
# large_orders = df[df["amount"] > 100]

# print(large_orders)
# print(df)

# mask = df["amount"] > 100
# print(mask.head())


# Multiple conditions

# Orders over $100 in the EMEA region
# filtered = df[(df["amount"] > 100) & (df["region"] == "EMEA")]
# print(filtered)

# active = df[(df["status"] == "completed") | (df["status"] == "shipped")]
# print(active)


# not_cancelled = df[~(df["status"] == "cancelled")]
# print(not_cancelled)


# filtered = df.query("amount > 100 and region == 'EMEA' and status != 'cancelled' " )
# print(filtered)



# target_statuses = ["completed", "shipped"]
# active = df[df["status"].isin(target_statuses)]
# print(active)


# filtered = df[df["amount"] > 100].copy()
# filtered["amount_usd"] = filtered["amount"] * 1.1

# print(filtered)


# Using any DataFrame:

# Filter to rows where amount > 50
# Filter to rows where status is either "completed" or "shipped" AND amount > 100
# Use .query() to write the same filter from step 2

# filtered = df.query("amount > 100 and status in ['completed', 'shipped']")
# print(filtered)



# Step 3: Transform Your Data

# Creating New Columns
# df["total"] = df["quantity"] * df["unit_price"]

# Extract parts of a date
# df["order_year"] = df["order_date"].dt.year
# df["order_month"] = df["order_date"].dt.month
# df["day_of_week"] = df["order_date"].dt.day_name()

# Cleaning Strings
# Remove whitespace and standardize case
# df["name"] = df["customer_name"].str.strip().str.upper()

# print(df)

# Remove whitespace and standardize case
# df["name"] = df["name"].str.strip().str.upper()

# Extract email domain
# df["email_domain"] = df["email"].str.split("@").str[1]

# Replace substrings
# df["phone"] = df["phone"].str.replace("-", "", regex=False)

# status_map = {
#     "comp": "completed",
#     "ship": "shipped",
#     "canc": "cancelled",
# }
# df["status"] = df["status"].map(status_map)



# df["status"] = df["status"].map(status_map).fillna(df["status"])



# conditions= [
#     df["amount"] > 1000,
#     df["amount"] > 100,
# ]

# choices = ["premium","standard"]

# df["tier"] = np.select(conditions, choices, default="basic")

df = pd.DataFrame({
    "name": ["  Alice ", "BOB", " carol"],
    "email": ["alice@co.com", "bob@co.com", "carol@co.com"],
    "amount": [50, 1500, 300],
    "status_code": ["comp", "ship", "comp"],
})


# Clean the name column (strip whitespace, title case)
# Extract the email domain
# Map status_code to full words using a dictionary
# Add an "amount_tier" column: "high" if > 1000, "medium" if > 200, "low" otherwise

df["name"] = df["name"].str.strip().str.title()
df["email_domain"] = df["email"].str.split("@").str[1]

status_map = {
    "comp": "completed",
    "ship": "shipped",
    "canc": "cancelled",
}
df["status"] = df["status_code"].map(status_map)

df["amount_tier"] = np.select(
    [df["amount"] > 1000, df["amount"] > 200],
    ["high", "medium"],
    default="low",
)





print(df)