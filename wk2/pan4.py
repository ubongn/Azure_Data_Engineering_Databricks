import pandas as pd
import numpy as np

# df = pd.read_csv("sales.csv")

# 1 million rows with 4 status values
# n = 1_000_000
# df = pd.DataFrame({
#     "status": np.random.choice(["active", "inactive", "pending", "closed"], n),
#     "region": np.random.choice(["NA", "EMEA", "APAC"], n),
#     "amount": np.random.uniform(10, 1000, n),
# })

# Total memory usage
# total_mb = df.memory_usage(deep=True).sum() / 1024**2
# print(f"Total memory: {total_mb:.1f} MB")

# Peer-column breakdown
# print(df.memory_usage(deep=True).sort_values(ascending=False).head(10))

# Technique 1: Category Type for Repeated Strings

# print(df["status"].dtype)
# print(df["status"].memory_usage(deep=True) / 1024**2, "MB")

# # Convert
# df["status"] = df["status"].astype("category")


# # After
# print(df["status"].dtype)
# print(df["status"].memory_usage(deep=True) / 1024**2, "MB")


# Automatically detect and convert good candidates
# for col in df.select_dtypes(include="object").columns:
#     ratio = df[col].nunique() / len(df)
#     if ratio < 0.05:
#         df[col] = df[col].astype("category")
#         print(f"Converted {col}: {df[col].nunique()} unique values ({ratio:.3%})")



# 1 million rows with 4 status values
# n = 1_000_000
# df = pd.DataFrame({
#     "status": np.random.choice(["active", "inactive", "pending", "closed"], n),
#     "region": np.random.choice(["NA", "EMEA", "APAC"], n),
#     "amount": np.random.uniform(10, 1000, n),
# })

# # Check memory before conversion
# initial_mem = df.memory_usage(deep=True).sum()
# print(f"Memory before conversion: {initial_mem / 1024**2:.2f} MB")

# # Convert status and region to category
# df["status"] = df["status"].astype("category")
# df["region"] = df["region"].astype("category")

# Check memory after conversion
# after_mem = df.memory_usage(deep=True).sum()
# print(f"Memory after conversion: {after_mem / 1024**2:.2f} MB")
# print(f"Reduction: {(initial_mem - after_mem) / 1024**2:.2f} MB")




# Technique 2: Downcast Numbers

# Downcast integers
# for col in df.select_dtypes(include="int64").columns:
#     df[col] = pd.to_numeric(df[col], downcast="integer")
#     print(f"{col}: {df[col].dtype}")  # might become int8, int16, or int32

# # Downcast floats
# for col in df.select_dtypes(include="float64").columns:
#     df[col] = pd.to_numeric(df[col], downcast="float")
#     print(f"{col}: {df[col].dtype}") 


# Technique 3: Read Only the Columns You Need

# SLOW — loads ALL 50 columns
# df = pd.read_csv("wide_table.csv")

# # FAST — loads only 3 columns
# df = pd.read_csv("wide_table.csv", usecols=["order_id", "amount", "date"])

# # Parquet: only reads the specified columns from disk
# df = pd.read_parquet("sales.parquet", columns=["order_id", "amount"])



# # Technique 4: Read in Chunks
# # Read the CSV in chunks of 100,000 rows
# chunks = pd.read_csv("huge.csv", chunksize=100_000)

# results = []
# for chunk in chunks:
#     # Process each chunk independently
#     processed = chunk[chunk["amount"] > 0]      # filter
#     processed = processed.groupby("region")["amount"].sum()  # aggregate
#     results.append(processed)

# # Combine all chunk results
# final = pd.concat(results).groupby(level=0).sum()



# Technique 5: Use the pyarrow Backend (pandas 2.0+)
# # Default pandas backend
# df_default = pd.read_csv("data.csv")
# print(f"Default: {df_default.memory_usage(deep=True).sum() / 1024**2:.1f} MB")

# # pyarrow backend
# df_arrow = pd.read_csv("data.csv", dtype_backend="pyarrow")
# print(f"Arrow: {df_arrow.memory_usage(deep=True).sum() / 1024**2:.1f} MB")







# Take the 1-million-row DataFrame from Checkpoint 1 and apply all techniques in order:

# Step 0: Check baseline memory
# print(f"Before: {df.memory_usage(deep=True).sum() / 1024**2:.1f} MB")

# # Step 1: Category for strings
# for col in ["status", "region"]:
#     df[col] = df[col].astype("category")

# # Step 2: Downcast numbers
# for col in df.select_dtypes(include="float64").columns:
#     df[col] = pd.to_numeric(df[col], downcast="float")

# # Step 3: Check final memory
# print(f"After: {df.memory_usage(deep=True).sum() / 1024**2:.1f} MB")

# Complete Optimization Function
# Here's a reusable function that applies all techniques:

def optimize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Reduce memory usage of a DataFrame by optimizing types."""
    start_mem = df.memory_usage(deep=True).sum() / 1024**2

    # 1. Category for low-cardinality strings
    for col in df.select_dtypes(include="object").columns:
        if df[col].nunique() / len(df) < 0.05:
            df[col] = df[col].astype("category")

    # 2. Downcast integers
    for col in df.select_dtypes(include="int64").columns:
        df[col] = pd.to_numeric(df[col], downcast="integer")

    # 3. Downcast floats
    for col in df.select_dtypes(include="float64").columns:
        df[col] = pd.to_numeric(df[col], downcast="float")

    end_mem = df.memory_usage(deep=True).sum() / 1024**2
    reduction = (1 - end_mem / start_mem) * 100
    print(f"Memory: {start_mem:.1f} MB → {end_mem:.1f} MB ({reduction:.0f}% reduction)")

    return df



df = pd.read_csv("sales.csv")
df = optimize_dataframe(df)