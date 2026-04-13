# raw_headers = ["  Order ID ", "Amount (USD)", " Status"]


# clean_headers = [
#     h.strip().lower().replace(" ", "_").replace("(", "").replace(")", "")
#     for h in raw_headers
# ]

# print(clean_headers)

files = [
    "data/raw/orders_2026.csv",
    "data/raw/customers_2026.csv",
    "data/raw/notes.txt",
    "data/raw/products_2026.csv",
    "data/raw/readme.md",
]

clean_files = [f for f in files if f.endswith(".csv")]

print(clean_files)