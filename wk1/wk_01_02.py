# raw_headers = ["  Order ID ", "Amount (USD)", " Status"]


# clean_headers = [
#     h.strip().lower().replace(" ", "_").replace("(", "").replace(")", "")
#     for h in raw_headers
# ]

# print(clean_headers)

# files = [
#     "data/raw/orders_2026.csv",
#     "data/raw/customers_2026.csv",
#     "data/raw/notes.txt",
#     "data/raw/products_2026.csv",
#     "data/raw/readme.md",
# ]

# clean_files = [f for f in files if f.endswith(".csv")]

# print(clean_files)


# yesterday_ids = [101, 102, 103, 104, 105]
# today_ids = [103, 104, 105, 106, 107]

# Use set comprehensions (or just set()) to find:

# both_days = set(yesterday_ids) & set(today_ids)

# print("IDs that appeared both days:", both_days)


# # New IDs that only appeared today
# new_today = set(today_ids) - set(yesterday_ids)
# print("New IDs that only appeared today:", new_today)

# # IDs that were in yesterday but not today
# old_yesterday = set(yesterday_ids) - set(today_ids)
# print("IDs that were in yesterday but not today:", old_yesterday)


# Refactor this overly complex comprehension into clear steps:


output = [{"name": r["name"].upper(), "score": r["score"] * 1.1} for r in records if r["score"] > 50 and r["active"]]


# Split it into two or three separate comprehensions.

# # Refactored version:
# output = []
# for r in records:
#     if r["score"] > 50 and r["active"]:
#         name = r["name"].upper()
#         score = r["score"] * 1.1
#         output.append({"name": name, "score": score})
        

# Split it into two or three separate comprehensions.
# Step 1: Filter records based on score and active status      
filtered_records = [r for r in records if r["score"] > 50 and r["active"]]

# Step 2: Transform the filtered records into the desired output format
output = [{"name": r["name"].upper(), "score": r["score"] * 1.1} for r in filtered_records]

