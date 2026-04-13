# Comprehensions: Elegant Data Transformations
import re


# Pattern 1: Cleaning Column Names

raw_headers = ["Customer ID", "First Name", "Last Name", "E-Mail Address", "Phone #"]

def slugify(name: str) -> str:
    """Convert a messy column name to clean snake_case."""
    name = name.strip().lower()
    name = re.sub(r"[^a-z0-0]+", "_", name)
    name = name.strip("_")
    return name



clean_headers = [slugify(h) for h in raw_headers]

# print(clean_headers)

# Pattern 2: Restructuring Records
# api_response = [
#     {"userId": 1, "firstName": "Alice", "lastName": "Smith", "orders": 15},
#     {"userId": 2, "firstName": "Bob", "lastName": "Jones", "orders": 3},
#     {"userId": 3, "firstName": "Carol", "lastName": "Lee", "orders": 28},
# ]


# records = [
#     {
#         "user_id": user["userId"],
#         "full_name": f"{user['firstName']} {user['lastName']}",
#         "order_count": user["orders"],
#         "is_power_user": user["orders"] > 10,
#     }
#     for user in api_response
# ]

# print(records)

# Pattern 3: Flattening Nested Data

# customers_with_orders = [
#     {"name": "Alice", "orders": [100, 200, 150]},
#     {"name": "Bob", "orders": [50]},
#     {"name": "Carol", "orders": [300, 250]},
# ]


# flat_orders = [
#     {"name": customer["name"], "amount": amount}
#     for customer in customers_with_orders
#     for amount in customer["orders"]
# ]

# print(flat_orders)



products = [
    {"name": "Laptop", "tags": ["electronics", "computers"]},
    {"name": "Desk", "tags": ["furniture", "office"]},
    {"name": "Mouse", "tags": ["electronics", "accessories"]},
]

product_tags =[
    {"products": product["name"], "tag": tag}
    for product in products
    for tag in product["tags"]
]

# print(product_tags)



# Pattern 4: Conditional Expressions in Comprehensions

amounts = [150, -20, 300, 0, -5, 200]

# Clamp negative values to 0
clean_amounts = [amt if amt >= 0 else 0 for amt in amounts]
# print(clean_amounts)  # [150, 0, 300, 0, 0, 200]


numbers = [1, 2, 3, 4, 5, 6]

# FILTER: only keep even numbers → [2, 4, 6]
evens = [n for n in numbers if n % 2 == 0]

# TRANSFORM: label all numbers → ["odd", "even", "odd", "even", "odd", "even"]
labels = ["even" if n % 2 == 0 else "odd" for n in numbers]



# Given this list of temperatures in Celsius:
temps_c = [0, 20, 35, -5, 100, 15]

# Write a comprehension to convert all to Fahrenheit: (c * 9/5) + 32
# Write a comprehension to keep only temps above freezing (> 0°C)
# Write a comprehension that labels each temp as "hot" (≥30), "warm" (≥15), or "cold" (below 15)



temps_f = [(c * 9/5) + 32 for c in temps_c]
print("Temperatures in Fahrenheit:", temps_f)
above_freezing = [c for c in temps_c if c > 0]
print("Temperatures above freezing:", above_freezing)
labels = ["hot" if c >= 30 else "warm" if c >= 15 else "cold" for c in temps_c]
print("Temperature labels:", labels)