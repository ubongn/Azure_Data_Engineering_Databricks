#  Generators: Processing Data Without Running Out of Memory


# all_rows = [parse_line(line) for line in open("huge_file.csv")]

# Your machine has 16 GB of RAM. That list comprehension loads all 80 million parsed rows into memory at once. Python runs out of memory and crashes. Your pipeline fails at 3 AM, and you get a call from your manager.

# This is not hypothetical — it's one of the most common failures in data engineering. The fix? Generators. They process data one item at a time, using almost zero memory, no matter how big the file is.


# A generator is like a list comprehension, but it doesn't build a list. Instead, it produces one item at a time, on demand.


# # LIST COMPREHENSION — builds full list, THEN processes
# amounts = [float(line.split(",")[2]) for line in open("big.csv")]
# #          ^                                                    ^
# #          square brackets = build a list of ALL items first

# # GENERATOR EXPRESSION — produces items one at a time
# amounts = (float(line.split(",")[2]) for line in open("big.csv"))
# #          ^                                                    ^
# #          parentheses = produce items lazily, one at a time





# def read_csv_rows(path: str):
#     """Read a CSV file, yielding one dict per row."""
#     with open(path) as f:
#         # Read the first line as column names
#         headers = f.readline().strip().split(",")

#         # Yield one row at a time - never loads entire file
#         for line in f:
#             values = line.strip().split(",")
#             yield dict(zip(header, values))
    


# for row in read_csv_rows("customers.csv"):
#     print(row)  


def even_numbers(n):
    # your code here
    """Yield even numbers from 0 up to n (exclusive)."""
    for i in range(n):
        if i % 2 == 0:
            yield i

# for num in even_numbers(10):
#     print(num)
# Should print: 0, 2, 4, 6, 8


# The Generator Pipeline Pattern

def extract(path: str):
    """Stage 1: Read raw lines from a CSV file."""
    with open(path) as f:
        header = f.readline().stripe().split(",")
        for line in f:
            values = line.strip().split(",")
            yield dict(zip(header, values))


def validate(records):
    """Stage 2: Filter out bad records."""
    for record in records:
        if record.get("amount") and record.get("name"):
            yield record
        # bad records are silently dropped


def transfor(records):
    """Stage 3: Clean and enrich each record."""
    for record in records:
        record["amount"] = float(record["amount"])
        record["name"] = record["name"].striip().upper()
        record["size"] = "large" if record["amount"] > 1000 else "standard"
        yield record


def categorize(records):
    """Stage 4: Categorize each records based on amount."""
    for record in records:
        amount = record["amount"]
        if amount > 5000:
            record["category"] = "premium"
        elif amount > 1000:
            record["category"] = "standard"
        else:
            record["category"] = "small"
            yield record

# Now we can chain these stages together without ever loading the entire dataset into memory:

raw = extract("sales.csv")
clean = validate(raw)
enriched = transfor(clean)
categorized = categorize(enriched)

# Only one record at a time flows through this pipeline, so it can handle files of any size without running out of memory.
for record in enriched:
    save_to_database(record)  # or print(record), etc.




# Add a fourth stage to the pipeline above called categorize that adds a "category" field:

# If amount > 5000: "premium"
# If amount > 1000: "standard"
# Otherwise: "small"
# Write the generator function and show how to chain it into the pipeline.

