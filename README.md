## Azure Data Engineering

What I am learning:

- Python environment setup using `venv`
- Creating and using virtual environments for projects
- Basic data inspection with pandas: `shape`, `info`, `head`, `dtypes`, `isnull()`
- Filtering DataFrames with conditions and `query()`
- Transforming data: cleaning strings, extracting email domains, mapping codes, and adding tier columns
- Using `numpy.select()` for conditional categories
- Keeping projects clean with `.gitignore`

## Week 2 Summary

In Week 2 I worked through pandas and data engineering workflows across several notebooks:

- `wk2/pan.py`: data inspection, filtering, string cleaning, status mapping, and logic for tier classification
- `wk2/pan3.py`: grouping and aggregation, `dropna=False`, multi-key grouping, region averages, joins, and customer order analysis
- `wk2/pan4.py`: memory optimization techniques using `category` dtype, downcasting numeric types, and a reusable `optimize_dataframe()` function
- `wk2/pan5.py`: API ingestion pipeline with `requests`, retry logic, bronze Parquet writes, and a Silver layer with enriched posts and comment counts

Key skills covered:

- pandas data cleaning and transformation
- groupby and aggregation patterns
- join types and finding unmatched rows
- window functions: `cumsum()`, `shift()`, and growth calculations
- memory reduction with categorical data and downcasting
- building a simple ETL pipeline from API ingestion to bronze/silver storage
