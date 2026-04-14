import pandas as pd
import numpy as np


df = pd.read_csv("orders.csv", parse_dates=["order_date"])


total_by_region = df.groupby()

print(df)