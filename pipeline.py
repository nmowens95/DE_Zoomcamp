import sys
import pyarrow.parquet as pq
import pandas as pd

# print(sys.argv)

# day = sys.argv[1]

# print(f"Job finished successfully for day = {day}")


# Testing parquet actual row size
table = pq.read_table('csv_data/yellow_taxi_pickup_cleaned.parquet')
print(f'Row count: {table.num_rows}')