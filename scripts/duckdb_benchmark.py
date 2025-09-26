import duckdb
import os
import time
import glob
import pandas as pd





con_memory = duckdb.connect(database=':memory:', read_only=False)

query_template = '''
SELECT 
    brand,
    COUNT(*) AS product_count,
    AVG(price) AS avg_price,
    SUM(stock) AS total_stock,
    SUM(price * stock) AS inventory_value
FROM {table_name}
WHERE category = 'Electronics'
GROUP BY brand
HAVING SUM(stock) > 15
QUALIFY RANK() OVER (ORDER BY SUM(price * stock) DESC) <= 3
ORDER BY inventory_value DESC;
'''

# parquets

data_parquet = []

folder_path = '../data/parquets'
files = sorted(os.listdir(folder_path))

for i,filename in enumerate(files):
    table_name = f"read_parquet('{os.path.join(folder_path, filename)}')"
    query = query_template.format(table_name=table_name)
    print(filename)
    t1 = time.time()
    con_memory.execute(query)
    duration = time.time() - t1
    print(duration)
    data_parquet.append(['DuckDB',filename,duration])

# csv

data_csv = []

folder_path = '../data/csvs'
files = sorted(os.listdir(folder_path))

for i,filename in enumerate(files):
    table_name = f"read_csv('{os.path.join(folder_path, filename)}')"
    query = query_template.format(table_name=table_name)
    print(filename)
    t1 = time.time()
    con_memory.execute(query)
    duration = time.time() - t1
    print(duration)
    data_csv.append(['DuckDB',filename,duration])

con_memory.close()


columns = ['Engine','file','time']
df = pd.DataFrame(data_parquet+data_csv,columns=columns)

os.makedirs('../results', exist_ok=True)
df.to_csv('../results/duckdb.csv',index=False)