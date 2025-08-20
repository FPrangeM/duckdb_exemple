import duckdb
import os
import time
import glob




folder_path = '../data/parquets'
files = sorted(os.listdir(folder_path))

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

for filename in files:
    table_name = f"read_parquet('{os.path.join(folder_path, filename)}')"
    query = query_template.format(table_name=table_name)
    print(filename)
    t1 = time.time()
    con_memory.execute(query)
    print(time.time() - t1)


con_memory.close()
