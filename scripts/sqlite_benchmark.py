import sqlite3
import os
import time
import glob
import pandas as pd

folder_path = '../data/parquets'
files = sorted(os.listdir(folder_path))

# Cria conexão SQLite em memória
con_memory = sqlite3.connect(':memory:')

query_template = '''
SELECT *
FROM (
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
    ORDER BY inventory_value DESC
    LIMIT 3
)
ORDER BY inventory_value DESC;
'''

for filename in files:
    file_path = os.path.join(folder_path, filename)
    table_name = os.path.splitext(filename)[0]
    # Lê o parquet e carrega no SQLite
    df = pd.read_parquet(file_path)
    df.to_sql(table_name, con_memory, if_exists='replace', index=False)
    query = query_template.format(table_name=table_name)
    print(filename)
    t1 = time.time()
    con_memory.execute(query).fetchall()
    print(time.time() - t1)

con_memory.close()