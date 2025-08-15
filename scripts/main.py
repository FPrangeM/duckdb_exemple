import duckdb


# Utilizar o duckdb em memória
con_memory = duckdb.connect(database=':memory:', read_only=False)

con_memory.execute("SELECT * FROM './../data/products.csv'").fetchdf()
con_memory.execute("SELECT * FROM './../data/products.parquet'").fetchdf()


# Utilizar com um .db
db_file = 'my_duckdb.db'
con_file = duckdb.connect(database=db_file, read_only=False)

con_file.execute("CREATE OR REPLACE TABLE products_csv AS SELECT * FROM './../data/products.csv'")
con_file.execute("CREATE OR REPLACE TABLE products_parquet AS SELECT * FROM './../data/products.parquet'")

aggregation_query = """
    SELECT 
        product_name, 
        COUNT(product_id) as quantity, 
        AVG(price) as average_price
    FROM products_csv
    GROUP BY product_name
    ORDER BY product_name
"""

con_file.execute(f"CREATE OR REPLACE TABLE aggregated_products AS ({aggregation_query})")

con_memory.close()
con_file.close()
