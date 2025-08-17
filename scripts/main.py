import duckdb


# Utilizar o duckdb em memória
con_memory = duckdb.connect(database=':memory:', read_only=False)

con_memory.execute("SELECT * FROM '../data/products_10M.parquet' limit 5").fetch_df()

con_memory.execute("SELECT * FROM './../data/products.csv'").fetch_df()

# Utilizar com um .db
db_file = 'my_duckdb.db'
con_file = duckdb.connect(database=db_file, read_only=False)

# con_file.execute("CREATE OR REPLACE TABLE products_csv AS SELECT * FROM './../data/products.csv'")
# con_file.execute("CREATE OR REPLACE TABLE products_parquet AS SELECT * FROM '../data/products_10M.parquet'")

aggregation_query = """
SELECT 
    brand,
    COUNT(*) AS product_count,
    AVG(price) AS avg_price,
    SUM(stock) AS total_stock,
    SUM(price * stock) AS inventory_value
FROM '../data/products_10M.parquet'
WHERE category = 'Electronics'  -- Filtra apenas eletrônicos
GROUP BY brand
HAVING SUM(stock) > 15  -- Marcas com menos de 100 itens no estoque total
QUALIFY RANK() OVER (ORDER BY SUM(price * stock) DESC) <= 3  -- Marcas com preço médio acima de 50
ORDER BY inventory_value DESC;
"""

con_file.execute(f"{aggregation_query}").fetch_df()
con_file.execute(f"CREATE OR REPLACE TABLE aggregated_products AS ({aggregation_query})")

con_memory.close()
con_file.close()
