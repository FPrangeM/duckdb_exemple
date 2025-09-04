import duckdb


# Utilizar o duckdb em memória
con_memory = duckdb.connect(database=':memory:', read_only=False)

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

con_memory.execute(f"{aggregation_query}").fetch_df()
con_memory.execute(f"CREATE OR REPLACE TABLE aggregated_products AS ({aggregation_query})")

con_memory.close()
