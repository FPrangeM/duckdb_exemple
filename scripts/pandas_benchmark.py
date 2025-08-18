import pandas as pd
import time

t1 = time.time()
# Carregar o Parquet em DataFrame apenas para inserir no SQLite
df = pd.read_parquet('../data/parquets/products_010000000.parquet')
t2 = time.time()-t1
print(f'Arquivo carregado em {t2:0.2f} segundos')

t1 = time.time()
filtered_df = df[
    (df['category'] == 'Electronics') & 
    (df['stock'] > 5)
]

grouped_df = filtered_df.groupby('brand').agg(
    product_count=('price', 'count'),
    avg_price=('price', 'mean'),
    total_stock=('stock', 'sum'),
    inventory_value=('price', lambda x: (x * filtered_df.loc[x.index, 'stock']).sum())
).reset_index()

# HAVING avg_price >= 30
grouped_df = grouped_df[grouped_df['avg_price'] >= 30]

# QUALIFY RANK() OVER (ORDER BY inventory_value DESC) <= 5
grouped_df['rank'] = grouped_df['inventory_value'].rank(method='min', ascending=False)
result_df = grouped_df[grouped_df['rank'] <= 5].sort_values('inventory_value', ascending=False)

# Remover a coluna de rank se não quiser exibir
result_df = result_df.drop(columns=['rank'])

print(result_df)
t2 = time.time()-t1
print(f'Consulta realizada em {t2:0.2f} segundos')
