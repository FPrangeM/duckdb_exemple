import pandas as pd


data = {'product_id': [1, 2, 3, 4, 1, 3],
        'product_name': ['Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Laptop', 'Keyboard'],
        'price': [1200, 25, 75, 300, 1250, 70]}
df = pd.DataFrame(data)

df.to_parquet('./../data/products.parquet', engine='pyarrow')