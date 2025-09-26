import pandas as pd
import duckdb
import time
import os
import math

# Define o tamanho máximo do arquivo csv e inicializa variáveis auxiliares
max_file_size = 10_000_000
file_size = max_file_size
file_sizes = []

# Calcula o número de zeros à esquerda para o nome do arquivo
leading_zeros=int(math.log10(max_file_size))+1

# Gera uma lista de tamanhos de arquivos decrescentes (dividindo por 10 a cada iteração)
while file_size >= 100:
    file_sizes.append(int(file_size))
    file_size/=10

# Lê o arquivo CSV de entrada em um DataFrame do pandas
input_csv_path = '../data/products.csv'
df = pd.read_csv(input_csv_path)

# Para cada tamanho de arquivo desejado, gera um arquivo csv correspondente
for file_size in file_sizes[::-1]:

    # Calcula quantas vezes o DataFrame deve ser replicado para atingir o tamanho desejado
    n_replications = int(file_size / df.shape[0])
    file_name = f'products_{file_size:0{leading_zeros}d}.csv'
    output_path = "../data/csvs/"+file_name
    if os.path.exists(output_path):
        os.remove(output_path)

    # Cria o arquivo csv usando DuckDB e mede o tempo de execução
    start_time = time.time()
    con = duckdb.connect(database=':memory:')

    query = f"""
    COPY (
        SELECT t.*
        FROM '{input_csv_path}' t
        CROSS JOIN generate_series(1, {n_replications})
    ) TO '{output_path}' (FORMAT 'CSV');
    """

    con.execute(query)
    con.close()

    total_time = time.time() - start_time
    print(f'Arquivo {file_name} criado em {total_time:.2f} segundos.')
