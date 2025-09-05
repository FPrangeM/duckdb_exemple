from google.cloud import bigquery
from google.cloud import storage
import os



service_account_path = '/home/prange/Documentos/chaves/gcp/prangedev-355c50bbb83a.json'

bigquery_client = bigquery.Client.from_service_account_json(service_account_path)
storage_client = storage.Client.from_service_account_json(service_account_path)


# Upload para o google storage


source_folder = '../data/parquets/'
bucket_name = 'teste_duckdb'
bucket = storage_client.bucket(bucket_name)
destination_folder = 'parquets'

for source_file_name in os.listdir(source_folder):
    # source_file_name = 'products_000000100.parquet'
    source_file_path = os.path.join(source_folder,source_file_name)
    destination_blob_name = os.path.join(destination_folder,source_file_name)

    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_path)







dataset_id = "seu-projeto-id.meu_novo_dataset"

# 3. Cria um objeto Dataset
dataset = bigquery.Dataset('prangedev.sandbox_2')
dataset.location = "US"


dataset = bigquery_client.create_dataset(dataset, timeout=30)



query_string = '''
SELECT 
    brand,
    COUNT(*) AS product_count,
    AVG(price) AS avg_price,
    SUM(stock) AS total_stock,
    SUM(price * stock) AS inventory_value
FROM `prangedev.sandbox.parquet1`
WHERE category = 'Electronics'
GROUP BY brand
HAVING SUM(stock) > 15
QUALIFY RANK() OVER (ORDER BY SUM(price * stock) DESC) <= 3
ORDER BY inventory_value DESC
'''

query_job = bigquery_client.query(query_string)

(query_job.ended - query_job.started).total_seconds()


results_df = query_job.to_dataframe()
