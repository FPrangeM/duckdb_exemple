import os 


script_dir = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(script_dir, '..', 'data')

# Agora você pode listar os arquivos na pasta 'data'
print(os.listdir(data_dir))