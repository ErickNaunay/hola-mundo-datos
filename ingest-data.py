import pandas as pd

def main():
    
    URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet"

    datos_crudos = pd.read_parquet(URL)

    print(f'Cantidad de datos: {datos_crudos.shape[0]}')
    print()
    print(f'Columnas: {datos_crudos.columns}')
    print()    
    print(datos_crudos.head())


# Verificamos que el archivo se ejecuta como principal
if __name__ == '__main__':
    main()