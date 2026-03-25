import pandas as pd
import sqlalchemy
import math

def main():
    
    URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet"

    print('Iniciando la descarga de los datos')

    datos_crudos = pd.read_parquet(URL)

    print('Datos descargados del internet (NY TAXI GOV)')

    print(f'Cantidad de datos: {datos_crudos.shape[0]}')
    print(f'Columnas: {datos_crudos.columns}')  
    print(datos_crudos.head())
    print()

    tamano = 10000

    # Agregar las filas por chunks (grupos)
    # round() -> .5 -> 1
    # ceil() -> .00000000001 -> 1
    # floor() -> .9999999 -> 0
    #for i in range(1, math.ceil(datos_crudos.shape[0]/tamano)):
    
    conexion = sqlalchemy.create_engine('postgresql://root:root@hola-mundo-datos-data-warehouse-1:5432/warehouse')

    print('Inicio de guaradado en el almacen de datos')

    datos_crudos.iloc[:100, :].to_sql(
        name='viajes_taxi_amarillo',
        con=conexion,
        if_exists='replace'
    )

    print('Se guardo exitosamente en el almacen de datos')


# Verificamos que el archivo se ejecuta como principal
if __name__ == '__main__':
    main()