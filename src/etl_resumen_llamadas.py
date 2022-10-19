#Pseudo codigo
# 1. leer archivo .csv
# 2. Extraer el resumen
# 3. Guardar el resumen en formato .csv

from dataclasses import dataclass
from fileinput import filename
from pkgutil import get_data
import pandas as pd
import os
from pathlib import Path
import logging

#root_dir = Path(".").resolve()
bucket = 'gs://rcortes_bucket_llamadas123'

def main(): #funcion principal
    # Basic configuration for logging
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s' #Hora de ejecucion, nombre de la funcion, nivel(adv, aviso etc)
    logging.captureWarnings(True)
    logging.basicConfig(
        level=logging.INFO, 
        format=log_fmt,
        filename='data/logs/etl_llamadas.log')
    
    
    filename = 'llamadas123_julio_2022.csv'
    # leer archivo
    data = get_data(filename)
    # extraer resumen
    df_resumen = get_summary(data)
    # guarde el resumen
    save_data(df_resumen, filename)
    
    logging.info('DONE!!!')

def save_data(df, filename):
    out_name = 'resumen_' + filename
    root_dir = Path(".").resolve()
    out_path = os.path.join(bucket, 'data','processed', out_name)

    #print(out_path)
    df.to_csv(out_path)
    
    #GUARDAR la tabla en BigQuery
    df.to_gbq(destination_table="espbigdataeseit2022.llamadas_123")
    



def get_summary(data):
    dict_resumen = dict()

    for col in data.columns:
        valores_unicos = data[col].unique()
        n_valores = len(valores_unicos)
 
        dict_resumen[col]= n_valores

    df_resume = pd.DataFrame.from_dict(dict_resumen, orient='index')
    df_resume.rename({0: 'Count'}, axis=1, inplace=True) #axis=1 es para qiue pandas se mueva por columnas no por filas

    return df_resume

def get_data(filename):
    logger = logging.getLogger('get_data') #Genero el logger de la funcion a ejecutar
    
    data_dir = "raw"
    #root_dir=Path(".").resolve()#.parent
    filepath = os.path.join(bucket, "data", data_dir, filename)
    
    logger.info(f'Reading file: {filepath}') #La informacion 

    data = pd.read_csv(filepath, encoding='latin-1', sep=';') #leer el archivo en la ruta
    
    logger.info(f'La tabla contiene {data.shape[0]} filas y {data.shape[1]} columnas') # la tabla contiene tantas columna tantas filas 
    return data

if __name__== '__main__':
    main()