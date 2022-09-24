#Quiero una función que me calcule:
# 1. recibe el archivo llmadas 123
# 2. transformacion y limpieza de los datos
# 3. Guarde el archivo nuevo y lo pegue en la carpeta processed


#Importar librerias de manipulación de datos
import numpy as np
import pandas as pd

# importar librerías del sistema
import os
from pathlib import Path #para traer las rutas

root_dir=Path(".").resolve()#.parent #Directorio_raiz

def main(): #funcion principal
    filename ="llamadas123_julio_2022.csv"
    #Leer el archivo llamadas 123
    data = get_data(filename)
    #Transformacion y Limpieza de los datos
    df_transformacion = get_transform(data)
    #TransGuarde el archivo nuevo y lo pegue en la carpeta processedformacion y Limpieza de los datos
    save_data(df_transformacion, filename)


def get_data(filename):   
    data_dir = "raw" #directorio del archivo
    filepath = os.path.join(root_dir, "data", data_dir, filename)

    print(filepath)

    data = pd.read_csv(filepath, encoding='latin-1', sep=';') #leer el archivo en la ruta
    #data.head() # mostrar la cabecera de la tabla
    return data


def get_transform(data):
    df_transformacion = data.drop_duplicates()
     #df_transform['UNIDAD'].value_counts(dropna=False) #Conteo de los valores sin eliminarme lo nulos
    df_transformacion = data['UNIDAD'].fillna('SIN_DATO').value_counts(dropna=False) # Reemplace en la columna UNIDAD los nulos por SIN_DATO
    df_transformacion = data['UNIDAD'] = data['UNIDAD'].fillna('SIN_DATO') #Sobreescribo la columna UNIDAD que tenia valores nulos por una columna nueva sin nulos
       
    return df_transformacion


def save_data(df_transformacion, filename):
    out_name = 'eda_' + filename
    root_dir = Path(".").resolve()
    out_path = os.path.join(root_dir, 'data','processed', out_name)
    print(out_path)
    df_transformacion.to_csv(out_path)

if __name__== '__main__':
    main()