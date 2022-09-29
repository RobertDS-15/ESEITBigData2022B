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
    #
    #df_trans_localidad = get_localidad(df_transformacion)
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
    df_transformacion = data   
    
    def cat(x):
        if x == 1  :
            return 'Usaquen'
        if x == 2  :
            return 'Chapinero'
        if x == 3  :
            return 'Santa Fe'
        if x == 4  :
            return 'San Cristobal'
        if x == 5  :
            return 'Usme'
        if x == 6  :
            return 'Tunjuelito'
        if x == 7  :
            return 'Bosa'
        if x == 8  :
            return 'Kenndy'
        if x == 9  :
            return 'Fontibon'
        if x == 10  :
            return 'Engativa'
        if x == 11:
            return 'Suba'
        if x == 12  :
            return 'Barrios Unidos'     
        if x == 13:
            return 'Teusaquillo'
        if x == 14  :
            return 'Los Martires'
        if x == 15  :
            return 'Antonio Narino'
        if x == 16  :
            return 'Puente Aranda'
        if x == 17:
            return 'La Calendaria'
        if x == 18:
            return 'Rafael Uribe Uribe'
        if x == 19:
            return 'Ciudad Bolivar'
        if x == 20:
            return 'Sumapaz'     
    
    df_transformacion['LOCALIDAD'] = df_transformacion['CODIGO_LOCALIDAD'].apply(lambda x: cat(x))
    return df_transformacion

def save_data(df_transformacion, filename):
    out_name = 'eda_' + filename
    root_dir = Path(".").resolve()
    out_path = os.path.join(root_dir, 'data','processed', out_name)
    print(out_path)
    df_transformacion.to_csv(out_path)

if __name__== '__main__':
    main()