#Quiero una función que me calcule:
# 1. recibe el archivo llmadas 123
# 2. transformacion y limpieza de los datos
# 3. Guarde el archivo nuevo y lo pegue en la carpeta processed


#Importar librerias de manipulación de datos
import numpy as np
import pandas as pd
from dateutil.parser import parse

# importar librerías del sistema
import os
from pathlib import Path #para traer las rutas

#root_dir=Path(".").resolve()#.parent #Directorio_raiz
bucket = 'gs://rcortes_bucket_llamadas123'

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
    filepath = os.path.join(bucket, "data", data_dir, filename)

    print(filepath)

    data = pd.read_csv(filepath, encoding='latin-1', sep=';') #leer el archivo en la ruta
    #data.head() # mostrar la cabecera de la tabla
    return data


def get_transform(data):
    
    data.rename(    columns = {
    'FECHA_INICIO_DESPLAZAMIENTO-MOVIL' : 'FECHA_INICIO_DESPLAZAMIENTO_MOVIL',
    'CODIGO DE LOCALIDAD'               : 'CODIGO_LOCALIDAD',
    'CLASIFICACION FINAL'               : 'CLASIFICACION_FINAL'},
    inplace=True
    )
    
    df_transformacion = data.drop_duplicates()
     #df_transform['UNIDAD'].value_counts(dropna=False) #Conteo de los valores sin eliminarme lo nulos
    df_transformacion = data['UNIDAD'].fillna('SIN_DATO').value_counts(dropna=False) # Reemplace en la columna UNIDAD los nulos por SIN_DATO
    df_transformacion = data['UNIDAD'] = data['UNIDAD'].fillna('SIN_DATO') #Sobreescribo la columna UNIDAD que tenia valores nulos por una columna nueva sin nulos
    col = 'FECHA_INICIO_DESPLAZAMIENTO_MOVIL'
    df_transformacion[col] = pd.to_datetime(data[col], errors='coerce')
    pd.to_datetime(parse(data['RECEPCION'][13052], dayfirst=False))
    #col1 = 'FECHA_INICIO_DESPLAZAMIENTO-MOVIL'
    #df_transformacion[col1] = pd.to_datetime(data[col1], errors='coerce')
    #pd.to_datetime(parse(data['RECEPCION'][13052], dayfirst=False))
    
    def convertir_formato_fecha(str_fecha):
        val_datetime = parse(str_fecha, dayfirst=False)
        return val_datetime

    df_transformacion = data.reset_index()  

    lista_fechas = list()
    n_filas = data.shape[0] 
        #n_filas = len(data['RECEPCION'])
    for i in range(0, n_filas):

        str_fecha = data['RECEPCION'][i]

        try:
            
            val_datetime = convertir_formato_fecha(str_fecha=str_fecha)
            lista_fechas.append(val_datetime)
        except Exception as e:
            #print(i, e)
            lista_fechas.append(str_fecha)
            continue

    data['RECEPCION_CORREGIDA'] = lista_fechas
    data['RECEPCION_CORREGIDA'] = pd.to_datetime(data['RECEPCION_CORREGIDA'], errors='coerce')
    data['EDAD']=data['EDAD'].replace({'SIN_DATO' : np.nan}) # reemplazar SIN_DATO por unn valor nulo de tipo numerico

    field_edad   ='5'
    f = lambda field_edad: field_edad if pd.isna(field_edad) == True else int(field_edad)

    f(field_edad)

    df_transformaciondata = data['EDAD']=data['EDAD'].apply(f)

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
    #df_transformacion = data
    return df_transformacion

def save_data(df_transformacion, filename):
    out_name = 'eda_' + filename
    root_dir = Path(".").resolve()
    out_path = os.path.join(bucket, 'data','processed', out_name)
    print(out_path)
    df_transformacion.to_csv(out_path)
    
    #GUARDAR la tabla en BigQuery
    #df_transformacion.to_gbq(destination_table="espbigdataeseit2022_eda.llamadas_123_eda")
    df_transformacion.to_gbq(destination_table='espbigdataeseit2022_eda.llamadas_123',  if_exists='append' )

if __name__== '__main__':
    main()