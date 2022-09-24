#Quiero una función que me calcule:
# 1. La suma de todos los numero de una lista
# 2. Minimo valor de la lista
# 3. Maximo valor de la lista
# 4. Calcular la media y la desviacióñ estandar

import numpy as np
import argparse
import pandas as pd

def calcular_min_max(lista_numeros, verbose=1):
    '''
    Retorna los valores minimo y maximo de una de lista de numeros
    Args:
        lista_numero: type list
        verbose: 
    '''
    min_value = min(lista_numeros)
    max_value = max(lista_numeros)

    if verbose == 1:
        print('Valor Minimo', min_value)
        print('Valor Maximo', max_value)
    else:
        pass
    return min_value, max_value


def calcular_valores_centrales(lista_numeros, verbose=1):
    '''Calcula la media y la desviación estandar de una lista de números

    Args:
        lista_numeros (list): Lista con valores númericos
        verbose (bool, optional): Para decidir si imprimir mensajes

    Returns:
        Tuple: (media, dev_std)
    '''
    media = np.mean(lista_numeros)
    dev_std = np.std(lista_numeros)

    if verbose == 1:
        print('Media', media)
        print('Desviación Estandar', dev_std)
    else:
        pass
    return media, dev_std


def calcular_valores(lista_numeros, verbose=1):
    '''Retorna una tupla con valores suma, minimo, maximo, madia y desviacion estandar de una lista de numeros

    Args:
        lista_numeros (list): Lista con valores númericos
        verbose (bool, optional): Para decidir si imprimir mensajes

    Returns:
        _type_: _description_
    '''
    suma                = np.sum(lista_numeros) # calcular_suma(lista_numeros)
    min_val, max_val    = calcular_min_max(lista_numeros, verbose)
    media, dev_std      = calcular_valores_centrales(lista_numeros, verbose)
    return suma, min_val, max_val, media, dev_std

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--verbouse",
        type = int, 
        default = 1, 
        help = "para imprimir en pantalla informacion"
    )
    args = parser.parse_args()



    lista_valores = [5, 4, 8, 9, 21]
    calcular_valores(lista_numeros=lista_valores, verbose = args.verbose)

if __name__== '__main__':
    main()