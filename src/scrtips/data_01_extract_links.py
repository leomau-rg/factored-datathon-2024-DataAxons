"""Este script es para convertir el html que tienen todas las ligas y md5 en un csv"""
import re
from pathlib import Path, PosixPath
from typing import List

import pandas as pd


def where_is_regex(pat:str, text:str) -> tuple:
    """busca el match de un patron de expresion regular en un texto 
    y regresa los índices de inicio y fin de esa cadena
    
    Args
    ==========
    pat (str): patron de expresión regular a buscar
    text (str): texto donde se va a buscar el patrón
    
    Returns
    ==========
    (start, end) (tuple): tupla de índices donde comienza y termina la cadena encontrada
                          si no se encontró ningún match regresa (-1,-1)
    """
    match = re.search(pat, text)
    if match:
        return match.start(), match.end()
    else:
        return -1, -1
    

def match_regex(pat:str, text:str) -> str:
    """busca un patrón de texto y regresa la cadena encontrada, 
    si no encuentra nada regresa cadena vacía

    Args
    ==========
    pat (str): patron de expresión regular a buscar
    text (str): texto donde se va a buscar el patrón
    
    Returns
    ==========
    result (str): cadena encontrada
    """
    result = ""
    ini, end = where_is_regex(pat, text)
    if ini != -1:
        result = text[ini:end]
    return result


def process_line(txt_line:str) -> list:
    """Procesa una linea de texto extrayendo la url del archivo zip y el md5 para validar la descarga
    
    Args
    ==========
    txt_line (str): linea de texto en crudo
    
    Returns
    ==========
    row (tuple): tupla de 2 str: (url, md5)
    """
    url_match = match_regex(r'"http:.+\.zip"', txt_line)
    md5_match = match_regex(r'[a-f0-9]{32}', txt_line)

    if url_match == '':
        raise ValueError(f'No se encontró la url en el texto: {txt_line}')
    if md5_match == '':
        raise ValueError(f'No se encontró el md5 en el texto: {txt_line}')
    row = [url_match.replace('"', ''), md5_match]
    return row


def process_raw_data_links(file_path:List[str, PosixPath]) -> None:
    filepath = Path(file_path)

    with open(filepath) as f:
        lines = [l.strip() for l in f.readlines()]

    print(f'[INFO]>> {len(lines)} lines found')

    data = {
        "url": [],
        "md5": []
    }

    for idx, txline in enumerate(lines):
        try:
            row = process_line(txline)
            data["url"].append(row[0])
            data['md5'].append(row[1])
        except Exception as e:
            print(f'[WARNING]>> en la línea {idx}: {e}')

    pd.DataFrame().from_dict(data).to_csv(str(filepath.parent / "all_gdelt_event_files.csv"), index=False)

