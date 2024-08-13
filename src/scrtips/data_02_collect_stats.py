import io
import zipfile
import requests
from pathlib import PosixPath

import pandas as pd
from contextlib import closing


#def parse_file_content()


def run(datafile:PosixPath) -> None:

    urls_df = pd.read_csv(datafile)
    
    # los rows contienen los headers ['url', 'md5']
    for idx, row in urls_df.iterrows():

        url = row['url']
        md5 = row['md5']
        
        if 'MASTER' in url.upper():
            continue
        
        print(f'[INFO]>> {str(idx).rjust(4, "0")} requesting {url}...')
        response = requests.get(url, stream=True)

        with closing(response), zipfile.ZipFile(io.BytesIO(response.content)) as archive:
            zip_files = archive.infolist()
            
            # si hay más de un archivo solo se agarra el primero
            if len(zip_files) > 1:
                print(f'[WARNING]>> el achvio de la url {url} tiene más de un archivo')

            for member in zip_files:
                bytes_data = archive.read(member)
                bytes_str = bytes_data.decode('utf-8')
                print(bytes_str[:1500])
                print(type(bytes_str))
                df = pd.read_csv(io.StringIO(bytes_str), sep="\t", header=None, low_memory=False)
                # TODO: leer de forma rbusta el archivo
                df.describe()
            
        break     

