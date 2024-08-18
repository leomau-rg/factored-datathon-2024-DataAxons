    """Este archivo define funciones para obtener el contenido de una noticia dado su url"""
    import re
    import requests
    from bs4 import BeautifulSoup



def multi_reader(url:str, with_headers:bool=False, method:str='p', class_name:str=None) -> str:
    """Dado un url, obtiene el contenido de la noticia.

    Args
    ==========
    url (str): dirección web del sitio que contiene la noticia
    
    with_headers (bool): indica si llamar a la función requests.get()
    con alguna configuración de los headers.
    
    method (str): indica si buscar mediante las etiquetas 'p' o 'div'. 
    
    class_name (str): Name of the class which contains the text of the
    news in the url. Used when method == 'div'.

    Returns
    ==========
    output (str): Texto de la noticia.
    """

    try:
        
        if with_headers == True:
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
            page = requests.get(url,headers=headers)
        else:
            page = requests.get(url)

        if method == 'p':
            soup = BeautifulSoup(page.text,'html')
            output = ''
            for z in soup.find_all('p'):
                output += str(z.text)
            return output if output else None
        elif method == 'div':
            soup = BeautifulSoup(page.content, 'html.parser')
            output = soup.find('div', class_=class_name)
            return output.text if output else print('nimais')
        else:
            print(f'{method} not recognized.\nEnter either \'p\' or \'div\'')
            return None
            
    except Exception as e:
        print(f'An error ocurred: {e}')
        return None



def news_extractor(url:str) -> str:
    """Devuelve el texto de la noticia limpio de algunos sitios web.

    Args
    ==========
    url (str): Dirección web de la noticia.
    
    Returns
    ==========
    output (str): Texto de la noticia limpio.
    """
    parenturl = list(re.finditer(r"http(s?):\/\/(www\.)?(.+?\.).+?\/",url))[0].group()

    if parenturl == 'https://www.yahoo.com/':
        output = multi_reader(url, method='div', class_name='caas-body')
    elif parenturl == 'https://www.themarketsdaily.com/':
        output = re.sub('(\s){2,}',' ',re.sub('Get Aflac alerts:','',re.sub('(\\n){2,}','\\n',multi_reader(url,method ='div',class_name='entry').split('Recommended Stories')[0])))
    elif parenturl == 'https://businessmirror.com.ph/':
        output = multi_reader(url,with_headers=True,method='p').split('Be the First to #KnowMoreInput your search keywords and press Enter')[0]
    elif parenturl == 'https://www.dailypolitical.com/':
        output = re.sub('\\n(\\t){1,}','\n',multi_reader(url,method='p')).split('Receive News & Ratings')[0]

    return output
