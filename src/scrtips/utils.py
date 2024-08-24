import re
import requests
from bs4 import BeautifulSoup
import polars as pl
import matplotlib.pyplot as plt
from typing import Any
import os
import sys
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
# Set the default encoding to UTF-8
os.environ["PYTHONIOENCODING"] = "utf-8"

# If printing to the console, also set the stdout encoding
sys.stdout.reconfigure(encoding='utf-8')
def get_summary_yh(url):
    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')
        div_class = 'caas-body'  
        div_element = soup.find('div', class_=div_class)
        return div_element.text if div_element else None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
def multi_reader(url: str, method: str = 'p', class_name: str = None) -> str:
    """
    Dado un URL, obtiene el contenido de la noticia.

    Args:
        url (str): Dirección web del sitio que contiene la noticia.
        method (str): Indica si buscar mediante las etiquetas 'p' o 'div'.
        class_name (str): Nombre de la clase que contiene el texto de la noticia en el URL.
                          Usado cuando method == 'div'.

    Returns:
        output (str): Texto de la noticia.

    Examples
    print("Yahoo")
    print(get_summary('https://www.yahoo.com/news/two-florida-congressional-dems-warn-171323616.html'))
    print("The Markets Daily")
    print(get_summary("https://www.themarketsdaily.com/2024/08/22/nym-nym-price-hits-0-0825.html"))
    print("Daily Political")
    print(get_summary("https://www.dailypolitical.com/2024/08/23/paypal-nasdaqpypl-shares-down-0-7.html"))
    print("IndiaTimes")
    print(get_summary('https://economictimes.indiatimes.com/news/elections/assembly-elections/jammu-kashmir/jammu-kashmir-assembly-election-dates-when-it-will-be-held-all-you-need-to-know-ec-rajiv-kumar-article-370-delimitation/articleshow/112565368.cms'))
    print("The enterprise leader")
    print(get_summary("https://theenterpriseleader.com/2024/08/16/skyline-champion-co-nysesky-director-sells-287860-64-in-stock.html"))
    print("Daily mail")
    print(get_summary("https://www.dailymail.co.uk/femail/article-13773257/DEAR-BEL-dont-want-daughter-having-casual-sex-house.html"))
    print("wkrb13")
    print(get_summary("https://www.wkrb13.com/2024/08/23/invesco-qqq-nasdaqqqq-stock-price-up-0-2.html"))
    print("tickerreport")
    print(get_summary("https://www.tickerreport.com/banking-finance/12426805/assenagon-asset-management-s-a-invests-1-11-million-in-clear-channel-outdoor-holdings-inc-nysecco.html"))
    print("etfdaily news")
    print(get_summary("https://www.etfdailynews.com/2024/08/23/interactive-brokers-group-inc-nasdaqibkr-shares-sold-by-assenagon-asset-management-s-a/"))
    print("the Guardian")
    print(get_summary("https://www.theguardian.com/commentisfree/article/2024/aug/22/outraged-donald-trump-artificial-intelligence-deepfakes-taylor-swift"))
    """
    try:
        page = requests.get(url)
        soup = BeautifulSoup(page.text, 'html.parser')

        if method == 'p':
            output = ''.join([z.text for z in soup.find_all('p')])
            return output if output else None

        elif method == 'div':
            output = soup.find('div', class_=class_name)
            return output.text if output else None

        else:
            print(f'{method} not recognized.\nEnter either \'p\' or \'div\'')
            return None

    except Exception as e:
        print(f'An error occurred: {e}')
        return None


def get_summary(url: str) -> str:
    """
    Devuelve el texto de la noticia para diferentes sitios web.

    Args:
        url (str): Dirección web de la noticia.

    Returns:
        output (str): Texto de la noticia limpio.
    """
    parenturl = list(re.finditer(r"http(s?):\/\/(www\.)?(.+?\.).+?\/", url))[0].group()

    if "yahoo" in url:
        return multi_reader(url, method='div', class_name='caas-body')
    elif "themarketsdaily" in url:
        output = re.sub(r'(\s){2,}', ' ', re.sub('Get Aflac alerts:', '', re.sub(r'(\n){2,}', '\n', multi_reader(url, method='div', class_name='entry').split('Recommended Stories')[0])))
        return output
    elif "dailypolitical" in url:
        return re.sub(r'\n(\t){1,}', '\n', multi_reader(url, method='p')).split('Receive News & Ratings')[0]
    elif "indiatimes" in url:
        return get_summary_it(url)
    elif "enterpriseleader" in url:
        return multi_reader(url)
    elif "dailymail" in url:
        return get_summary_dm(url)
    elif "wkrb13" in url:
        return multi_reader(url)
    elif "tickerreport" in url:
        return multi_reader(url)
    elif "etfdailynews" in url:
        return multi_reader(url)
    elif "theguardian" in url:
        return multi_reader(url)
    else:
        print(f"URL {url} not recognized.")
        return None

def get_summary_it(url: str) -> str:
    """Summary extractor for Indiatimes."""
    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')
        div_class = 'summary'
        div_element = soup.find('h2', class_=div_class)
        return div_element.text if div_element else None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None


def get_summary_dm(url: str) -> str:
    """Summary extractor for Daily Mail."""
    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')
        div_class = 'mol-para-with-font'
        div_element = soup.find_all('p', class_=div_class)
        return ''.join([z.text for z in div_element]) if div_element else None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None


def read_csv(file_path: str, separator: str = '\t') -> pl.DataFrame:
    """Read a CSV file into a Polars DataFrame."""
    return pl.read_csv(file_path, separator=separator)


def filter_health_data(df: pl.DataFrame) -> pl.DataFrame:
    """Filter the DataFrame for rows containing 'HEALTH' in the 'THEMES' column."""
    return df.filter(pl.col("THEMES").str.contains("HEALTH"))


def get_source_shape_counts(df: pl.DataFrame) -> pl.DataFrame:
    """Group by 'SOURCES' and count the occurrences."""
    return (
        df.group_by("SOURCES")
        .agg(pl.count("SOURCES").alias("count"))
        .sort("count", descending=True)
    )


def save_to_csv(df: pl.DataFrame, file_path: str, separator: str = '\t') -> None:
    """Save the DataFrame to a CSV file."""
    df.write_csv(file_path, separator=separator)




def read_csv(file_path: str, separator: str = '\t') -> pl.DataFrame:
    """Read a CSV file into a Polars DataFrame.
    
    Args:
        file_path (str): The path to the CSV file.
        separator (str): The separator used in the CSV file.
    
    Returns:
        pl.DataFrame: The DataFrame containing the CSV data.
    """
    return pl.read_csv(file_path, separator=separator)

def filter_health_data(df: pl.DataFrame) -> pl.DataFrame:
    """Filter the DataFrame for rows containing 'HEALTH' in the 'THEMES' column.
    
    Args:
        df (pl.DataFrame): The original DataFrame.
    
    Returns:
        pl.DataFrame: The filtered DataFrame.
    """
    return df.filter(pl.col("THEMES").str.contains("HEALTH"))

def get_source_shape_counts(df: pl.DataFrame) -> pl.DataFrame:
    """Group by 'SOURCES' and count the occurrences.
    
    Args:
        df (pl.DataFrame): The filtered DataFrame.
    
    Returns:
        pl.DataFrame: The DataFrame with source shape counts.
    """
    return (
        df.group_by("SOURCES")
        .agg(pl.count("SOURCES").alias("count"))
        .sort("count", descending=True)
    )

def save_to_csv(df: pl.DataFrame, file_path: str, separator: str = '\t') -> None:
    """Save the DataFrame to a CSV file.
    
    Args:
        df (pl.DataFrame): The DataFrame to save.
        file_path (str): The path to the CSV file.
        separator (str): The separator to use in the CSV file.
    """
    df.write_csv(file_path, separator=separator)







analyzer = SentimentIntensityAnalyzer()

# Function to calculate sentiment scores

def process_text_df(health_df: pl.DataFrame) -> pl.Series:
    """Extracts and processes the 'SOURCEURLS' from the DataFrame."""
    text_df = health_df["SOURCEURLS"].head().map_elements(multi_reader)
    text_df = text_df.rename('TEXT')
    return text_df

def compute_vader_scores(text_df: pl.Series) -> pl.DataFrame:
    """Computes the Vader sentiment scores for the given text data."""
    def get_vader_scores(text: str) -> dict:
        if text:
            return analyzer.polarity_scores(text)
        return {'neg': None, 'neu': None, 'pos': None, 'compound': None}

    vader_scores = text_df.map_elements(get_vader_scores)
    neg_col = vader_scores.map_elements(lambda x: x['neg']).alias('NEG')
    neu_col = vader_scores.map_elements(lambda x: x['neu']).alias('NEU')
    pos_col = vader_scores.map_elements(lambda x: x['pos']).alias('POS')
    compound_col = vader_scores.map_elements(lambda x: x['compound']).alias('COMPOUND')

    return pl.DataFrame([neg_col, neu_col, pos_col, compound_col])




#Here we read the data from a csv file
df = read_csv("20240812.gkg.csv")
health_df = filter_health_data(df)
#Here we scrap the text from the articles in the csv file
text_df = process_text_df(health_df.head(10))
print(text_df)
#Here we apply Vader polarity test
vader_scores = compute_vader_scores(text_df)
print(vader_scores)
#Here we append the new columns to the DataFrame
sample_health_df = health_df.head(10).insert_column(1,text_df)
sample_health_df= pl.concat([sample_health_df,vader_scores],how="horizontal")
print(sample_health_df)
