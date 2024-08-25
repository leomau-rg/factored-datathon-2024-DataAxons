from pathlib import Path 
import json
import wordcloud as wc 
import os

import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from wordcloud import WordCloud, STOPWORDS
import matplotlib.pyplot as plt
stopwords = set(STOPWORDS)

nltk.download('stopwords')


#Se corre en los elementos text_df que se construyen en util.py 
'''Ejemplo: 
    df = read_csv("20240812.gkg.csv")
    health_df = filter_health_data(df)
    Here we scrap the text from the articles in the csv file
    text_df = process_text_df(health_df.head(10))

    construct_and_plot_wordcloud(text_df, 10)
'''

def remove_stopwords_from_text(text: str) ->  str: 
    """Filters a text from the non desired stopwords
    
    Args:
        text (str): The text to clean.
        stopwords (arr): Array of words to remove from text.
    
    Returns:
        str: Returns a text without any word from the stopwords array.
    """

    word_tokenz =  word_tokenize(text)
    filtered_words = [word for word in word_tokenz if word not in stopwords]
    return ' '.join(filtered_words)




def construct_and_plot_wordcloud(df_serie: pl.Series, max_words: int, stopwords: list = stopwords) -> wc:
    """ This function is used to create a cloudword given a filtered text

        Args: 
            df (pl.DataFrame): The original dataframe
            max_words: Maximum number of words diplayed in the wordcloud.
            stopwords: Array of stopwords to filter from texts.
        Returns:
            None        
    """
    #Construction
    
    combined_text = df_serie.str.join()[0]
    try:
        wordcloud = WordCloud(
            background_color = 'white',
            stopwords=stopwords, 
            max_words=max_words, 
            max_font_size=40, 
            scale=3,
            random_state=1
        ).generate(combined_text)

    except Exception as e:
         print(f"The unexpected error {e} ocurred durying construction.")

    #Plotting
    try: 
        title = 'Primer título'
        fig = plt.figure(1, figsize=(12,12))
        plt.axis('off')
        if title: 
            #fig.subtitle(title, fontsize=20)
            fig.subplots_adjust(top=2.3)

        plt.imshow(wordcloud)        

    except Exception as e:
        print(f"The unexpected error {e} ocurred during plot")