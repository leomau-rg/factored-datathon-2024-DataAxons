import re


def remove_special_characters(text):
    text = re.sub('[^(\w+) ,\.’—]+','', text) # quita los caracteres especiales
    text = re.sub('[ \n]{2,}', ' ',text) # quita los espacios en blanco repetidos
    return text

