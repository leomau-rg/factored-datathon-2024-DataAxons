from typing import Iterable
import scrapy 
import re
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from scrapy.settings import Settings 
from scrapy.linkextractors import LinkExtractor 

        
class MultiSiteSpider(scrapy.Spider):
    custom_settings = {
            "REQUEST_FINGERPRINTER_IMPLEMENTATION": "2.7",
            "DOWNLOAD_DELAY": 2
        }

         

    DEFAULT_REQUEST_HEADERS = {
        'Accept-Language': 'en-US,en;q=0.9',
        "Referer": "https://www.google.com/",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Sec-Ch-Ua": "\"Not A(Brand\";v=\"99\", \"Google Chrome\";v=\"121\", \"Chromium\";v=\"121\"",
        "Sec-Ch-Ua-Platform": "\"Windows\"",
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    }
    
    name = 'multi_site'
    allowed_domains = ['yahoo.com', 'themarketsdaily.com', 'businessmirror.com.ph', 'dailypolitical.com']
    start_urls = [
        #'https://www.yahoo.com/news/israeli-defence-minister-warns-iran-001132797.html',
        #'https://www.themarketsdaily.com/2024/08/11/forthright-wealth-management-llc-raises-holdings-in-aflac-incorporated-nyseafl.html',
        'https://businessmirror.com.ph/2024/08/12/generika-drugstore-salutes-uniformed-and-non-uniformed-heroes-with-exclusive-program/',
        #'https://www.dailypolitical.com/2024/08/11/bank-of-america-increases-eli-lilly-and-company-nyselly-price-target-to-1125-00.html'
    ]
 

    def start_requests(self):
        LEOMAU_HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
        for url in self.start_urls: 
            yield scrapy.Request(url=url,
                                headers= LEOMAU_HEADERS)

    
    def parse(self, response):
        #match = re.search(pattern, response)
        print('+---------------------------------------+')
        #print(f' Los headers originales: {response.text}')
        print('+---------------------------------------+')
         
        # Extract the title of the article
        title = response.css('h1::text').get()

        # Extract the date of the article
        date = response.css('time::attr(datetime)').get()
        
        url = response.url
        print(f'--------------------------------------------{url}-------------------------------------------')
        #content = response.css('p::text').getall()
        #content = ' '.join(content)
        ## Extract the article content
        try:

            if 'yahoo.com' in response.url: 
                #print('--------------------------------------yahoo-------------------------------------') 
                content = response.css('div.caas-body p::text').getall()
                content = ' '.join(content)
            #
            elif 'themarketsdaily.com' in response.url:
                #print('--------------------------------------themarketsdaily-------------------------------------') 
                content = response.css('div.entry p::text').getall()
                content = ' '.join(content)

            elif 'businessmirror.com' in response.url:  
                content = response.css('p::text')
                content = ' '.join(text.strip() for text in content)
            
            elif 'dailypolitical.com' in response.url:
                content = response.css('div.entry p::text').getall()
                content = ' '.join(content)
            else:  
                print('O_o')
            
        except AttributeError as e:
            print("+---------------------+")
            print(f'Se produjo un error de tipo: {type(e).__name__}')
            print(f'Mensaje del error es {str(e)}') 
            print("+---------------------+") 

        
            
        

        yield {
            'title': title,
            'date': date,
            'content': content
        }

# Cell 3: Set up and run the crawler
def run_spider():
    # Create a settings object if not using a project
    settings = Settings()
    settings.set('FEEDS', {
        'output.json': {
            'format': 'json',
            'overwrite': True
        }
    })

    process = CrawlerProcess(settings)
    process.crawl(MultiSiteSpider)
    process.start()

# Run the spider
run_spider()

   

