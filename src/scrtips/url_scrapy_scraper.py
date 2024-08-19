import scrapy 
import re
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from scrapy.settings import Settings 
from scrapy.linkextractors import LinkExtractor 

    #def __init__(self, start_urls=None, arg*, kwargs**):
    #    super(MultiSiteSpider, self).__init__(*args, **kwargs)
    #    if start_urls:
    #        self.start_urls = start_urls 
        
class MultiSiteSpider(scrapy.Spider):
    name = 'multi_site'
    allowed_domains = ['yahoo.com', 'themarketsdaily.com', 'businessmirror.com', 'dailypolitical.com']
    start_urls = ['https://www.yahoo.com/news/israeli-defence-minister-warns-iran-001132797.html',
              'https://www.themarketsdaily.com/2024/08/11/forthright-wealth-management-llc-raises-holdings-in-aflac-incorporated-nyseafl.html',
              #'https://businessmirror.com.ph/2024/08/12/generika-drugstore-salutes-uniformed-and-non-uniformed-heroes-with-exclusive-program/',
              'https://www.dailypolitical.com/2024/08/11/bank-of-america-increases-eli-lilly-and-company-nyselly-price-target-to-1125-00.html'
]

    def parse(self, response):
        #match = re.search(pattern, response)
         
        # Extract the title of the article
        title = response.css('h1::text').get()

        # Extract the date of the article
        date = response.css('time::attr(datetime)').get()
        
        url = response.url
        print(f'************************* {url} ***************************')
        content = response.css('p::text').getall()
        content = ' '.join(content)
        # Extract the article content
        #if 'yahoo.com' in response.url: 
        #    content = response.css('div.caas-body p::text').getall()
        #    content = ' '.join(content)
        #    print('*******************CONTENT*****************', content)
        
        #elif 'themarketsdaily.com' in response.url:
        #    content = response.css('p::text')
        
        #elif 'businessmirror.com' in response.url: 
        #    content = response.css('p::text')
        
        #elif 'dailypolitical.com' in response.url:
        #    content = response.css('p::text')
        #else:
        #    print(f'la página {url} no te la manejo')
        
        
        

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

   

