import scrapy 
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from scrapy.settings import Settings 




class YahooSpider(scrapy.Spider):
    name = 'yahoo'
    allowed_domains = ['yahoo.com']
    start_urls = ['https://www.yahoo.com/news/israeli-defence-minister-warns-iran-001132797.html']

    def parse(self, response):
        # Extract the title of the article
        title = response.css('h1::text').get()

        # Extract the date of the article
        date = response.css('time::attr(datetime)').get()
        
        # Extract the article content
        content = response.css('div.caas-body p::text').getall()
        content = ' '.join(content)

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
    process.crawl(YahooSpider)
    process.start()

# Run the spider
run_spider()