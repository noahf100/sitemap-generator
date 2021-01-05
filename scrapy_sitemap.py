import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor


class RecipeSpider(CrawlSpider):
    name = 'recipe_spider'
    
    def __init__(self):
        self.start_urls = ['https://www.hellofresh.com']
        self.wanted_prefixes = ['https://www.hellofresh.com/recipes']
        self.allowed_domains = ['www.hellofresh.com']

        self._rules = (Rule (LinkExtractor(), callback=self.parse_item, follow=True),)

    def _MatchesPrefix(self, url):
        for prefix in self.wanted_prefixes:
            if url.startswith(prefix):
                return True
        return False

    def parse_item(self, response):
        url = response.url
        print(url)
        if self._MatchesPrefix(url):
            return {
                'url': url
            }