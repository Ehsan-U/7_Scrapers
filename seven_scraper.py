import json
import logging
import scrapy
from scrapy.spiders import Spider
from scrapy.crawler import CrawlerProcess
from pprint import pprint

class Seven_Scraper(Spider):
    name = 'seven_spiders'
    # iadfrance.fr
    iadfrance_info = {
        'page':1,
        'group_ids':[]
    }
    # safti.fr
    safti_info = {
        'page': 1,
        'safti_headers': {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer undefined',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
            'Accept': '*/*',
            'host': 'api.safti.fr',
        },
        'next':True,
        'uuids':set(),
        'limit':4000
    }


    def start_requests(self):
        urls = {
            "https://www.iadfrance.fr/trouver-un-conseiller/hauts-de-france":self.parse_iadfrance,
            "https://api.safti.fr/public_site/agent/search":self.parse_safti,
        }

        for url, parse_method in urls.items():
            if "safti.fr" in url:
                payload = {"page": str(self.safti_info['page']), "limit": '9'}
                yield scrapy.FormRequest(url, callback=parse_method, headers=self.safti_info['safti_headers'], formdata=payload)
            else:
                yield scrapy.Request(url, callback= parse_method)

    def parse_iadfrance(self, response):
        for agent in response.xpath("//div[contains(@class,'agent_card') and contains(@class,'onResult')]"):
            name = agent.xpath(".//a[contains(@class,'agent_name')]/text()").get()
            phone = agent.xpath(".//button[@id='adphone']/@data-phone").get()
            address = agent.xpath(".//span[contains(@class,'agent_card_location')]/text()").get()
            # email not present on website (they are using forms)
            email = ''

            item = {
                "name": self.clean(name),
                "phone": self.clean(phone),
                "address": self.clean(address),
                "email": self.clean(email)
            }
            pprint(item)
        if response.xpath("//div[@class='show--more-text']"):
            next_url = self.iadfrance_nextPage(response)
            yield scrapy.Request(url=next_url, callback=self.parse_iadfrance)
    def iadfrance_nextPage(self, response):
        self.iadfrance_info['page'] += 1
        page = self.iadfrance_info['page']
        self.iadfrance_info['group_ids'] += response.xpath(
            "//div[contains(@class,'agent_card') and contains(@class,'onResult')]//a[contains(@data-gtm, 'email')]/@id").getall()
        ignore_ids = "-".join(self.iadfrance_info['group_ids'])
        next_url = f"https://www.iadfrance.fr/trouver-un-conseiller/hauts-de-france?ignore_ids={ignore_ids}&page={page}"
        return next_url


    def parse_safti(self, response):
        body = response.body
        data = json.loads(body)
        if data.get("agents"):
            for agent in data.get("agents"):
                uuid = agent['uuid']
                if not uuid in self.safti_info['uuids']:
                    self.safti_info['uuids'].add(uuid)
                    name = agent['firstName'] + " " + agent['lastName']
                    phone = agent['phoneNumber']
                    address = agent['city'] + " " + f"({agent['postCode']})"
                    email = ''
                    item = {
                           "name": name,
                           "phone": phone,
                           "address": address,
                           "email": email
                       }
                    yield item
        if len(self.safti_info['uuids']) <= self.safti_info['limit']:
            payload = self.safti_nextPage()
            yield scrapy.FormRequest(url=response.url, callback=self.parse_safti, formdata=payload, headers=self.safti_info['safti_headers'])
    def safti_nextPage(self):
        self.safti_info['page'] += 1
        page = self.safti_info['page']
        payload = {"page": str(page), "limit": '9'}
        return payload


    @staticmethod
    def clean(data):
        if type(data) == list:
            return " ".join(data).strip()
        elif data:
            return data.strip()
        else:
            return ''



crawler = CrawlerProcess(settings={
    "REQUEST_FINGERPRINTER_IMPLEMENTATION": '2.7',
    "ROBOTSTXT_OBEY": False,
    "LOG_LEVEL":logging.INFO,
    "USER_AGENT": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36',
    "DOWNLOADER_MIDDLEWARES" : {
        'scrapy_selenium.SeleniumMiddleware': 800
    },
    "HTTPCACHE_ENABLED": True
})
crawler.crawl(Seven_Scraper)
crawler.start()

# "https://bskimmobilier.com/commercial-immobilier":self.parse_bskimmobilier