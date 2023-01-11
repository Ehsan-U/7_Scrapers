import json
import logging
import re
import scrapy
from scrapy.spiders import Spider
from scrapy.crawler import CrawlerProcess
from pprint import pprint

class Seven_Scraper(Spider):
    name = 'seven_spiders'
    # iadfrance.fr
    iadfrance_info = {
        'url': "https://www.iadfrance.fr/trouver-un-conseiller/hauts-de-france",
        'page': 1,
        'group_ids': []
    }

    # safti.fr
    safti_info = {
        'url': "https://api.safti.fr/public_site/agent/search",
        'page': 1,
        'headers': {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer undefined',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
            'Accept': '*/*',
            'host': 'api.safti.fr',
        },
        "initial_payload": {"page": '1', "limit": '9'},
        'next': True,
        'uuids': set(),
        'limit': 4000
    }

    # bskimmobilier.com
    bskimmobilier_info = {
        'url': "https://bskimmobilier.com/commercial-immobilier",
        'page': 1
    }

    # megagence
    megagence_info = {
        'url': "https://www.megagence.com/nos-consultants",
        'page': 1
    }

    # lafourmi-immo.com
    lafourmi_info = {
        'url': "https://www.lafourmi-immo.com/agents?f[geoloc]=bourges&f[radius]=500",
        'page': 1,
        "headers" : {
          'Accept': 'application/json',
          'X-Requested-With': 'XMLHttpRequest',
          'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
          'host': 'www.lafourmi-immo.com',
          'Content-Type': 'application/json',
        }
    }

    # efficity.com
    efficity_info = {
        'url': "https://www.efficity.com/consultants-immobiliers/liste/",
    }

    # proprietes-privees.com
    proprietes_privees_info = {
        'url': "https://www.proprietes-privees.com/negociateur/get-mandatary?page=1&department=0",
        "page": 1
    }

    def start_requests(self):
        urls = {
            self.iadfrance_info['url']: self.parse_iadfrance,
            # self.safti_info['url']: self.parse_safti,
            # self.bskimmobilier_info['url']: self.parse_bskimmobilier,
            # self.megagence_info['url']: self.parse_megagence,
            # self.lafourmi_info['url']: self.parse_lafourmi,
            # self.efficity_info['url']: self.parse_efficity,
            # self.proprietes_privees_info['url']: self.parse_proprietes_privees,
        }

        for url, parse_method in urls.items():
            if "safti.fr" in url:
                yield scrapy.FormRequest(url, callback=parse_method, headers=self.safti_info['headers'], formdata=self.safti_info['initial_payload'])
            elif "lafourmi-immo.com" in url:
                yield scrapy.Request(url, callback=parse_method, headers=self.lafourmi_info['headers'])
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
            yield item
        if response.xpath("//div[@class='show--more-text']"):
            next_page = self.get_nextPage(website=response.url, response=response)
            yield scrapy.Request(url=next_page, callback=self.parse_iadfrance)


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
            payload = self.get_nextPage(website=response.url)
            yield scrapy.FormRequest(url=response.url, callback=self.parse_safti, formdata=payload, headers=self.safti_info['safti_headers'], dont_filter=True)


    def parse_bskimmobilier(self, response):
        for agent in response.xpath("//div[@class='informations']"):
            name = agent.xpath(".//div[@class='name']/text()").get()
            phone = agent.xpath(".//div[@class='phone']/a/text()").get()
            address = agent.xpath(".//div[@class='city']/text()").get()
            email = agent.xpath(".//div[@class='mail']/a/text()").get()
            item = {
                "name": self.clean(name),
                "phone": self.clean(phone),
                "address": self.clean(address),
                "email": self.clean(email)
            }
            yield item
        if response.xpath("//div[@class='informations']"):
            next_page = self.get_nextPage(website=response.url)
            yield scrapy.Request(url=next_page, callback=self.parse_bskimmobilier)


    def parse_megagence(self, response):
        for agent in response.xpath("//article[@class='counsellor-list']"):
            name = agent.xpath(".//div[@class='counsellor-list-name']/text()").getall()
            phone = agent.xpath(".//div[@class='counsellor-list-phone']/a/text()").get()
            address = agent.xpath("//article[@class='counsellor-list']//div[@class='counsellor-list-location']/text()").get()
            email = ''
            item = {
                "name": self.clean(name),
                "phone": self.clean(phone),
                "address": self.clean(address),
                "email": self.clean(email)
            }
            yield item
        if response.xpath("//article[@class='counsellor-list']"):
            next_page = self.get_nextPage(website=response.url)
            yield scrapy.Request(url=next_page, callback=self.parse_megagence)


    def parse_lafourmi(self, response):
        body = response.body
        try:
            data = json.loads(body)
        except Exception as e:
            print(e)
        else:
            if data['features']:
                for agent in data["features"]:
                    url = response.urljoin(agent["properties"].get("popup")[:-14])
                    yield scrapy.Request(url, callback=self.parse_lafourmi_helper)
                next_page = self.get_nextPage(website=response.url)
                yield scrapy.Request(url=next_page, callback=self.parse_lafourmi, headers=self.lafourmi_info['headers'])
    def parse_lafourmi_helper(self, response):
        name = response.xpath("//h2[@class='ellipsis']/span/text()").get()
        phone = response.xpath("//div[@class='panel-body']//a[contains(@href, 'tel') and @rel]/@href").get()
        if phone:
            phone = phone[4:]
        address = response.xpath("//p[@class='ellipsis small geoloc']/text()").getall()
        email = ''
        item = {
            "name": self.clean(name),
            "phone": self.clean(phone),
            "address": self.clean(address),
            "email": self.clean(email)
        }
        yield item


    def parse_efficity(self, response):
        for agent in response.xpath("//a[@data-gtm-category='EntreeConsultant']"):
            name = agent.xpath("./p[1]/text()").get()
            address = agent.xpath("./p[2]/text()").get()
            url = response.urljoin(agent.xpath("./@href").get())
            yield scrapy.Request(url, callback=self.parse_efficity_helper, cb_kwargs=dict(name=name, address=address))
    def parse_efficity_helper(self, response, name, address):
        phone = response.xpath("//a[contains(@href, 'tel:+') and not(@class)]/text()").get()
        email = response.xpath("//a[contains(@href, 'mail') and not(@class)]/text()").get()
        item = {
            "name": self.clean(name),
            "phone": self.clean(phone),
            "address": self.clean(address),
            "email": self.clean(email)
        }
        yield item


    def parse_proprietes_privees(self, response):
        body = response.body
        data = json.loads(body)
        if data['data']:
            for agent in data['data']:
                name = agent['first_name'] + ' ' + agent['last_name']
                address = agent['zone']
                alias = agent['alias']
                url = f"https://www.proprietes-privees.com/negociateur/{alias}"
                yield scrapy.Request(url, callback=self.parse_proprietes_privees_helper, cb_kwargs=dict(name=name, address=address))
            next_page = self.get_nextPage(website=response.url)
            yield scrapy.Request(url=next_page, callback=self.parse_proprietes_privees)
    def parse_proprietes_privees_helper(self, response, name, address):
        phone = response.xpath("//a[@id='phoneIcon']/@data-content").get()
        if phone:
            phone = re.search(r'(?:callto:)(.*?)(?:")', phone).group(1)
        email = ''
        item = {
            "name": self.clean(name),
            "phone": self.clean(phone),
            "address": self.clean(address),
            "email": self.clean(email)
        }
        yield item




    def get_nextPage(self, website, response=None):

        if "iadfrance.fr" in website:
            self.iadfrance_info['page'] += 1
            page = self.iadfrance_info['page']
            self.iadfrance_info['group_ids'] += response.xpath(
                "//div[contains(@class,'agent_card') and contains(@class,'onResult')]//a[contains(@data-gtm, 'email')]/@id").getall()
            ignore_ids = "-".join(self.iadfrance_info['group_ids'])
            next_page = f"https://www.iadfrance.fr/trouver-un-conseiller/hauts-de-france?ignore_ids={ignore_ids}&page={str(page)}"
            return next_page

        elif "safti.fr" in website:
            self.safti_info['page'] += 1
            page = self.safti_info['page']
            payload = {"page": str(page), "limit": '9'}
            return payload

        elif "bskimmobilier.com" in website:
            self.bskimmobilier_info['page'] += 1
            page = self.bskimmobilier_info['page']
            next_page = f'https://bskimmobilier.com/commercial-immobilier?page={str(page)}'
            return next_page

        elif "megagence.com" in website:
            self.megagence_info['page'] += 1
            page = self.megagence_info['page']
            next_page = f'https://www.megagence.com/nos-consultants?page={str(page)}'
            return next_page

        elif "lafourmi-immo.com" in website:
            self.lafourmi_info['page'] +=1
            page = self.lafourmi_info['page']
            next_page = f"https://www.lafourmi-immo.com/agents?f[geoloc]=bourges&f[radius]=500?page={str(page)}"
            return next_page

        elif "proprietes-privees.com" in website:
            self.proprietes_privees_info['page'] +=1
            page = self.proprietes_privees_info['page']
            next_page = f"https://www.proprietes-privees.com/negociateur/get-mandatary?page={str(page)}&department=0"
            return next_page


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
    "LOG_LEVEL":logging.DEBUG,
    "USER_AGENT": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36',
    "FEEDS":{"data.csv":{'format':'csv'}}

})
crawler.crawl(Seven_Scraper)
crawler.start()

