import json
import logging
import re
import scrapy
from scrapy.spiders import Spider
from scrapy.crawler import CrawlerProcess
from argparse import ArgumentParser
from scrapy.spidermiddlewares.httperror import HttpError



class Controller():
    def __init__(self):
        self.spiders = {
            "iadfrance_spider": Iadfrance_Scraper,
            "safti_spider": Safti_Scraper,
            "bskimmobilier_spider": Bskimmobilier_Scraper,
            "megagence_spider": Megagence_Scraper,
            "lafourmi_spider": Lafourmi_Scraper,
            "efficity_spider": Efficity_Scraper,
            "proprietes_privees_spider": Proprietes_Privees_Scraper,
        }

    def take_args(self):
        parser = ArgumentParser()
        parser.add_argument('-s', '--spider', dest='spider', help='Give the spider name', default=None)
        values = parser.parse_args()
        args_dict = vars(values)
        return args_dict

    def clean(self, data):
        if type(data) == list:
            return " ".join(data).strip()
        elif data:
            return data.strip()
        else:
            return ''

    def init_crawler(self, spider_name):
        if spider_name:
            spider_class = self.spiders[spider_name]
            crawler = CrawlerProcess(settings={
                "HTTPCACHE_ENABLED": True,
                "URLLENGTH_LIMIT": 10000,
                "REQUEST_FINGERPRINTER_IMPLEMENTATION": '2.7',
                "ROBOTSTXT_OBEY": False,
                # "LOG_LEVEL": logging.DEBUG,
                "LOG_ENABLED": False,
                "USER_AGENT": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36',
                "FEEDS":{f"{spider_name}.csv":{'format':'csv'}}

            })
            crawler.crawl(spider_class)
            crawler.start()
        else:
            print("\n [+] python scraper.py -s *spider_name* :\n")
            for n, spider in enumerate(self.spiders.keys(), start=1):
                print(f"    {n}. {spider}")
            print()
        




###################   Spiders   ################### 


class Iadfrance_Scraper(Spider, Controller):
    name = "iadfrance_spider"

    iadfrance_info = {
        'urls': {
            "https://www.iadfrance.fr/trouver-un-conseiller/hauts-de-france": {
                'page':1,
                'group_ids': [],
            },
            "https://www.iadfrance.fr/trouver-un-conseiller/grand-est": {
                'page':1,
                'group_ids': [],
            },
            "https://www.iadfrance.fr/trouver-un-conseiller/bourgogne-franche-comte": {
                'page': 1,
                'group_ids': [],
            },
            "https://www.iadfrance.fr/trouver-un-conseiller/auvergne-rhone-alpes": {
                'page': 1,
                'group_ids': [],
            },
            "https://www.iadfrance.fr/trouver-un-conseiller/provence-alpes-cote-dazur": {
                'page': 1,
                'group_ids': [],
            },
            "https://www.iadfrance.fr/trouver-un-conseiller/occitanie": {
                'page': 1,
                'group_ids': [],
            },
            "https://www.iadfrance.fr/trouver-un-conseiller/nouvelle-aquitaine": {
                'page': 1,
                'group_ids': [],
            },
            "https://www.iadfrance.fr/trouver-un-conseiller/centre-val-de-loire": {
                'page': 1,
                'group_ids': [],
            },
            "https://www.iadfrance.fr/trouver-un-conseiller/pays-de-la-loire": {
                'page': 1,
                'group_ids': [],
            },
            "https://www.iadfrance.fr/trouver-un-conseiller/normandie": {
                'page': 1,
                'group_ids': [],
            },
            "https://www.iadfrance.fr/trouver-un-conseiller/bretagne": {
                'page': 1,
                'group_ids': [],
            },
            "https://www.iadfrance.fr/trouver-un-conseiller/ile-de-france": {
                'page': 1,
                'group_ids': [],
            },
        },
        'headers':{
          'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
          'Content-Type': 'application/json',
          'Accept': 'application/json',
          'X-Requested-With': 'XMLHttpRequest',
          'host': 'www.iadfrance.fr',
          'Referer': 'https://www.iadfrance.fr'
        },
    }


    def start_requests(self):
        for url in self.iadfrance_info['urls'].keys():
            yield scrapy.Request(url, callback=self.parse_iadfrance, cb_kwargs={'first_req':True, 'base_url':url})

    def parse_iadfrance(self, response, first_req, base_url):
        if first_req:
            for agent in response.xpath("//div[contains(@class,'agent_card') and contains(@class,'onResult')]"):
                # update grp_ids for each link
                group_id = agent.xpath(".//a[contains(@data-gtm,'email')]/@id").get()
                self.iadfrance_info['urls'][base_url]['group_ids'].append(group_id)

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
                ignore_ids, next_page = self.get_nextPage(base_url)
                yield scrapy.FormRequest(url=base_url, method='GET', callback=self.parse_iadfrance, formdata={"ignore_ids":ignore_ids, "page":str(next_page)},cb_kwargs={'first_req': None, "base_url":base_url},headers=self.iadfrance_info['headers'])
        else:
            body = response.body
            data = json.loads(body)
            if data.get('html'):
                sel = scrapy.Selector(text=data.get('html'))
                for agent in sel.css(".agent_card.flex.column.items-center.justify-center.onResult"):
                    # update grp_ids for each link
                    group_id = agent.css(".agent-contact-form.full-width.i-btn.i-btn--secondary::attr('id')").get().strip()
                    self.iadfrance_info['urls'][base_url]['group_ids'].append(group_id)

                    name = agent.css('.text-biggy.text-weight-bolder.text-none.agent_name::text').get()
                    phone = agent.css("a[id='adphone']::attr('data-phone')").get()
                    address = agent.css(".text-darkblue.text-biggy.text-none.agent_card_location::text").getall()[-1]
                    email = ''
                    item = {
                        "name": self.clean(name),
                        "phone": self.clean(phone),
                        "address": self.clean(address),
                        "email": self.clean(email)
                    }
                    yield item
                if data.get("nextUrl"):
                    ignore_ids, next_page = self.get_nextPage(base_url)
                    yield scrapy.FormRequest(url=base_url, method='GET', callback=self.parse_iadfrance, formdata={"ignore_ids":ignore_ids, "page":str(next_page)}, cb_kwargs={'first_req':None, "base_url":base_url}, headers=self.iadfrance_info['headers'], errback=self.handle_err)


    def get_nextPage(self, base_url):
        self.iadfrance_info['urls'][base_url]['page'] +=1
        page = self.iadfrance_info['urls'][base_url]['page']
        ignore_ids = "-".join(self.iadfrance_info['urls'][base_url]['group_ids'])
        return ignore_ids, page


    def handle_err(self, failure):
        if failure.check(HttpError):
            pass
            

class Safti_Scraper(Spider):
    name = "safti_spider"

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


    def start_requests(self):
        url = self.safti_info['url']
        yield scrapy.FormRequest(url, callback=self.parse_safti, headers=self.safti_info['headers'], formdata=self.safti_info['initial_payload'])


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
            payload = self.get_nextPage()
            yield scrapy.FormRequest(url=response.url, callback=self.parse_safti, formdata=payload, headers=self.safti_info['headers'], dont_filter=True)


    def get_nextPage(self):
        self.safti_info['page'] += 1
        page = self.safti_info['page']
        payload = {"page": str(page), "limit": '9'}
        return payload



class Bskimmobilier_Scraper(Spider, Controller):
    name = "bskimmobilier_spider"

    bskimmobilier_info = {
        'url': "https://bskimmobilier.com/commercial-immobilier",
        'page': 1
    }

    def start_requests(self):
        url = self.bskimmobilier_info['url']
        yield scrapy.Request(url, callback=self.parse_bskimmobilier)


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
            next_page = self.get_nextPage()
            yield scrapy.Request(url=next_page, callback=self.parse_bskimmobilier)


    def get_nextPage(self):
        self.bskimmobilier_info['page'] += 1
        page = self.bskimmobilier_info['page']
        next_page = f'https://bskimmobilier.com/commercial-immobilier?page={str(page)}'
        return next_page



class Megagence_Scraper(Spider, Controller):
    name = 'megagence_spider'

    megagence_info = {
        'url': "https://www.megagence.com/nos-consultants",
        'page': 1
    }


    def start_requests(self):
        url = self.megagence_info['url']
        yield scrapy.Request(url, callback=self.parse_megagence)


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
            next_page = self.get_nextPage()
            yield scrapy.Request(url=next_page, callback=self.parse_megagence)


    def get_nextPage(self):
        self.megagence_info['page'] += 1
        page = self.megagence_info['page']
        next_page = f'https://www.megagence.com/nos-consultants?page={str(page)}'
        return next_page



class Lafourmi_Scraper(Spider, Controller):
    name = 'lafourmi_spider'

    lafourmi_info = {
        'url': "https://www.lafourmi-immo.com/agents?f%5Bgeoloc%5D=bourges&f%5Bradius%5D=500",
        'page': 1,
        "headers" : {
          'Accept': '*/*',
          'X-Requested-With': 'XMLHttpRequest',
          'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
          'host': 'www.lafourmi-immo.com',
          'Content-Type': 'application/json',
        }
    }


    def start_requests(self):
        url = self.lafourmi_info['url']
        yield scrapy.Request(url, callback=self.parse_lafourmi, headers=self.lafourmi_info['headers'])

    def parse_lafourmi(self, response):
        for agent in response.xpath("//a[contains(@href,'/agents/')]/@href").getall():
            url = response.urljoin(agent)
            yield scrapy.Request(url, callback=self.parse_lafourmi_helper)
        if response.xpath("//a[contains(@href,'/agents/')]"):
            next_page = self.get_nextPage()
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

    def get_nextPage(self):
        self.lafourmi_info['page'] +=1
        page = self.lafourmi_info['page']
        next_page = f"https://www.lafourmi-immo.com/agents?f%5Bgeoloc%5D=bourges&f%5Bradius%5D=500&page={str(page)}"
        return next_page



class Efficity_Scraper(Spider, Controller):
    name = 'efficity_spider'


    def start_requests(self):
        url = 'https://www.efficity.com/consultants-immobiliers/liste/'
        yield scrapy.Request(url, callback=self.parse_efficity)


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



class Proprietes_Privees_Scraper(Spider, Controller):
    name = 'proprietes_privees_spider'


    proprietes_privees_info = {
        'url': "https://www.proprietes-privees.com/negociateur/get-mandatary?page=1&department=0",
        "page": 1
    }


    def start_requests(self):
        url = self.proprietes_privees_info['url']
        yield scrapy.Request(url, callback=self.parse_proprietes_privees)


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
            next_page = self.get_nextPage()
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


    def get_nextPage(self):
        self.proprietes_privees_info['page'] +=1
        page = self.proprietes_privees_info['page']
        next_page = f"https://www.proprietes-privees.com/negociateur/get-mandatary?page={str(page)}&department=0"
        return next_page




######## Control #########

control = Controller()
args = control.take_args()
control.init_crawler(args['spider'])

