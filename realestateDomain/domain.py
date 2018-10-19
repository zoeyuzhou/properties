import scrapy
import re
import time
from lxml import etree


class domainSpider(scrapy.Spider):
    name = 'domainSpider'
    redis_key = 'domainSpider:urls'
    f = open("../data/property_id.txt", "w+")
    f2 = open("../data/property_school.csv", "w+")

    user_agent = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.22 \
                             Safari/537.36 SE 2.X MetaSr 1.0'
    headers = {'User-Agent': user_agent}

    # start_urls = 'https://www.domain.com.au/sale/castle-hill-nsw-2154/house/?bedrooms=4-any&bathrooms=2-any&price=0-1300000&excludeunderoffer=1&ssubs=1'
    # start_urls = 'https://www.domain.com.au/sale/ermington-nsw-2115/house/?bedrooms=4-any&bathrooms=2-any&price=0-1400000&excludeunderoffer=1'
    start_urls = 'https://www.domain.com.au/sale/?suburb=castle-hill-nsw-2154,baulkham-hills-nsw-2153,cherrybrook-nsw-2126,' \
                 'ermington-nsw-2115,west-ryde-nsw-2114,denistone-west-nsw-2114,dundas-nsw-2117,dundas-valley-nsw-2117,' \
                 'carlingford-nsw-2118&ptype=house&bedrooms=4-any&bathrooms=2-any&price=0-1400000&excludeunderoffer=1&sort=inspectiontime-asc'

    def start_requests(self):
        yield scrapy.Request(url=self.start_urls, headers=self.headers, method='GET', callback=self.parse)

    def parse(self, response):
        lists = response.body.decode('utf-8')
        selector = etree.HTML(lists)
        area_list = selector.xpath('//*[@id="skip-link-content"]/div[3]/ul/li/div/div[2]/div[2]/div[2]//a')
        area2_list = selector.xpath('//*[@id="skip-link-content"]/div[3]/ul/li/div/div[2]/div[2]/a')

        page = selector.xpath('//*[@id="skip-link-content"]/div[3]/div[2]/div/a[5]/text()')
        for area in area_list + area2_list:
            try:
                area_url = area.xpath('@href').pop()
                property_address = re.sub(r'https://www\.domain\.com\.au/', '', area_url)
                property_list = property_address.split("-")
                property_id = property_list[-1]
                print(area_url)
                print(property_id)
                self.f.write(property_id + '\n')
                yield scrapy.Request(url=area_url, headers=self.headers, callback=self.detail_url)
            except Exception:
                pass

        # int_page = int(page[0])
        # TODO: make int_page valid
        for i in range(2, 3):
            url = self.start_urls + '&page={}'.format(str(i))
            # url = 'https://www.domain.com.au/sale/castle-hill-nsw-2154/house/?bedrooms=4-any&bathrooms=2-any&price=0-1300000&excludeunderoffer=1&ssubs=1&page={}'.format(str(i))
            yield scrapy.Request(url, headers=self.headers, method='GET', callback=self.parse)

    def detail_url(self, response):
        time.sleep(2)
        lists = response.body.decode('utf-8')
        selector = etree.HTML(lists)
        url = response.request.url
        url_list = url.split("-")
        property_id = url_list[-1]
        property_address = re.sub(r'https://www\.domain\.com\.au/', '', url)

        self.f2.write(property_id + ', ')
        self.f2.write(property_address + ', ')

        school_name_list = selector.xpath('//h5[@class="school-catchment__school-title"]/text()')
        school_distance_list = selector.xpath('//div[@class="school-catchment__school-distance"]/text()')
        school_distance_list = [x for x in school_distance_list if x != ' away']
        for i in range(0, len(school_name_list)):
            self.f2.write(school_name_list[i] + ', ' + school_distance_list[i] + ', ')

        self.f2.write('\n')


