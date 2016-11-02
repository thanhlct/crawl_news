import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.log import configure_logging

from multiprocessing import Lock
import sys
import logging
import argparse
    
import pdb

store_file = ''
url = ''

crawled_page = 0
#limited_page_number = 17000
limited_page_number = 17
total_page = 0
count_page = 0

lock = Lock()


class ZingSpider(scrapy.Spider):
    name= 'Zing spider'
    #start_urls = ['http://news.zing.vn/giao-thong.html']

    def __init__(self):
        scrapy.Spider.__init__(self)
        self.start_urls = [url]
        self.pages = []
        self.next_page = None

    def parse(self, res):#category pages
        global total_page, count_page
        sys.stderr.write('======process (%d/%d) %s\n'%(crawled_page, limited_page_number, res.url))
        links = res.css('section.cate_content div.cover a::attr(href)')
        total_page = len(links)
        count_page = 0
        for link in links.extract():
            yield scrapy.Request(res.urljoin(link), callback=self.parse_news)

        self.next_page = res.css('p.more a::attr(href)').extract_first()
        self.next_page = res.urljoin(self.next_page)
        

    def parse_news(self, res):#news page
        global count_page, crawled_page
        print(res.url)
        title = res.css('h1::text').extract_first() + '.'
        description = res.css('p.the-article-summary::text').extract_first().strip()
        content = res.css('div.the-article-body p::text').extract()[:-2]
        content = ' '.join([p.strip() for p in content])
        captions = res.css('td.pCaption::text').extract()
        captions = ' '.join([caption.strip() for caption in captions])
             
        page = '\t'.join([res.url, title, description, content, captions])
        page = page.replace('\n', ' ')
        self.pages.append(page)

        lock.acquire()
        try:
            count_page +=1
            if count_page==total_page:
                self.write_file()
                crawled_page += total_page
                if crawled_page < limited_page_number:
                    #process next page until get more than limited page number
                    self.pages = []
                    yield scrapy.Request(self.next_page, callback=self.parse)
                else:
                    sys.stderr.write('======DONE (%d/%d) %s\n'%(crawled_page, limited_page_number, res.url))
        finally:
            lock.release()

    def write_file(self):
        with open(store_file, 'a') as f:
            f.write('\n'.join(self.pages))
            f.write('\n')

def config(args):
    global url, store_file
    url = args.url
    store_file = url[url.rindex('/')+1:url.index('.html')] + '.tsv'

def main(args):
    config(args)
    process = CrawlerProcess({
        'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)',
		'LOG_ENABLED': False,
		#'LOG_LEVEL': logging.DEBUG
    })
    process.crawl(ZingSpider)
    process.start()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Crawl Zing news for a category')
    parser.add_argument('-url', metavar='category.url', dest='url', help='The url of a category', type=str, default='http://news.zing.vn/giao-thong.html')
    args = parser.parse_args()
    main(args)
