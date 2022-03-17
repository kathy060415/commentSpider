import scrapy
from scrapy.crawler import CrawlerProcess
# import pandas as pd
import json

class CommentScrape(scrapy.Spider):
    name = 'comments'
    allowed_domains = ['tirerack.com']
    custom_settings = {
        'DOWNLOADER_MIDDLEWARES': {
            'commentSpider.commentSpider.middlewares.SeleniumMiddleware': 100
        }
    }

    j = 1

    def start_requests(self):
        start_urls = [
            'https://www.tirerack.com/survey/SurveyComments.jsp?&category=tire&additionalComments=y&commentStatus=P&tireMake=BFGoodrich&tireModel=Radial+T%2FA&fromTireDetail=true&tirePageLocQty='
        ]

        for url in start_urls:
            yield scrapy.Request(url=url, callback=self.parse, method='GET', encoding='utf-8')


    def parse(self, response):
        # end_page = response.xpath('/html/body/section[2]/div/div[2]/div[2]/ul/ul/li[47]/a/text()').extract()
        # end = int(end_page[0])
        # end = int(response.xpath('/html/body/section[2]/div/div[2]/div[2]/ul/ul/li[47]/a/text()').extract()[0])
        # end = int(response.xpath('//*[@id="pageSelection47"]/a/text()').extract()[0])

        for i in range(0,10):
            date = response.xpath('//*[@id="surveyDate{}"]/text()'.format(i)).get()
            rating = response.xpath('//*[@id="surveyRating{}"]/text()'.format(i)).get()
            text = response.xpath('//*[@id="additionalComments{}"]/text()'.format(i)).get()
            vehicle = response.xpath('//*[@id="surveyVehicle{}"]/text()'.format(i)).get()
            miles = response.xpath('//*[@id="totalMiles{}"]/text()'.format(i)).extract()
            conditions = response.xpath('//*[@id="surveyDrivingCondition{}"]/text()'.format(i)).extract()
            location = response.xpath('//*[@id="surveyLocation{}"]/text()'.format(i)).extract()
            style = response.xpath('//*[@id="surveyDrivingStyle{}"]/text()'.format(i)).extract()

            comment = {
                'Date': date,
                'Rating': rating,
                'Review': text,
                'Vehicle Name': vehicle,
                'Miles Driven on Tires': miles,
                'Driving Conditions': conditions,
                'Location': location,
                'Driving Style': style,
            }

            yield comment

        #add url to next page (parse to next page)

        next_page = 'https://www.tirerack.com/survey/SurveyComments.jsp?&page={}&category=tire&additionalComments=y&commentStatus=P&tireMake=BFGoodrich&tireModel=Radial+T%2FA&fromTireDetail=true&tirePageLocQty='.format(self.j)

        self.j += 1
        if next_page is not None and self.j <= 48:
            next_page = response.urljoin(next_page)
            yield response.follow(next_page, callback=self.parse)
        # 'https://www.tirerack.com/survey/SurveyComments.jsp?&page={}&category=tire&additionalComments=y&commentStatus=P&tireMake=BFGoodrich&tireModel=Radial+T%2FA&fromTireDetail=true&tirePageLocQty='


#터미널 없이 크롤러 작동
def main(event, context):
    # TODO implement

    process = CrawlerProcess(settings={
        # "FEEDS":{
        #     "tires.csv": {"format": "csv"},
        # },
        'FEED_FORMAT': 'csv',
        'FEED_URI': 'comments.csv',
        'USER_AGENT': 'Mozilla/5.0(Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36'
    })

    process.crawl(CommentScrape)
    process.start()
#
#     # # export to s3
#     # tires_df = pd.read_csv("tires.csv")
#     # tires_df.to_parquet('s3://sagemaker-studio-share/datasets/crawling/tirerack/tires.parquet')
#     # OUTPUT_PATH = s3://sagemaker-studio-share/datasets/crawling/tirerack_review/Year={row.Year}/Month={row.Month}
#     # return {
#     #     'statusCode': 200,
#     #     'body': json.dumps('Hello from Lambda!')
#     # }
#
#
main('', '')