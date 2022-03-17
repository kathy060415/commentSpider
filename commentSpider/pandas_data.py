import datetime
import pandas as pd
import numpy as np
import pyarrow
import fsspec
import s3fs

path = 'C:/Users/kathy/Desktop/그린캣소프트/commentSpider/commentSpider/commentSpider/spiders/comments.csv'
df = pd.read_csv(path)

df['Date'] = pd.to_datetime(df['Date'], format = r"%B %d, %Y")

# for i,row in df.iterrows():
#     datetime.datetime.strptime(row.Date, r"%B %d, %Y")

df['Year'] = df['Date'].dt.year
df['Month'] = df['Date'].dt.month
df['Day']=df['Date'].dt.day

new_df = df.drop(['Day', 'Month', 'Year'], axis = 1)

df.to_csv('comments_final', index = False)

#     # # export to s3
#     # tires_df = pd.read_csv("tires.csv")
#     # tires_df.to_parquet('s3://sagemaker-studio-share/datasets/crawling/tirerack/tires.parquet')
i = 2000
j = 1

year_months = df[['Year','Month']]
year_months = year_months[~year_months.duplicated()]

for _,row in year_months.iterrows():
    _year =row.Year
    _month =row.Month
    _suf = datetime.datetime.now().strftime("%y%m%d_%H%M%S")
    OUTPUT_PATH = 's3://sagemaker-studio-share/datasets/crawling/tirerack_review/Year={}/Month={}/tirereview_{}_crawl.parquet'.format(_year, _month, _suf)
    df[np.logical_and(df.Year==_year, df.Month==_month)].drop(['Day', 'Month', 'Year'], axis = 1).to_parquet(OUTPUT_PATH, index = False)

# for i in range(2000, 2022):
#     for j in range(1, 13):
#         OUTPUT_PATH = 's3://sagemaker-studio-share/datasets/crawling/tirerack_review/Year={}/Month={}/tirereview_{}{}_crawl.parquet'.format(i, j, i, j)
#         df[np.logical_and(df.Year==i, df.Month==j)].drop(['Day', 'Month', 'Year'], axis = 1).to_parquet(OUTPUT_PATH, index = False)

