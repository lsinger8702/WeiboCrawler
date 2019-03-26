from selenium import webdriver 
from urllib.parse import  urlencode
from datetime import timedelta, datetime

import requests
import json
import time
import pymongo
import re
import os

PROXY_POOL_URL = 'http://127.0.0.1:5000/random'
base_url = 'https://m.weibo.cn/api/container/getIndex?'
keywords = [ '金证股份',  '中国软件','恒生电子', '贵州茅台', '中昌数据', 
    '上证', '深证', '创业板', '中证', '建设银行','海康威视', '华胜天成', 
    '京东方', '万科A', '沪指', '歌华有线', '招商银行', '中国联通', '用友网络' ]
#keywords = []

# Redis 
#redis = StrictRedis(host='localhost', port=6379, db=3, password=None)

# pymongo
client = pymongo.MongoClient(host='localhost', port=27017)
db = client.weibo
collection = db[keywords[0]]

def get_proxy():
    headers = {
        'Connection': 'close'
    }
    os.environ['no_proxy'] = '127.0.0.1, localhost'
    try:
        response = requests.get(PROXY_POOL_URL, headers=headers)
        if response.status_code == 200:
            return response.text
    except:
        print('Error: get proxy')
        time.sleep(5)
        return None

def get_page(keyword, page, proxies):
    params = {
        'type': 'wb',
        'queryVal': keyword,
        'containerid': '100103type=2&q=' + keyword,
        'page': page
    }
    url = base_url + urlencode(params)
    try:
        response = requests.get(url, proxies=proxies, timeout=(5, 30))
        if response.status_code == 200:
            return response.json()
        else:
        	return None
    except :
        print('Error: get page')
        return None

def parse_page(json):
    if json:
        items = json.get('data').get('cards')[0]['card_group']
        for item in items:            
            item = item.get('mblog')

            if '前' in item.get('created_at') or '刚' in item.get('created_at'):
                pubtime = time.strftime("%Y-%m-%d", time.localtime())
            elif '昨天' in item.get('created_at'):
            	yesterday = datetime.today() + timedelta(-1)
            	yesterday_format = yesterday.strftime('%Y-%m-%d')
            	pubtime = yesterday_format
            else:
                pubtime = item.get('created_at')
                if re.match('^\d\d-\d\d$', pubtime):
                    pubtime = '2019-' + pubtime

            if item.get('longText'):
                text = item.get('longText').get('longTextContent')
            else:
                text = item.get('text')

            w_id = str(item.get('id'))
            user_id = str(item.get('user').get('id'))
            published_time = pubtime
            w_text = text
            attitudes = item.get('attitudes_count')
            comments = item.get('comments_count')
            reposts = item.get('reposts_count')
            source = item.get('source')
            
            if collection.find({'w_id': w_id}).count() == 0:
            	tmp = {'w_id': w_id, 'user_id': user_id, 'published_time': published_time,
            'w_text': w_text, 'attitudes': attitudes, 'comments': comments,
            'reposts': reposts, 'source': source}
            	res = collection.insert(tmp)
            else:
            	condition = {'w_id': w_id, 'published_time': {'$regex': '昨天'}}
            	newValues = {'$set': {'published_time': published_time}}
            	collection.update_one(condition, newValues)

def main(s, t):
    proxy = get_proxy()
    while(proxy == None):
        time.sleep(5)
        proxy = get_proxy()
    print('proxy', proxy)
    proxies = {
        'http': 'http://' + proxy,
        'https': 'https://' + proxy
    }
    #proxies = None
    for keyword in keywords:
        global collection 
        collection = db[keyword]
        print('keyword', keyword)
        for page in range(s, t):
            print('page', str(page), keyword)
            json = get_page(keyword, page, proxies)
            while json == None:
                proxy = get_proxy()
                if not proxy:
                	print('------------------Cannot get proxy------------------')
                	continue
                proxies = {
                    'http': 'http://' + proxy,
                    'https': 'https://' + proxy
                }
                print('------------------Proxy changed------------------')
                print('proxy', proxy)
                json = get_page(keyword, page, proxies)
            if json:
                if json['ok']:
                    parse_page(json) 
                    #print('count', redis.dbsize())    
                    print(collection.find().count())               
                else:
                    print('------------------' + keyword + ' Done------------------')
                    print('page', str(page))
                    break


if __name__ == '__main__':
	main(1, 200)
