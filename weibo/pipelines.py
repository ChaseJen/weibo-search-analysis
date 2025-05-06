# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import copy
import csv
import os

import scrapy
from scrapy.exceptions import DropItem
from scrapy.pipelines.files import FilesPipeline
from scrapy.pipelines.images import ImagesPipeline
from scrapy.utils.project import get_project_settings
from weibo.utils.sentiment import get_sentiment_score
from elasticsearch import Elasticsearch
import json
from datetime import datetime

settings = get_project_settings()


class ElasticsearchPipeline(object):
    def __init__(self):
        self.es = Elasticsearch(["http://localhost:9200"])
        self.index_name = "guoping"
        self.create_index()

    def normalize_time(self, item):
        created_at = item['weibo'].get('created_at', '').strip()
        if len(created_at) == 16:
            item['weibo']['created_at'] = created_at + ':00'
        elif len(created_at) == 19:
            pass
        else:
            print(f"⚠️ 无效时间格式: {created_at}，设为默认值")
            item['weibo']['created_at'] = '1970-01-01 00:00:00'
        return item

    def create_index(self):
        if not self.es.indices.exists(index=self.index_name):
            index_mapping = {
                "mappings": {
                    "properties": {
                        "id": {"type": "keyword"},
                        "bid": {"type": "keyword"},
                        "user_id": {"type": "keyword"},
                        "screen_name": {"type": "text"},
                        "text": {"type": "text"},
                        "article_url": {"type": "text"},
                        "location": {"type": "text"},
                        "at_users": {"type": "text"},
                        "topics": {"type": "keyword"},
                        "reposts_count": {"type": "integer"},
                        "comments_count": {"type": "integer"},
                        "attitudes_count": {"type": "integer"},
                        "created_at": {
                            "type": "date",
                            "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd HH:mm"
                        },
                        "source": {"type": "text"},
                        "pics_url": {"type": "text"},
                        "video_url": {"type": "text"},
                        "retweet_id": {"type": "keyword"},
                        "ip": {"type": "keyword"},
                        "user_authentication": {"type": "keyword"},
                        "score": {"type": "float"},
                        "keyword": {"type": "keyword"}
                    }
                }
            }
            try:
                self.es.indices.create(index=self.index_name, body=index_mapping)
                print(f"索引 {self.index_name} 已创建")
            except Exception as e:
                print(f"创建索引时出错: {e}")

    def process_item(self, item, spider):
        if item:
            text = item['weibo']['text']
            print(f"当前微博文本: {text}")

            score = get_sentiment_score(text)
            if score is None:
                score = 5  # 兜底处理，防止插入失败
            print(f"当前微博情感得分: {score}")

            topics = item['weibo'].get('topics', '')
            topics_list = [topic.strip() for topic in topics.split(',') if topic.strip()] if topics else []

            item = self.normalize_time(item)

            doc = {
                "id": item['weibo'].get('id', ''),
                "bid": item['weibo'].get('bid', ''),
                "user_id": item['weibo'].get('user_id', ''),
                "screen_name": item['weibo'].get('screen_name', ''),
                "text": item['weibo'].get('text', ''),
                "article_url": item['weibo'].get('article_url', ''),
                "location": item['weibo'].get('location', ''),
                "at_users": item['weibo'].get('at_users', ''),
                "topics": topics_list,
                "reposts_count": item['weibo'].get('reposts_count', 0),
                "comments_count": item['weibo'].get('comments_count', 0),
                "attitudes_count": item['weibo'].get('attitudes_count', 0),
                "created_at": item['weibo'].get('created_at', ''),
                "source": item['weibo'].get('source', ''),
                "pics_url": ','.join(item['weibo'].get('pics', [])),
                "video_url": item['weibo'].get('video_url', ''),
                "retweet_id": item['weibo'].get('retweet_id', ''),
                "ip": item['weibo'].get('ip', ''),
                "user_authentication": item['weibo'].get('user_authentication', ''),
                "score": score,
                "keyword": item['weibo'].get('keyword', '')
            }

            try:
                response = self.es.index(index=self.index_name, body=doc, request_timeout=30)
                print(f"文档已存入 Elasticsearch，响应: {response}")
            except Exception as e:
                print(f"将文档存入 Elasticsearch 失败: {e}")
        
        return item



# class CsvPipeline(object):
#     def process_item(self, item, spider):
#         base_dir = 'resources' + os.sep + item['keyword']
#         if not os.path.isdir(base_dir):
#             os.makedirs(base_dir)
#         file_path = base_dir + os.sep + item['keyword'] + '.csv'
#         if not os.path.isfile(file_path):
#             is_first_write = 1
#         else:
#             is_first_write = 0

#         if item:
#             text = item['weibo']['text']
#             print(f"当前微博文本: {text}")  # 调试用，打印微博文本，确保文本存在
#             score = get_sentiment_score(text)
#             print(f"当前微博情感得分: {score}")  # 调试用，打印微博情感得分，确保得分存在

#             with open(file_path, 'a', encoding='utf-8-sig', newline='') as f:
#                 writer = csv.writer(f)
#                 if is_first_write:
#                     header = [
#                         'id', 'bid', 'user_id', 'screen_name', 'text', 'article_url',
#                         'location', 'at_users', 'topics', 'reposts_count', 'comments_count', 'attitudes_count', 'created_at',
#                         'source', 'pics_url', 'video_url', 'retweet_id', 'ip', 'user_authentication','score','keyword'
#                     ]
#                     writer.writerow(header)

#                 writer.writerow([
#                     item['weibo'].get('id', ''),
#                     item['weibo'].get('bid', ''),
#                     item['weibo'].get('user_id', ''),
#                     item['weibo'].get('screen_name', ''),
#                     item['weibo'].get('text', ''),
#                     item['weibo'].get('article_url', ''),
#                     item['weibo'].get('location', ''),
#                     item['weibo'].get('at_users', ''),
#                     item['weibo'].get('topics', ''),
#                     item['weibo'].get('reposts_count', ''),
#                     item['weibo'].get('comments_count', ''),
#                     item['weibo'].get('attitudes_count', ''),
#                     item['weibo'].get('created_at', ''),
#                     item['weibo'].get('source', ''),
#                     ','.join(item['weibo'].get('pics', [])),
#                     item['weibo'].get('video_url', ''),
#                     item['weibo'].get('retweet_id', ''),
#                     item['weibo'].get('ip', ''),
#                     item['weibo'].get('user_authentication', ''),
#                     score,
#                     item['weibo'].get('keyword', '')
#                 ])
#         return item

# class MyImagesPipeline(ImagesPipeline):
#     def get_media_requests(self, item, info):
#         if len(item['weibo']['pics']) == 1:
#             yield scrapy.Request(item['weibo']['pics'][0],
#                                  meta={
#                                      'item': item,
#                                      'sign': ''
#                                  })
#         else:
#             sign = 0
#             for image_url in item['weibo']['pics']:
#                 yield scrapy.Request(image_url,
#                                      meta={
#                                          'item': item,
#                                          'sign': '-' + str(sign)
#                                      })
#                 sign += 1

#     def file_path(self, request, response=None, info=None):
#         image_url = request.url
#         item = request.meta['item']
#         sign = request.meta['sign']
#         base_dir = 'resources' + os.sep + item['keyword'] + os.sep + 'images'
#         if not os.path.isdir(base_dir):
#             os.makedirs(base_dir)
#         image_suffix = image_url[image_url.rfind('.'):]
#         file_path = base_dir + os.sep + item['weibo'][
#             'id'] + sign + image_suffix
#         return file_path


# class MyVideoPipeline(FilesPipeline):
#     def get_media_requests(self, item, info):
#         if item['weibo']['video_url']:
#             yield scrapy.Request(item['weibo']['video_url'],
#                                  meta={'item': item})

#     def file_path(self, request, response=None, info=None):
#         item = request.meta['item']
#         base_dir = 'resources' + os.sep + item['keyword'] + os.sep + 'videos'
#         if not os.path.isdir(base_dir):
#             os.makedirs(base_dir)
#         file_path = base_dir + os.sep + item['weibo']['id'] + '.mp4'
#         return file_path


# class MongoPipeline(object):
#     def open_spider(self, spider):
#         try:
#             from pymongo import MongoClient
#             self.client = MongoClient(settings.get('MONGO_URI'))
#             self.db = self.client['weibo']
#             self.collection = self.db['weibo']
#         except ModuleNotFoundError:
#             spider.pymongo_error = True

#     def process_item(self, item, spider):
#         try:
#             import pymongo

#             new_item = copy.deepcopy(item)
#             if not self.collection.find_one({'id': new_item['weibo']['id']}):
#                 self.collection.insert_one(dict(new_item['weibo']))
#             else:
#                 self.collection.update_one({'id': new_item['weibo']['id']},
#                                            {'$set': dict(new_item['weibo'])})
#         except pymongo.errors.ServerSelectionTimeoutError:
#             spider.mongo_error = True

#     def close_spider(self, spider):
#         try:
#             self.client.close()
#         except AttributeError:
#             pass


# class MysqlPipeline(object):
#     def create_database(self, mysql_config):
#         """创建MySQL数据库"""
#         import pymysql
#         sql = """CREATE DATABASE IF NOT EXISTS %s DEFAULT
#             CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci""" % settings.get(
#             'MYSQL_DATABASE', 'weibo')
#         db = pymysql.connect(**mysql_config)
#         cursor = db.cursor()
#         cursor.execute(sql)
#         db.close()

#     def create_table(self):
#         """创建MySQL表"""
#         sql = """
#                 CREATE TABLE IF NOT EXISTS weibo (
#                 id varchar(20) NOT NULL,
#                 bid varchar(12) NOT NULL,
#                 user_id varchar(20),
#                 screen_name varchar(30),
#                 text varchar(2000),
#                 article_url varchar(100),
#                 topics varchar(200),
#                 at_users varchar(1000),
#                 pics varchar(3000),
#                 video_url varchar(1000),
#                 location varchar(100),
#                 created_at DATETIME,
#                 source varchar(30),
#                 attitudes_count INT,
#                 comments_count INT,
#                 reposts_count INT,
#                 retweet_id varchar(20),
#                 PRIMARY KEY (id),
#                 ip varchar(100),
#                 user_authentication varchar(100)
#                 ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"""
#         self.cursor.execute(sql)

#     def open_spider(self, spider):
#         try:
#             import pymysql
#             mysql_config = {
#                 'host': settings.get('MYSQL_HOST', 'localhost'),
#                 'port': settings.get('MYSQL_PORT', 3306),
#                 'user': settings.get('MYSQL_USER', 'root'),
#                 'password': settings.get('MYSQL_PASSWORD', '771130rqr'),
#                 'charset': 'utf8mb4'
#             }
#             self.create_database(mysql_config)
#             mysql_config['db'] = settings.get('MYSQL_DATABASE', 'weibo')
#             self.db = pymysql.connect(**mysql_config)
#             self.cursor = self.db.cursor()
#             self.create_table()
#         except ImportError:
#             spider.pymysql_error = True
#         except pymysql.OperationalError:
#             spider.mysql_error = True

#     def process_item(self, item, spider):
#         data = dict(item['weibo'])
#         data['pics'] = ','.join(data['pics'])
#         keys = ', '.join(data.keys())
#         values = ', '.join(['%s'] * len(data))
#         sql = """INSERT INTO {table}({keys}) VALUES ({values}) ON
#                      DUPLICATE KEY UPDATE""".format(table='weibo',
#                                                     keys=keys,
#                                                     values=values)
#         update = ','.join([" {key} = {key}".format(key=key) for key in data])
#         sql += update
#         try:
#             self.cursor.execute(sql, tuple(data.values()))
#             self.db.commit()
#         except Exception:
#             self.db.rollback()
#         return item

#     def close_spider(self, spider):
#         try:
#             self.db.close()
#         except Exception:
#             pass


class DuplicatesPipeline(object):
    def __init__(self):
        self.ids_seen = set()

    def process_item(self, item, spider):
        if item['weibo']['id'] in self.ids_seen:
            raise DropItem("过滤重复微博: %s" % item)
        else:
            self.ids_seen.add(item['weibo']['id'])
            return item
