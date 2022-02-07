#!/usr/bin/env python
# coding: utf-8
import sys
import traceback
import xlrd, xlwt
import sqlite3
import datetime
from datetime import datetime
import fake_useragent
import requests
import re
import json
from pprint import pprint
import psycopg2
from psycopg2 import Error
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
class Sql_execute:
    def insert(tab, *data):
        try:
            val = ' VALUES (%s'
            for i in range(1, len(data)):
                val = val + ',%s'
            val = val + ')'
            connection = psycopg2.connect(user="postgres",
                                          # пароль, который указали при установке PostgreSQL
                                          password="admin",
                                          host="127.0.0.1",
                                          port="5432",
                                          database="planes")
            connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            # Курсор для выполнения операций с базой данных
            cursor = connection.cursor()
            cursor.execute("INSERT INTO "+tab+val,(data))
            connection.commit()
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL", error)
        finally:
            if connection:
                cursor.close()
                connection.close()
class Pages():
    def __init__(self, url, referer, cookie):
        self.url=url
        self.referer=referer
        self.cookie=cookie
        
    def yarequest(self):       
        s = requests.Session()
        s.cookies.clear()
        user = fake_useragent.UserAgent().random
        header = {
            'user-agent': user,
            'upgrade-insecure-requests': '1',
            'sec-fetch-user': '?1',
            'sec-fetch-site': 'none',
            'sec-fetch-mode': 'navigate',
            'cookie': self.cookie,
            'sec-fetch-dest': 'document',
            'sec-ch-ua-mobile': '?0',
            'accept': 'text/html,application/xhtml+xml,application/xml;q= 0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q =0.9',
            'accept-encoding': 'gzip,deflate,br',
            'accept-language': 'ru - RU, ru;q = 0.9, en - US;q = 0.8, en;q = 0.7',
            'cache-control': 'max-age=0',
            'referer': self.referer,
            'device-memory': '8',
            'downlink': '10',
            'dpr': '1',
            'ect': '4g',
            'rtt': '50',
            'viewport-width': '1920'
        }
        # proxy = ''
        # s.proxies = {"http": proxy, "https": proxy}
        response = s.get(self.url, headers=header)
        if response.status_code == 200:
            return response.text
        else:
            print('ERROR: status_code not 200')
            print('\n')
            print('status_code = '+str(response.status_code))
            sys.exit()
    
    def get_json(self):        
        # json clearin + loadin
        try:
            text=Pages.yarequest(self)
            a1 = str.find(text, 'dataSource')
            text = text[a1 + 11:] # точно 11?? #да))))
            a2 = str.find(text, '</script>')
            text = text[:a2 - 1]
            yajsn = json.loads(text)
            return(yajsn)
        except Exception as e:
            print('Ошибка:\n', traceback.format_exc())
            print('\n')
            print('Словил капчу/Поменялась контрукция " dataSource -...- </script> "')
    
    def extract_main(self):
        try:
            yaj=Pages.get_json(self)
            # extract data from json
            s = yaj['news']['storyList']
            appear = yaj['news']['time']
            for i in range(len(s)):
                # айди
                id = yaj['news']['storyList'][i]['id']
                # Сообщений за час
                lastHourDocs = yaj['news']['storyList'][i]['lastHourDocs']
                # Просмотры сюжета
                percwatch = yaj['news']['storyList'][i]['fullWatches']
                # Рубрика
                rubricName = yaj['news']['storyList'][i]['rubricName']
                # Глобальный интерес
                generalInterest = yaj['news']['storyList'][i]['stat']['generalInterest']
                # Региональный интерес
                regionalInterest = yaj['news']['storyList'][i]['stat']['regionalInterest']
                # Вес сюжета
                weight = yaj['news']['storyList'][i]['stat']['weight']
                # Документов в сюжете
                storyDocs = yaj['news']['storyList'][i]['storyDocs']
                # Документов в теме
                themeDocs = yaj['news']['storyList'][i]['themeDocs']
                # Сюжетов в теме
                themeStories = yaj['news']['storyList'][i]['themeStories']
                # Cюжет
                title = yaj['news']['storyList'][i]['title']
                title = re.sub("\xa0", " ", title)
                url = yaj['news']['storyList'][i]['url']
                # связанные сюжеты
                related = yaj['news']['storyList'][i]['related']
            print('Parsing MAIN ends now at '+str(datetime.now())+' All OK')
        except Exception as e:
            print('Ошибка:\n', traceback.format_exc())
            print('\n')
            print('Поменялась структура JSON')
            
    def extract_instory(self):
        try:
            yaj=Pages.get_json(self)
            # extract data from json
            #Кол-во подстраниц
            s1 = yaj['news']['instoryPage']
            temp = int(0)
            for i in range(len(s1)):
                if i % 2 == 0:
                    #Кол-во статей на подстраницах
                    s2 = yaj['news']['instoryPage'][i]['docs']
                    for j in range(len(s2)):
                        temp+=1
                        #Заголовок статьи
                        head_post =yaj['news']['instoryPage'][i]['docs'][j]['title'][0]['text']
                        # Текст статьи
                        secondary_post = yaj['news']['instoryPage'][i]['docs'][j]['text'][0]['text']
                        # Время публикации статьи
                        secondary_tags_time = yaj['news']['instoryPage'][i]['docs'][j]['time']
                        # Автор статьи
                        secondary_tags = yaj['news']['instoryPage'][i]['docs'][j]['sourceName']
                         #Ссылка на автора
                        head_post_url = yaj['news']['instoryPage'][i]['docs'][j]['url']
            print('Parsing INSTORY ends now at '+str(datetime.now())+' All OK')
        except Exception as e:
            print('Ошибка:\n', traceback.format_exc())
            print('\n')
            print('Поменялась структура JSON')
    
    def extract_story(self):
        try:
            yaj=Pages.get_json(self)
            # extract data from json
            # Главный заголовок
            main_post = yaj['news']['story']['title']
            # Главный автор
            main_tag = yaj['news']['story']['sourceName']
            #Сыллка на главного автора
            head_post_url = yaj['news']['story']['sourceUrl']
            con = sqlite3.connect('AirisPressDatabase.db')
            c = con.cursor()
            date = str(datetime.today().strftime('%d_%m_%Y'))
            s = yaj['news']['story']['summarization']['items']
            #story_top_side
            for i in range(len(s)):
                #Второстепенный заголовок
                secondary_post = yaj['news']['story']['summarization']['items'][i]['text']
                #Автор второстепенный
                secondary_tags = yaj['news']['story']['summarization']['items'][i]['sourceName']
                #ссылка на автора
                head_post_url = yaj['news']['story']['summarization']['items'][i]['url']
            #story_bottom_side
            s = yaj['news']['story']['tail']
            for i in range(len(s)):
                # Автор истории
                story_tag = yaj['news']['story']['tail'][i]['sourceName']
                # Время появления истории
                story_time=yaj['news']['story']['tail'][i]['time']
                #Заголовок истории
                story_post=yaj['news']['story']['tail'][i]['title']
                #Url истории
                story_url=yaj['news']['story']['tail'][i]['url']
                #неизвестный параметр1
                inclusterAgencyRating=yaj['news']['story']['tail'][i]['docDebugMeta']['inclusterAgencyRating']
                # неизвестный параметр2
                handRulesWeight=yaj['news']['story']['tail'][i]['docDebugMeta']['handRulesWeight']
            print('Parsing STORY ends now at '+str(datetime.now())+' All OK')
        except Exception as e:
            print('Ошибка:\n', traceback.format_exc())
            print('\n')
            print('Поменялась структура JSON')
