import os
import time
from datetime import datetime

import requests
import json

import logging
from log import Logger

import threading
from queue import Queue

import pymysql
from pymysql.converters import escape_string


MAX_QUEUE_SIZE = 100
urlQueue = Queue(MAX_QUEUE_SIZE)
jsonQueue = Queue(MAX_QUEUE_SIZE)



class CrawlThread(threading.Thread):
    def __init__(self, thread_id):
        super(CrawlThread, self).__init__()
        self.thread_id = thread_id
        self.logger = Logger("./logs/crawl-"+str(thread_id)+".log", logging.DEBUG, __name__ + str(thread_id)).getlog()
        self.queue = urlQueue
        self.data_queue = jsonQueue
        
        self.token_id = -1
        self.token_list = []
        with open("./api/github/auth_token.txt", "r") as f:
            for line in f.readlines():
                self.token_list.append(line.strip())
        self.header = self.updateHeaderToken()

    def run(self):
        self.logger.info("Crawl " + str(self.thread_id) + " start")
        self.crawlSpider()
        self.logger.info("Crawl " + str(self.thread_id) + " end")
        

    def crawlSpider(self):
        global urlQueue
        global jsonQueue
        while True:
            if urlQueue.empty():
                self.logger.info("Crawl " + str(self.thread_id) + " wait")
                time.sleep(10)
                if urlQueue.empty():
                    self.logger.info("Crawl " + str(self.thread_id) + " exit")
                    break
            else:
                item = urlQueue.get()
                url = item['url']
                # self.logger.info("Crawl " + str(self.thread_id) + " crawl " + url)
                # crawl
                try:
                    response = requests.get(url, headers=self.header, timeout=30)
                    response.raise_for_status()
                    # self.logger.info("Crawl " + str(self.thread_id) + " crawl " + url + " success")
                except Exception as e:
                    self.logger.error("Crawl " + str(self.thread_id) + " crawl " + url + " failed")
                    self.logger.error(e)
                    if response.status_code == 403:
                        self.header = self.updateHeaderToken()
                        self.logger.error("token " + self.token_list[self.token_id] + " expired, switch to token " + self.token_list[self.token_id] + "; left " + str(len(self.token_list) - self.token_id) + " tokens")
                    continue

                if response.json() == []:
                    # self.logger.info("Crawl " + str(self.thread_id) + " crawl " + url + " empty")
                    continue
                self.data_queue.put({'id': item['id'], 'data': response.json()})
                urlQueue.task_done()



    def updateHeaderToken(self):
        self.token_id += 1
        if self.token_id == len(self.token_list):
            self.token_id = 0
            time.sleep(1000)
        return {'Authorization': 'Bearer ' + self.token_list[self.token_id]}
    

class ParseThread(threading.Thread):
    def __init__(self, thread_id, target_json,key, params):
        super(ParseThread, self).__init__()
        self.thread_id = thread_id
        self.logger = Logger("./logs/parse-"+str(thread_id)+".log", logging.DEBUG, __name__ + str(thread_id)).getlog()
        # self.queue = urlQueue
        self.json_file = target_json
        self.key = key
        self.params = params

        self.conn = pymysql.connect(
            host='localhost',
            port=3306,
            user='root',
            password='heikediguo',
            database='crawl',
            charset='utf8',
        )


    def run(self):
        self.logger.info("Parse " + str(self.thread_id) + " start")
        self.parseSpider()
        self.logger.info("Parse " + str(self.thread_id) + " end")

    def generate_json(self):
        with open("./temp_data/json_gz/"+self.json_file, 'r', encoding='utf-8') as f:
            for line in f:
                yield json.loads(line)


    def insertDB(self,insert_list):
        insert_sql_list = {
            "user_data": """
            insert into user(                login,id,node_id,type,site_admin,name,company,blog,location,email,hireable,bio,twitter_username,public_repos,public_gists,followers,following,created_at,updated_at
            )
            values(
                %s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,
                %s,%s,%s,%s
            ) as new
            on DUPLICATE KEY UPDATE
            public_repos= IF(user.updated_at < new.updated_at, new.public_repos, user.public_repos),
            public_gists= IF(user.updated_at < new.updated_at, new.public_gists, user.public_gists),
            followers= IF(user.updated_at <new.updated_at, new.followers, user.followers),
            following= IF(user.updated_at < new.updated_at, new.following, user.following),
            updated_at= IF(user.updated_at < new.updated_at, new.updated_at, user.updated_at);
        """,
            "repo_data": """
            insert into repository(
                id,node_id,name,full_name,private,owner_id,description,created_at,updated_at,pushed_at,size,star_cnt,watchers_cnt,forks_cnt,open_issue_cnt,language,allow_forking,license,archived,has_wiki,topics,orgnization_id
            )
            values(
                %s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,
                %s,%s
            ) as new
            on DUPLICATE KEY UPDATE
            description = IF(repository.updated_at < new.updated_at, new.description, repository.description),
            pushed_at= IF(repository.updated_at < new.updated_at, new.pushed_at, repository.pushed_at),
            star_cnt= IF(repository.updated_at < new.updated_at, new.star_cnt, repository.star_cnt),
            watchers_cnt= IF(repository.updated_at < new.updated_at, new.watchers_cnt, repository.watchers_cnt),
            forks_cnt= IF(repository.updated_at < new.updated_at, new.forks_cnt, repository.forks_cnt),
            open_issue_cnt= IF(repository.updated_at < new.updated_at, new.open_issue_cnt, repository.open_issue_cnt),
            updated_at= IF(repository.updated_at < new.updated_at, new.updated_at, repository.updated_at);
        """,
            "followers_data": """
            insert into follow(
                follower_id,following_id
            )
            values(
                %s,%s
            )
        """,
            "following_data": """
            insert into follow(
                follower_id,following_id
            )
            values(
                %s,%s
            )
        """
            
        }


        UTC_FORMAT = "%Y-%m-%dT%H:%M:%S%fZ"

        insert_value_list = []
        for item in insert_list:
            insert_json = item['data']
            if self.key == "user_data":
                insert_value_list.append((
                    insert_json['login'],
                    insert_json['id'],
                    insert_json['node_id'],
                    insert_json['type'],
                    insert_json['site_admin'],
                    insert_json['name'] ,
                    insert_json['company'],
                    insert_json['blog'],
                    insert_json['location'],
                    insert_json['email'],
                    insert_json['hireable'],
                    insert_json['bio'],
                    insert_json['twitter_username'],
                    insert_json['public_repos'],
                    insert_json['public_gists'],
                    insert_json['followers'],
                    insert_json['following'],
                    datetime.strptime(insert_json['created_at'], UTC_FORMAT),
                    datetime.strptime(insert_json['updated_at'], UTC_FORMAT),
                ))
            elif self.key == "repo_data":
                org = insert_json['organization'] if 'organization' in insert_json else None
                lic = insert_json['license'] if 'license' in insert_json else None
                insert_value_list.append((
                    insert_json['id'],
                    insert_json['node_id'],
                    insert_json['name'],
                    insert_json['full_name'],
                    insert_json['private'],
                    insert_json['owner']['id'],
                    insert_json['description'],
                    datetime.strptime(insert_json['created_at'], UTC_FORMAT),
                    datetime.strptime(insert_json['updated_at'], UTC_FORMAT),
                    datetime.strptime(insert_json['pushed_at'], UTC_FORMAT),
                    insert_json['size'],
                    insert_json['watchers_count'],
                    insert_json['subscribers_count'],
                    insert_json['forks_count'],
                    insert_json['open_issues_count'],
                    insert_json['language'],
                    insert_json['allow_forking'],
                    lic['key'] if lic != None and'key' in lic else None,
                    insert_json['archived'],
                    insert_json['has_wiki'],
                    str(insert_json['topics']),
                    org['id'] if org != None and 'id' in org else None,
                ))
            elif self.key == "followers_data":
                for json_item in insert_json:
                    insert_value_list.append((
                        json_item['id'],
                        item['id']
                    ))
                # print(insert_value_list)
            elif self.key == "following_data":
                for json_item in insert_json:
                    insert_value_list.append((
                        item['id'],
                        json_item['id'],
                    ))

        cursor = self.conn.cursor()
        try:
            # self.logger.info(insert_value_list)
            cursor.executemany(insert_sql_list[self.key], insert_value_list)
            self.conn.commit()
        except Exception as e:
            self.logger.error(e)
            self.conn.rollback()
        finally:
            cursor.close()



    def parseSpider(self):
        global jsonQueue
        global urlQueue

        f = open("./temp_data/json_gz/subjson/"+self.json_file + "-"+ self.key +".json", 'w', encoding='utf-8')
        gen = self.generate_json()

        while True:
            if urlQueue.empty():
                self.logger.info("thread parse: " + str(self.thread_id) + " generate url")
                for i in range(MAX_QUEUE_SIZE):
                    if urlQueue.full():
                        break
                    # 
                    try:
                        cjson = next(gen, None)
                    except Exception as e:
                        break

                    name = cjson[self.params[0]][self.params[1]]

                    # exclude any login including `[bot]`
                    if '[bot]' in name:
                        continue

                    id = cjson[self.params[0]][self.params[3]]
                    curl = self.params[2].format(name)
                    # self.logger.info("thread parse: " + str(self.thread_id) + " generate url " + curl)

                    urlQueue.put({'id':id, 'url':curl})
            if not jsonQueue.empty():
                # self.logger.info("thread parse: " + str(self.thread_id) + " store json")

                insert_list = []

                for i in range(MAX_QUEUE_SIZE):
                    if jsonQueue.empty():
                        break
                    item = jsonQueue.get()
                    cjson = item['data']
                    # store data for insert into db
                    insert_list.append(item)
                    # write to local file
                    f.write(json.dumps(cjson) + ',\n')

                # For this for-loop, after write to file, insert into db
                self.insertDB(insert_list)

            if urlQueue.empty() and jsonQueue.empty():
                self.logger.info("thread parse: " + str(self.thread_id) + " wait")
                time.sleep(10)
                if urlQueue.empty() and jsonQueue.empty():
                    self.logger.info("thread parse: " + str(self.thread_id) + " exit")
                    break

        f.close()        
            


def multiThreadQuery(json_list):
    global urlQueue
    global jsonQueue

    # create paths
    if not os.path.exists("./temp_data/json_gz/subjson"):
        os.makedirs("./temp_data/json_gz/subjson")


    crawl_threads = []
    
    query_term = {
        "user_data": ['actor', 'login','https://api.github.com/users/{}','id'],
        "followers_data": ['actor', 'login','https://api.github.com/users/{}/followers','id'],
        "following_data": ['actor', 'login','https://api.github.com/users/{}/following','id'],
        "repo_data": ['repo','name','https://api.github.com/repos/{}','id']
    }

    for i in range(5):
        crawl_thread = CrawlThread(i)
        crawl_thread.start()
        crawl_threads.append(crawl_thread)

    for json_file in json_list:
        for key in query_term.keys():
            parse_thread = ParseThread(i, json_file, key, query_term[key])
            parse_thread.start()
            parse_thread.join()
    
    for crawl_thread in crawl_threads:
        crawl_thread.join()

    print("all done")


if __name__ == '__main__':
    # tranverse all json file in the folder
    json_list = []
    for root, dirs, files in os.walk("./temp_data/json_gz"):
        for file in files:
            if os.path.splitext(file)[1] == '.json':
                json_list.append(os.path.join(file))

    print(json_list)
    multiThreadQuery(json_list)
