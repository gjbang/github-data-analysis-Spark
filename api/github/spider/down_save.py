import os
import time
import shutil
import datetime
import logging
from api.github.spider.log import Logger

from tqdm import tqdm

import gzip
import json
import requests


# generate json object from gzip file
def generate_gzip_json(gzip_path):
    with gzip.open(gzip_path, 'rb') as f:
        for line in f:
            yield json.loads(line)

# generate json object from json file
def generate_json(json_path):
    with open(json_path, 'r', encoding='utf-8') as f:
        for line in f:
            yield json.loads(line)

# decompress gzip file and delete gzip file
def decompress_gzip(gzip_path, json_path):
    # decompress gzip file
    with gzip.open(gzip_path, 'rb') as f_in:
        with open(json_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    # delete gzip file
    os.remove(gzip_path)


# download gzip file according to gzip list
def download_archive(path,gzip_list,logger,chunk=8192,timeout=30):
    for gzip in gzip_list:
        url = 'https://data.gharchive.org/' + gzip + '.json.gz'
        logger.info("download data from " + url)

        try:
            resp = requests.get(url, stream=True, timeout=timeout)
            resp.raise_for_status()
            total_len = int(resp.headers.get('content-length', 0))
            with open(path + gzip + '.json.gz', 'wb') as f:
                for chunk in resp.iter_content(chunk_size=chunk):
                    f.write(chunk)

        except Exception as e:
            logger.error("download data failed, error message: " + str(e))
            continue
        else:
            logger.info(url + " download success")
            decompress_gzip(path + gzip + '.json.gz', path + gzip + '.json')

# query meta data from github api, according to json list
def query_meta_data(path,logger,json_list):

    user_prefix = 'https://api.github.com/users/'

    query_term = {
        "user_data": [['actor', 'login'],'https://api.github.com/users/{}'],
        "followers_data": [['actor', 'login'],'https://api.github.com/users/{}/followers'],
        "following_data": [['actor', 'login'],'https://api.github.com/users/{}/following'],
        "repo_data": [['repo','url'],'{}']
    }

    token_list = []
    # read header auth token from file
    with open('./api/github/auth_token.txt', 'r') as f:
        token_list.append(f.readline().strip())

    current_token_id = 0
    header = {'Authorization': 'Bearer ' + token_list[0]}

    # tranverse json file -- each hour
    for json_f in json_list:
        logger.info("start query " + json_f)
        # check each query term
        for key, param in query_term.items():
            logger.info("start query " + key + " from " + json_f)
            # store query result separately
            with open(path + json_f + "-" +key + '.json', 'a') as f:
                for line in generate_json(path + json_f + '.json'):
                    try:
                        if param[0][0] in line:
                            target = line[param[0][0]][param[0][1]]
                            url = param[1].format(target)
                            response = requests.get(url, headers=header)
                            response.raise_for_status()
                            # write single json object to file
                            f.write(json.dumps(response.json()) + '\n')
                            # time.sleep()
                    except Exception as e:
                        print(e)
                        if response.status_code == 403:
                            current_token_id += 1
                            if current_token_id == 3:
                                current_token_id = 0
                                return 
                            header = {'Authorization': 'Bearer ' + token_list[current_token_id]}
                            logger.error("token " + token_list[current_token_id] + " expired, switch to token " + token_list[current_token_id] + "; left " + str(len(token_list) - current_token_id) + " tokens")
                        continue
            logger.info("query " + key + " from " + json + " success")
        logger.info("query " + json + " success")



# fetch data from github archive and pick up meta data
# main control function
def fecth_data(path,now_t,do_query,chunk=8192,timeout=30,hours=1,show_log=True):

    logger = Logger("down_archive.log", logging.DEBUG, __name__).getlog() 

    exist_json,gzip_list=[],[]

    if not os.path.exists(path):
        os.mkdir(path)
        logger.info("create path " + path)
    
    else:
        # tranverse path to find json file
        for root, dirs, files in os.walk(path):
            for file in files:
                if file.endswith(".json"):
                    exist_json.append(file[:-5])
        
        logger.info("find " + str(len(exist_json)) + " existing json files")

    # generate gzip list
    interval,current = 1,1
    while current <= hours:
        target_time = (now_t - datetime.timedelta(hours=interval)).strftime('%Y-%m-%d-%H')
        if target_time not in exist_json:
            target_url = 'https://data.gharchive.org/' + target_time + '.json.gz'
            r = requests.head(target_url)
            if r.status_code == 200:
                gzip_list.append(target_time)
                current += 1
        interval += 1

    # download gzip file
    logger.info(str(len(gzip_list)) + " new json files should be downloaded")
    if len(gzip_list) != 0:
        download_archive(path,gzip_list,logger,chunk,timeout)

        # if do_query:
        #     query_meta_data(path,logger,gzip_list)
        

    else:
        logger.info("no new json files need to be downloaded")





if __name__ == "__main__":
    now_time = datetime.datetime.now()
    path = "./temp_data/json_gz/"
    fecth_data(path,now_time,1)