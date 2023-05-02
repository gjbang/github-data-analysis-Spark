from functools import partial
from itertools import repeat
from apscheduler.schedulers.background import BackgroundScheduler
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests

import os
import gzip
import time
import glob
import json
import datetime
import shutil

import pymysql

import logging
from log import Logger

from construct_sql import cons_user_sql, cons_repo_sql,get_repo_insert_sql, get_user_insert_sql


# generate json object from gzip file
def generate_gzip_json(gzip_path):
    with gzip.open(gzip_path, 'rb') as f:
        for line in f:
            yield json.loads(line)

# write archieve data to json file in flume directory
# delete all urls property in json and serialize payload to string
def import_json_to_flume(cjson, opath):
    # modify json slightly to decrease the size of json
    stack = [cjson]
    while stack:
        obj = stack.pop()
        if isinstance(obj, dict):
            for key in list(obj.keys()):
                # delete all attributes which is "url" in json
                if isinstance(obj[key], str) and ('http://' in obj[key] or 'https://' in obj[key]):
                    del obj[key]
                # delete empty attributes : [], {}
                elif isinstance(obj[key], list) and len(obj[key]) == 0:
                    del obj[key]
                elif isinstance(obj[key], dict) and len(obj[key]) == 0:
                    del obj[key]
                else:
                    stack.append(obj[key])
        elif isinstance(obj, list):
            stack.extend(obj)

    stack = [cjson]
    while stack:
        obj = stack.pop()
        if isinstance(obj, dict):
            for key in list(obj.keys()):
                # delete all attributes which is "url" in json
                if key == 'payload':
                    obj[key] = json.dumps(obj[key])
                else:
                    stack.append(obj[key])
        elif isinstance(obj, list):
            stack.extend(obj)


    # logger.debug(" == start to append json to flume directory == ")
    # logger.debug(cjson)

    # append json to flume directory
    with open(opath, "a") as f:
        f.write(json.dumps(cjson) + "\n")


if __name__ == '__main__':
    g_path= "/tmp/data/2023-05-01-22.json.gz"

    for json_obj in generate_gzip_json(g_path):
        import_json_to_flume(json_obj, "/opt/module/flume/data/2023-05-01-22.json")

    