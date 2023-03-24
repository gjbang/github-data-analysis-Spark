import os
import shutil
import datetime
import logging

from tqdm import tqdm

import gzip
import json
import requests

# basic config
# home_path = '/home/temp_data/'
home_path = "./temp_data/"
show_log = True

# temp donwload path
temp_json_path = home_path + 'json_gz/'
json_file_list = []

# define which time interval to download
now_time = datetime.datetime.now()
total_hours = 1

# set jointly query information:
jointly_params = {
    'joint_user': [True, 'users'],
    'joint_repo': [True, 'repos'],
    'joint_following': [True, 'following'],
    'joint_followers': [True, 'followers'],
}

# parse gzip file and return json data line by line
def parse_gzip_json(path):
    g = gzip.open(path, 'r')
    # convert to utf-8 format
    for l in g:
        yield json.loads(l.decode('utf-8'))

def load_data(chunk_size=8192, timeout=30, show_progress=True, debug=True):
    logging.info("start download data from github archive")
    
    finished_hours = 0
    time_interval = 1
    while finished_hours < total_hours:
        target_time = (now_time - datetime.timedelta(hours=time_interval)).strftime('%Y-%m-%d-%H')
        logging.info("current timestamp " + target_time)

        target_url = 'https://data.gharchive.org/' + target_time + '.json.gz'  

        # download data
        try:
            logging.info("download data from " + target_url)
            resp = requests.get(target_url, stream=True, timeout=timeout)
            resp.raise_for_status()
            total_len = int(resp.headers.get('content-length', 0))

            if show_progress:
                with open(temp_json_path + target_time + '.json.gz', 'wb') as f,tqdm(
                    total=total_len, 
                    unit='iB', 
                    unit_scale=True, 
                    unit_divisor=1024, 
                    desc=target_url, 
                    ascii=True
                ) as pbar:
                    for chunk in resp.iter_content(chunk_size=chunk_size):
                        cur_size = f.write(chunk)
                        pbar.update(cur_size)
            else:
                with open(temp_json_path + target_time + '.json.gz', 'wb') as f:
                    for chunk in resp.iter_content(chunk_size=chunk_size):
                        f.write(chunk)

        except Exception as e:
            logging.error("download data failed, error message: " + str(e))
            time_interval += 1
            continue
        else:
            logging.info("read json data" )
            if debug:
                cnt = 0
                for line in parse_gzip_json(temp_json_path + target_time + '.json.gz'):
                    
                    # if need user's information, call api to get detaild
                    if jointly_params['joint_user'][0]:
                        if 'actor' in line:
                            login = line['actor']['login']
                            user_url = 'https://api.github.com/users/' + login
                            user_resp = requests.get(user_url)
                            user_data = user_resp.json()
                            line['actor']['user_data'] = user_data
                        
                        logging.info("add user information")
                    
                    # if need repo's information, call api to get detaild
                    if jointly_params['joint_repo'][0]:
                        if 'repo' in line:
                            repo_url = line['repo']['url']
                            repo_resp = requests.get(repo_url)
                            repo_data = repo_resp.json()
                            line['repo']['repo_data'] = repo_data

                        logging.info("add repo information")

                    # if need following's information, call api to get detaild
                    if jointly_params['joint_following'][0]:
                        if 'actor' in line:
                            login = line['actor']['login']
                            following_url = 'https://api.github.com/users/' + login + '/following'
                            following_resp = requests.get(following_url)
                            following_data = following_resp.json()
                            line['actor']['following_data'] = following_data

                        logging.info("add following information")

                    # if need followers's information, call api to get detaild
                    if jointly_params['joint_followers'][0]:
                        if 'actor' in line:
                            login = line['actor']['login']
                            followers_url = 'https://api.github.com/users/' + login + '/followers'
                            followers_resp = requests.get(followers_url)
                            followers_data = followers_resp.json()
                            line['actor']['followers_data'] = followers_data
                        
                        logging.info("add followers information")

                    print("line [{}]".format(cnt))
                    cnt += 1
                    print(line)
                    if cnt > 10:
                        break
            
            json_file_list.append(temp_json_path + target_time + '.json.gz')

            finished_hours += 1
            time_interval += 1
    




if __name__ == '__main__':
    # create dir
    if not os.path.exists(temp_json_path):
        os.makedirs(temp_json_path)
        logging.info("create dir: " + temp_json_path)
    # clear cache
    else:
        shutil.rmtree(temp_json_path)
        os.makedirs(temp_json_path)
        logging.info("clear dir: " + temp_json_path)

    # config logger
    if show_log:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', force=True)
    else:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', filename= home_path + "download.log", filemode="a", force=True)

    load_data(show_progress=True)
    
