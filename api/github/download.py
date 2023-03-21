import os
import gzip
import datetime
import json
import shutil
import requests

# temp donwload path
temp_json_path = './temp_json/'
json_file_list = []

# define which time interval to download
now_time = datetime.datetime.now()
total_hours = 1


# parse gzip file and return json data
def parse_gzip_json(path):
    g = gzip.open(path, 'r')
    # convert to utf-8 format
    for l in g:
        yield json.loads(l.decode('utf-8'))


def load_data():
    # create dir
    if not os.path.exists(temp_json_path):
        os.makedirs(temp_json_path)
        print("[INFO] create dir: " + temp_json_path)
    # clear cache
    else:
        shutil.rmtree(temp_json_path)
        os.makedirs(temp_json_path)
        print("[INFO] clear dir: " + temp_json_path)


    print("[INFO] start download data from github archive")
    finished_hours = 0
    time_interval = 1
    while finished_hours < total_hours:
        target_time = (now_time - datetime.timedelta(hours=time_interval)).strftime('%Y-%m-%d-%H')
        print("[INFO] current timestamp " + target_time)

        target_url = 'https://data.gharchive.org/' + target_time + '.json.gz'
        
        # download data
        try:
            print("[INFO] download data from " + target_url)
            with requests.get(target_url, stream=True) as r:
                r.raise_for_status()
                with open(temp_json_path + target_time + '.json.gz', 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
        except Exception as e:
            print("[ERROR] download data failed, error message: " + str(e))
            time_interval += 1
            continue
        else:
            # read json data
            print("[INFO] read json data" )
            cnt = 0
            for line in parse_gzip_json(temp_json_path + target_time + '.json.gz'):
                print("line " + str(cnt) + ": ")
                print(line)
                cnt += 1
                if cnt > 10:
                    break
            
            json_file_list.append(temp_json_path +  target_time + ".json.gz")

            time_interval += 1
            finished_hours += 1
        
    print("[INFO] download data finished and the json file list is: ")
    print(json_file_list)


if __name__ == '__main__':
    load_data()
