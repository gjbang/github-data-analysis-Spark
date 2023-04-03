import pymysql
from pymysql.converters import escape_string
from datetime import datetime
import requests
import json


# ------ test database connection - -----
conn = pymysql.connect(
    host='localhost',
    port=3306,
    user='root',
    password='heikediguo',
    database='crawl',
    charset='utf8',
)

cursor = conn.cursor()
cursor.execute("select VERSION()")
data = cursor.fetchone()
print("Database version : %s " % data)

insert_sql = """
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
    """

isner_value_array = [
    ('samuelcolvin', '4039449', 'MDQ6VXNlcjQwMzk0NDk=', 'User', '0', 'Samuel Colvin', '@pydantic', 'http://scolvin.com', 'London, United Kingdom', 's@muelcolvin.com',None, 'Python & Rust; Developer and Founder of Pydantic.', 'samuel_colvin', '236', '106', '3420', '41', '2013-04-02 17:24:04', '2023-03-30 15:38:05')
]

repo_insert_sql = """
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
    """

print(len(isner_value_array[0]))

cursor = conn.cursor()
cursor.executemany(insert_sql, isner_value_array)
conn.commit()

# {
#     "login": "octocat",
#     "id": 583231,
#     "node_id": "MDQ6VXNlcjU4MzIzMQ==",
#     "avatar_url": "https://avatars.githubusercontent.com/u/583231?v=4",
#     "gravatar_id": "",
#     "url": "https://api.github.com/users/octocat",
#     "html_url": "https://github.com/octocat",
#     "followers_url": "https://api.github.com/users/octocat/followers",
#     "following_url": "https://api.github.com/users/octocat/following{/other_user}",
#     "gists_url": "https://api.github.com/users/octocat/gists{/gist_id}",
#     "starred_url": "https://api.github.com/users/octocat/starred{/owner}{/repo}",
#     "subscriptions_url": "https://api.github.com/users/octocat/subscriptions",
#     "organizations_url": "https://api.github.com/users/octocat/orgs",
#     "repos_url": "https://api.github.com/users/octocat/repos",
#     "events_url": "https://api.github.com/users/octocat/events{/privacy}",
#     "received_events_url": "https://api.github.com/users/octocat/received_events",
#     "type": "User",
#     "site_admin": false,
#     "name": "The Octocat",
#     "company": "@github",
#     "blog": "https://github.blog",
#     "location": "San Francisco",
#     "email": "octocat@github.com",
#     "hireable": null,
#     "bio": null,
#     "twitter_username": null,
#     "public_repos": 8,
#     "public_gists": 8,
#     "followers": 8774,
#     "following": 9,
#     "created_at": "2011-01-25T18:44:36Z",
#     "updated_at": "2023-03-22T11:21:35Z"
# }

# UTC_FORMAT = "%Y-%m-%dT%H:%M:%S%fZ"

# cursor = conn.cursor()
# cursor.execute(insert_sql.format(
#     login=escape_string('octocat'),
#     id=583231,
#     node_id=escape_string('MDQ6VXNlcjU4MzIzMQ=='),
#     type=escape_string('User'),
#     site_admin=0,
#     name=escape_string('The Octocat'),
#     company=escape_string('@github'),
#     blog=escape_string('https://github.blog'),
#     location=escape_string('San Francisco'),
#     email=escape_string('octocat@github.com'),
#     hireable=0,
#     bio=None,
#     twitter_username=None,
#     public_repos=8,
#     public_gists=8,
#     followers=8774,
#     following=9,
#     created_at=datetime.strptime("2011-01-25T18:44:36Z", UTC_FORMAT),
#     updated_at=datetime.strptime("2023-03-22T11:21:35Z",UTC_FORMAT),
# ))
# conn.commit()


# ------ test json entry insert ------
# response = requests.get('https://api.github.com/users/gjbang/following')
# # insert one record at beginning of json response
# # response = response.json().insert(0, {id: 1})
# # response.json()[0]['id'] = 1
# f = response.json()
# f.insert(0, {'id': 1})
# print(f)
# print(f.append({'id': 1}))


# ------ read activity json file ------
# def read_json_file():
#     with open("./temp_data/json_gz/2023-04-01-23.json",'r') as f:
#         for line in f:
#             yield json.loads(line)

# gen = read_json_file()
# print(next(gen))


# ------ test judge json empty ------
# response = requests.get('https://api.github.com/users/RangerYYZ/followers')
# print(response.json())
# print(response.json() == [])

# ------ test json list for loop ------
# response = requests.get('https://api.github.com/users/gjbang/following')
# for i in response.json():
#     print(i)
