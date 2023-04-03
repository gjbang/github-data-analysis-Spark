create database crawl;
use crawl;

drop table user;
create table user(
    login VARCHAR(255) UNIQUE,
    id INT PRIMARY KEY,
    node_id VARCHAR(255),
    type VARCHAR(255) DEFAULT NULL,
    site_admin BOOLEAN DEFAULT NULL,
    name VARCHAR(255) DEFAULT NULL,
    company VARCHAR(255) DEFAULT NULL,
    blog VARCHAR(255) DEFAULT NULL,
    location VARCHAR(255) DEFAULT NULL,
    email VARCHAR(255) DEFAULT NULL,
    hireable BOOLEAN DEFAULT NULL,
    bio VARCHAR(255) DEFAULT NULL,
    twitter_username VARCHAR(255),
    public_repos INT DEFAULT 0,
    public_gists INT DEFAULT 0,
    followers INT DEFAULT 0,
    following INT DEFAULT 0,
    created_at DATETIME,
    updated_at DATETIME,
    etag VARCHAR(255) DEFAULT NULL
);
select * from user;

-- er follows ing
drop table follow;
create table follow(
    follower_id INT,
    following_id INT,
    PRIMARY KEY(follower_id, following_id)
    -- FOREIGN KEY(follower_id) REFERENCES user(id),
    -- FOREIGN KEY(following_id) REFERENCES user(id)
);
select * from follow;


drop table repository;
create table repository(
    id INT PRIMARY KEY,
    node_id VARCHAR(255),
    name VARCHAR(255) NOT NULL,
    full_name VARCHAR(255),
    private BOOLEAN DEFAULT FALSE,
    owner_id INT NOT NULL,
    description VARCHAR(255),
    created_at DATETIME,
    updated_at DATETIME,
    pushed_at DATETIME,
    size INT DEFAULT 0,
    star_cnt INT DEFAULT 0,
    watchers_cnt INT DEFAULT 0,
    forks_cnt INT DEFAULT 0,
    open_issue_cnt INT DEFAULT 0,
    language VARCHAR(255),
    allow_forking BOOLEAN DEFAULT TRUE,
    license VARCHAR(255),
    archived BOOLEAN DEFAULT FALSE,
    has_wiki BOOLEAN DEFAULT FALSE,
    topics VARCHAR(255),
    orgnization_id INT,
    etag VARCHAR(255) DEFAULT NULL
);
select * from repository;


-- values() means update with new value
select * from user where id=4039449;

insert into user(                login,id,node_id,type,site_admin,name,company,blog,location,email,hireable,bio,twitter_username,public_repos,public_gists,followers,following,created_at,updated_at
)
values(
    'samuelcolvin', '4039449', 'MDQ6VXNlcjQwMzk0NDk=', 'User', '0', 'Samuel Colvin', '@pydantic', 'http://scolvin.com', 'London, United Kingdom', 's@muelcolvin.com', NULL, 'Python & Rust; Developer and Founder of Pydantic.', 'samuel_colvin', '236', '106', '3420', '41', '2013-04-02 17:24:04', '2023-03-25 15:38:05'
) as newd
on DUPLICATE KEY UPDATE
public_repos= IF(updated_at < newd.updated_at, newd.public_repos, public_repos),
public_gists= IF(updated_at < newd.updated_at, newd.public_gists, public_gists),
followers= IF(updated_at < newd.updated_at, newd.followers, followers),
following= IF(updated_at < newd.updated_at, newd.following, following),
updated_at= IF(updated_at < newd.updated_at, newd.updated_at, updated_at);


-- public_repos= IF(updated_at < VALUES(updated_at), VALUES(public_repos), public_repos),
-- public_gists= IF(updated_at < VALUES(updated_at), VALUES(public_gists), public_gists),
-- followers= IF(updated_at < VALUES(updated_at), VALUES(followers), followers),
-- following= IF(updated_at < VALUES(updated_at), VALUES(following), following),
-- updated_at= IF(updated_at < VALUES(updated_at), VALUES(updated_at), updated_at);

-- insert into user(                login,id,node_id,type,site_admin,name,company,blog,location,email,hireable,bio,twitter_username,public_repos,public_gists,followers,following,created_at,updated_at
-- )
-- values(
--     'samuelcolvin', '4039449', 'MDQ6VXNlcjQwMzk0NDk=', 'User', '0', 'Samuel Colvin', '@pydantic', 'http://scolvin.com', 'London, United Kingdom', 's@muelcolvin.com', NULL, 'Python & Rust; Developer and Founder of Pydantic.', 'samuel_colvin', '236', '106', '3420', '41', '2013-04-02 17:24:04', '2023-03-24 15:38:05'
-- ) as newd
-- on DUPLICATE KEY UPDATE
-- public_repos= IF(updated_at < VALUES(updated_at), VALUES(public_repos), public_repos),
-- public_gists= IF(updated_at < VALUES(updated_at), VALUES(public_gists), public_gists),
-- followers= IF(updated_at < VALUES(updated_at), VALUES(followers), followers),
-- following= IF(updated_at < VALUES(updated_at), VALUES(following), following),
-- updated_at= IF(updated_at < VALUES(updated_at), VALUES(updated_at), updated_at);


insert into user(                login,id,node_id,type,site_admin,name,company,blog,location,email,hireable,bio,twitter_username,public_repos,public_gists,followers,following,created_at,updated_at
)
values(
    %s,%s,%s,%s,%s,
    %s,%s,%s,%s,%s,
    %s,%s,%s,%s,%s,
    %s,%s,%s,%s
)
on DUPLICATE KEY UPDATE
public_repos= IF(updated_at < %s, VALUES(public_repos), public_repos),
public_gists= IF(updated_at < %s, VALUES(public_gists), public_gists),
followers= IF(updated_at < %s, VALUES(followers), followers),
following= IF(updated_at < %s, VALUES(following), following),
updated_at= IF(updated_at < %s, VALUES(updated_at), updated_at);


insert into repository(
    id,node_id,name,full_name,private,owner_id,description,created_at,updated_at,pushed_at,size,star_cnt,watchers_cnt,forks_cnt,open_issue_cnt,language,allow_forking,license,archived,has_wiki,topics,orgnization_id
)
values(
    %s,%s,%s,%s,%s,
    %s,%s,%s,%s,%s,
    %s,%s,%s,%s,%s,
    %s,%s,%s,%s,%s,
    %s,%s
)
on DUPLICATE KEY UPDATE
description = IF(updated_at < %s, VALUES(description), description),
pushed_at= IF(updated_at < %s, VALUES(pushed_at), pushed_at),
star_cnt= IF(updated_at < %s, VALUES(star_cnt), star_cnt),
watchers_cnt= IF(updated_at < %s, VALUES(watchers_cnt), watchers_cnt),
forks_cnt= IF(updated_at < %s, VALUES(forks_cnt), forks_cnt),
open_issue_cnt= IF(updated_at < %s, VALUES(open_issue_cnt), open_issue_cnt),
updated_at= IF(updated_at < %s, VALUES(updated_at), updated_at);
