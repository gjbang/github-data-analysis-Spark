-- set user:root password
USE mysql;
create user 'root'@'%' identified by '123456';
ALTER USER 'root'@'%' IDENTIFIED WITH MYSQL_NATIVE_PASSWORD BY '123456';
ALTER USER 'root'@'localhost' IDENTIFIED WITH MYSQL_NATIVE_PASSWORD BY '123456';

-- set privileges
grant all privileges on *.* to 'root'@'%';
flush PRIVILEGES;

-- create hive metastore
create database metastore;

-- create meta datasource for mysql
create database github;
use github;
-- create table to store basic information of github users
create table if not exists users(
    id int primary key,
    login varchar(255) unique, 
    node_id varchar(255),
    gravatar_id varchar(255),
    type varchar(255),
    site_admin varchar(255),
    name varchar(255),
    company varchar(255),
    blog varchar(255),
    location varchar(255),
    email varchar(255),
    hireable varchar(255),
    bio varchar(255),
    twitter_username varchar(255),
    public_repos int default 0,
    public_gists int default 0,
    followers int default 0,
    following int default 0,
    created_at datetime,
    updated_at datetime,
    etag varchar(255) default null
);

-- create table to store basic information of github repos without any url
create table if not exists repos(
    id int primary key,
    node_id varchar(255),
    name varchar(255),
    full_name varchar(255),
    private boolean default FALSE,
    owner_id int not null,
    description varchar(255),
    fork varchar(255),
    created_at datetime,
    updated_at datetime,
    pushed_at datetime,
    homepage varchar(255),
    size int,
    stargazers_count int,
    watchers_count int,
    language varchar(255),
    has_issues boolean default FALSE,
    has_projects boolean default FALSE,
    has_downloads boolean default FALSE,
    has_wiki boolean default FALSE,
    has_pages boolean default FALSE,
    forks_count int,
    archived boolean default FALSE,
    disabled boolean default FALSE,
    open_issues_count int,
    license varchar(255),
    forks int,
    allow_forking boolean default FALSE,
    open_issues int,
    watchers int,
    organization_id int,
    topics varchar(255),
    default_branch varchar(255),
    etag varchar(255) default null
);