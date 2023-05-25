use github;
-- delete above table;
drop table if exists eventRatio;
drop table if exists eventAllRatio;
drop table if exists eventCount;
drop table if exists eventAllCount;
drop table if exists usersCount;
drop table if exists reposCount;



use github;
create table if not exists eventRatio(
    id int primary key auto_increment,
    createEventRaio FLOAT,
    deleteEventRaio FLOAT,
    issuesEventRaio FLOAT,
    issueCommentEventRaio FLOAT,
    pullRequestEventRaio FLOAT,
    pushEventRaio FLOAT,
    releaseEventRaio FLOAT,
    watchEventRaio FLOAT,
    updated_at datetime,
    created_at datetime
)auto_increment=0;

use github;
create table if not exists eventAllRatio(
    id int primary key auto_increment,
    createEventRaio FLOAT,
    deleteEventRaio FLOAT,
    issuesEventRaio FLOAT,
    issueCommentEventRaio FLOAT,
    pullRequestEventRaio FLOAT,
    pushEventRaio FLOAT,
    releaseEventRaio FLOAT,
    watchEventRaio FLOAT,
    updated_at datetime,
    created_at datetime
)auto_increment=0;


use github;
create table if not exists eventCount(
    id int primary key auto_increment,
    createEventCount int,
    deleteEventCount int,
    issuesEventCount int,
    issueCommentEventCount int,
    pullEventCount int,
    pushEventCount int,
    releaseEventCount int,
    watchEventCount int,
    updated_at datetime,
    created_at datetime
)auto_increment=0;


use github;
create table if not exists eventAllCount(
    id int primary key auto_increment,
    createEventCount int,
    deleteEventCount int,
    issuesEventCount int,
    issueCommentEventCount int,
    pullEventCount int,
    pushEventCount int,
    releaseEventCount int,
    watchEventCount int,
    updated_at datetime,
    created_at datetime
)auto_increment=0;


use github;
create table if not exists usersCount(
    id int primary key auto_increment,
    usersCount int,
    updated_at datetime
);

use github;
create table if not exists reposCount(
    id int primary key auto_increment,
    reposCount int,
    updated_at datetime
)auto_increment=0;



