use github;
drop table if exists topLanguage;
create table if not exists topLanguage(
    id int PRIMARY KEY auto_increment,
    language varchar(255),
    activity_cnt int,
    updated_at datetime,
    created_at datetime
)auto_increment=0;
drop table if exists topActiveRepo;
create table if not exists topActiveRepo(
    repo_id int PRIMARY KEY,
    act_id int,
    language varchar(255),
    full_name VARCHAR(255),
    name VARCHAR(255),
    activity_cnt int,
    updated_at datetime,
    created_at datetime
);