use github;
-- drop table if exists userBasic;
create table if not exists userBasic(
    id int primary key auto_increment,
    email_ratio float,
    company_ratio float,
    location_ratio float,
    bio_ratio float,
    blog_ratio float,
    hireable_ratio float,
    twitter_username_ratio float,
    followers_ratio float,
    following_ratio float,
    followers_following_ratio float,
    public_repos_ratio float,
    public_gists_ratio float,
    updated_at datetime,
    created_at datetime
)auto_increment=0;

use github;
create table if not exists top20_followers(
    id int primary key,
    login VARCHAR(255),
    followers int,
    updated_at datetime
);

use github;
create table if not exists top20_following(
    id int primary key,
    login VARCHAR(255),
    following int,
    updated_at datetime
);