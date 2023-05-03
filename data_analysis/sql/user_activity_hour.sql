use github;
drop table if exists topActiveUser;
create table if not exists topActiveUser(
    id int PRIMARY KEY auto_increment,
    login VARCHAR(255),
    company VARCHAR(255),
    location VARCHAR(255),
    followers INT,
    following INT,
    public_repos INT,
    activity_cnt INT,
    updated_at DATETIME,
    created_at DATETIME
);

drop table if exists topActiveRegion;
create table if not exists topActiveRegion(
    id int PRIMARY KEY auto_increment,
    location VARCHAR(255),
    activity_cnt INT,
    updated_at datetime
);

drop table if exists topActiveCompany;
create table if not exists topActiveCompany(
    id int PRIMARY KEY auto_increment,
    company VARCHAR(255),
    activity_cnt INT,
    updated_at datetime
);