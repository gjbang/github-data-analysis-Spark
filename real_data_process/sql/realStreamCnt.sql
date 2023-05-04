use github;
-- drop table if exists issueBasic;
create table if not exists realStreamEventCnt(
    id int PRIMARY KEY AUTO_INCREMENT,
    type varchar(255),
    count int,
    updated_at timestamp
);
