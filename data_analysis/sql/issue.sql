use github;
-- drop table if exists issueBasic;
create table if not exists issueBasic(
    id int primary key default 0 auto_increment,
    issueOpenCount int,
    issueClosedCount int,
    issueReopenedCount int,
    issueCompletedCount int,
    issueOtherClosedCount int,
    created_at datetime,
    updated_at datetime
);

use github;
create table if not exists issueInterval(
    id int primary key auto_increment,
    I1 int,
    I7 int,
    I30 int,
    I90 int,
    I180 int,
    I365 int,
    I730 int,
    I1095 int,
    I1460 int,
    others int,
    created_at datetime,
    updated_at datetime
);

use github;
create table if not exists issueComments(
    id int primary key auto_increment,
    C0 int,
    C10 int,
    C100 int,
    C1000 int,
    C10000 int,
    C100000 int,
    C1000000 int,
    others int,
    created_at datetime,
    updated_at datetime
);

use github;
create table if not exists issueNumber(
    id int primary key auto_increment,
    N1 int,
    N10 int,
    N100 int,
    N1000 int,
    N10000 int,
    N100000 int,
    N1000000 int,
    others int,
    created_at datetime,
    updated_at datetime
);

