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