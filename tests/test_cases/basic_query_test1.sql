-- 题目五：查询执行  测试点1: 尝试建表
create table student (id int, name char(32), major char(32) , primary key(id));
create table grade (course char(32), student_id int, score float , primary key(student_id));
show tables;
drop table student;
show tables;
create table grade (id int);
drop table t;
show tables;

drop table grade;