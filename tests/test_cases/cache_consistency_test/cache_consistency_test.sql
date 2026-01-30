preload 5
drop table cache_test;
create table cache_test (id int, name char(8), score float);
insert into cache_test values (1, 'node0', 10.0);
insert into cache_test values (2, 'node1', 20.0);
insert into cache_test values (3, 'node2', 30.0);

txn1 3 0
t1a select * from cache_test where id = 1;
t1b select * from cache_test where id = 1;
t1c update cache_test set score = 11.0 where id = 1;

txn2 2 1
t2a update cache_test set score = 98.2 where id = 1;
t2b select * from cache_test where id = 1;

permutation 5
t1a
t2a
t1b
t1c
t2b
