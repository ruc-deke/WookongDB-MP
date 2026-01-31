preload 5
drop table cache_complex_2;
create table cache_complex_2 (k int, v int);
insert into cache_complex_2 values (1, 10);
insert into cache_complex_2 values (2, 20);
insert into cache_complex_2 values (3, 30);

txn1 3 0
t1a update cache_complex_2 set v = 11 where k = 1;
t1b select * from cache_complex_2 where k = 1;
t1c update cache_complex_2 set v = 14 where k = 1;

txn2 3 1
t2a select * from cache_complex_2 where k = 1;
t2b update cache_complex_2 set v = 12 where k = 1;
t2c select * from cache_complex_2 where k = 1;

txn3 3 1
t3a select * from cache_complex_2 where k = 1;
t3b update cache_complex_2 set v = 13 where k = 1;
t3c select * from cache_complex_2 where k = 1;

txn4 2 0
t4a select * from cache_complex_2 where k = 1;
t4b select * from cache_complex_2 where k = 1;

permutation 11
t1a
t2a
t2b
t3a
t3b
t1b
t1c
t2c
t3c
t4a
t4b
