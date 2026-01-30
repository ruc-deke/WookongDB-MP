preload 7
drop table cache_complex_1;
create table cache_complex_1 (id int, val int, info char(10));
insert into cache_complex_1 values (1, 100, 'init');
insert into cache_complex_1 values (2, 200, 'init');
insert into cache_complex_1 values (3, 300, 'init');
insert into cache_complex_1 values (4, 400, 'init');
insert into cache_complex_1 values (5, 500, 'init');

txn1 4 0
t1a select * from cache_complex_1;
t1b update cache_complex_1 set val = 101 where id = 1;
t1c select * from cache_complex_1 where id = 1;
t1d select * from cache_complex_1;

txn2 4 1
t2a select * from cache_complex_1;
t2b select * from cache_complex_1 where id = 1;
t2c update cache_complex_1 set val = 102 where id = 1;
t2d select * from cache_complex_1;

txn3 3 1
t3a select * from cache_complex_1;
t3b select * from cache_complex_1 where id = 1;
t3c select * from cache_complex_1;

txn4 4 0
t4a select * from cache_complex_1;
t4b update cache_complex_1 set val = 404 where id = 4;
t4c select * from cache_complex_1;
t4d select * from cache_complex_1 where id = 4;

permutation 15
t1a
t1b
t2a
t2b
t1c
t1d
t2c
t2d
t3a
t3b
t3c
t4a
t4b
t4c
t4d
