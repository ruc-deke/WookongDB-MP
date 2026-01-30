preload 5
drop table cache_complex_5;
create table cache_complex_5 (id int, val int);
insert into cache_complex_5 values (1, 0);
insert into cache_complex_5 values (2, 0);

txn1 5 0
t1a update cache_complex_5 set val = val + 1 where id = 1;
t1b update cache_complex_5 set val = val + 1 where id = 1;
t1c update cache_complex_5 set val = val + 1 where id = 1;
t1d update cache_complex_5 set val = val + 1 where id = 1;
t1e update cache_complex_5 set val = val + 1 where id = 1;

txn2 5 1
t2a update cache_complex_5 set val = val + 1 where id = 1;
t2b update cache_complex_5 set val = val + 1 where id = 1;
t2c update cache_complex_5 set val = val + 1 where id = 1;
t2d update cache_complex_5 set val = val + 1 where id = 1;
t2e update cache_complex_5 set val = val + 1 where id = 1;

txn3 5 2
t3a update cache_complex_5 set val = val + 1 where id = 1;
t3b update cache_complex_5 set val = val + 1 where id = 1;
t3c update cache_complex_5 set val = val + 1 where id = 1;
t3d update cache_complex_5 set val = val + 1 where id = 1;
t3e update cache_complex_5 set val = val + 1 where id = 1;

txn4 1 0
t4a select * from cache_complex_5 where id = 1;

permutation 16
t1a
t2a
t3a
t1b
t2b
t3b
t1c
t2c
t3c
t1d
t2d
t3d
t1e
t2e
t3e
t4a
