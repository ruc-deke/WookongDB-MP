preload 5
drop table cache_complex_3;
create table cache_complex_3 (acc int, bal float);
insert into cache_complex_3 values (1, 1000.0);
insert into cache_complex_3 values (2, 2000.0);
insert into cache_complex_3 values (3, 3000.0);

txn1 4 0
t1a update cache_complex_3 set bal = 1010.0 where acc = 1;
t1b select * from cache_complex_3 where acc = 1;
t1c update cache_complex_3 set bal = 1020.0 where acc = 1;
t1d select * from cache_complex_3 where acc = 1;

txn2 4 1
t2a select * from cache_complex_3 where acc = 1;
t2b update cache_complex_3 set bal = 2010.0 where acc = 1;
t2c select * from cache_complex_3 where acc = 1;
t2d update cache_complex_3 set bal = 2020.0 where acc = 1;

txn3 4 1
t3a update cache_complex_3 set bal = 3010.0 where acc = 1;
t3b select * from cache_complex_3 where acc = 1;
t3c update cache_complex_3 set bal = 3020.0 where acc = 1;
t3d select * from cache_complex_3 where acc = 1;

txn4 1 0
t4a select * from cache_complex_3 where acc = 1;

permutation 13
t1a
t2a
t2b
t3a
t3b
t1b
t1c
t2c
t2d
t3c
t3d
t1d
t4a
