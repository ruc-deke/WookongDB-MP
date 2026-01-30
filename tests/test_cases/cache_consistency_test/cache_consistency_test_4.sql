preload 8
drop table cache_complex_4;
create table cache_complex_4 (id int, val int, tag char(8));
insert into cache_complex_4 values (1, 100, 'A');
insert into cache_complex_4 values (2, 200, 'B');
insert into cache_complex_4 values (3, 300, 'C');
insert into cache_complex_4 values (4, 400, 'D');

txn1 4 0
t1a update cache_complex_4 set val = 101 where id = 1;
t1b select * from cache_complex_4 where id = 2;
t1c update cache_complex_4 set val = 103 where id = 3;
t1d select * from cache_complex_4 where id = 1;

txn2 4 1
t2a update cache_complex_4 set val = 202 where id = 2;
t2b select * from cache_complex_4 where id = 3;
t2c update cache_complex_4 set val = 201 where id = 1;
t2d select * from cache_complex_4 where id = 2;

txn3 4 2
t3a update cache_complex_4 set val = 303 where id = 3;
t3b select * from cache_complex_4 where id = 1;
t3c update cache_complex_4 set val = 302 where id = 2;
t3d select * from cache_complex_4 where id = 3;

txn4 3 0
t4a select * from cache_complex_4 where id = 1;
t4b select * from cache_complex_4 where id = 2;
t4c select * from cache_complex_4 where id = 3;

permutation 15
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
t4a
t4b
t4c
