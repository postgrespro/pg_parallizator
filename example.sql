load 'pg_parallizator';
drop table if exists huge;
\timing
create table huge (pk integer primary key, k1 real, k2 real, k3 real, k4 real, k5 real, k6 real, k7 real, k8 real);
insert into huge values (generate_series(1,10000000), random(), random(), random(), random(), random(), random(), random(), random());
create index on huge(k1);
create index on huge(k2);
create index on huge(k3);
create index on huge(k4);
create index on huge(k5);
create index on huge(k6);
create index on huge(k7);
create index on huge(k8);
