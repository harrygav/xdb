-- test query 3
select count(*)
  from lineitem as l,
       partsupp as ps
where 1 = 1
    and l.l_partkey = ps.ps_partkey
    and l.l_suppkey = ps.ps_suppkey
group by true