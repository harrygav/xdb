-- Test query 1

select count(*)
  from lineitem as l,
       orders as o
where 1 = 1
  and l.l_orderkey = o.o_orderkey
group by true