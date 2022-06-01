-- TPC-H Query 5

select n.n_name,
       sum( l.l_extendedprice * (1 - l.l_discount )) as revenue
  from region as r,
     nation as n,
     customer as c,
     orders as o,
     lineitem as l,
     supplier as s
where 1 = 1
  and c.c_custkey = o.o_custkey
  and l.l_orderkey = o.o_orderkey
  and l.l_suppkey = s.s_suppkey
  and c.c_nationkey = s.s_nationkey
  and s.s_nationkey = n.n_nationkey
  and c.c_nationkey = n.n_nationkey
  and n.n_regionkey = r.r_regionkey
  and r.r_name = 'ASIA'
  and o.o_orderdate >= date '1994-01-01'
  and o.o_orderdate < date '1995-01-01'
group by n_name
order by revenue desc