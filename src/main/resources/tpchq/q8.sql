-- TPC-H Query 8

select EXTRACT(YEAR FROM o.o_orderdate )            as o_year,
       SUM(CASE WHEN n2.n2_name = 'BRAZIL' THEN l.l_extendedprice * (1 - l.l_discount ) ELSE 0 END) /
       SUM( l.l_extendedprice * (1 - l.l_discount )) as mkt_share
  from nation as n,
     region as r,
     customer as c,
     orders as o,
     lineitem as l,
     part as p,
     supplier as s,
     nation2 as n2
where 1 = 1
  and p.p_partkey = l.l_partkey
  and s.s_suppkey = l.l_suppkey
  and l.l_orderkey = o.o_orderkey
  and o.o_custkey = c.c_custkey
  and c.c_nationkey = n.n_nationkey
  and n.n_regionkey = r.r_regionkey
  and r.r_name = 'AMERICA'
  and s.s_nationkey = n2.n2_nationkey
  and o.o_orderdate >= DATE '1995-01-01'
  and o.o_orderdate <= DATE '1996-12-31'
  and p.p_type = 'ECONOMY ANODIZED STEEL'
group by EXTRACT(YEAR FROM o.o_orderdate)
order by EXTRACT(YEAR FROM o.o_orderdate)

