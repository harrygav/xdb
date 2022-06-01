-- TPC-H Query 7

select n.n1_name                                   as supp_nation,
       n.n2_name                                   as cust_nation,
       EXTRACT(YEAR FROM l.l_shipdate )             as l_year,
       SUM( l.l_extendedprice * (1 - l.l_discount )) as revenue
  from nation1 as n,
     customer as c,
     orders as o,
     lineitem as l,
     supplier as s
where 1 = 1
  and s.s_suppkey = l.l_suppkey
  and o.o_orderkey = l.l_orderkey
  and c.c_custkey = o.o_custkey
  and s.s_nationkey = n.n1_nationkey
  and c.c_nationkey = n.n2_nationkey
  and l.l_shipdate >= date '1995-01-01'
  and l.l_shipdate <= date '1996-12-31'
group by n1_name, n2_name, EXTRACT(YEAR FROM l.l_shipdate)
order by n1_name, n2_name, EXTRACT(YEAR FROM l.l_shipdate)