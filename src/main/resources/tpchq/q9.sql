-- TPC-H Query 9

select n.n_name                                                                      as nation,
       EXTRACT(YEAR FROM o.o_orderdate )                                              as o_year,
       SUM( l.l_extendedprice * (1 - l.l_discount ) - ps.ps_supplycost * l.l_quantity ) as sum_profit
  from part as p,
     lineitem as l,
     orders as o,
     partsupp as ps,
     supplier as s,
     nation as n
where 1 = 1
  and s.s_suppkey = l.l_suppkey
  and ps.ps_suppkey = l.l_suppkey
  and ps.ps_partkey = l.l_partkey
  and p.p_partkey = l.l_partkey
  and o.o_orderkey = l.l_orderkey
  and s.s_nationkey = n.n_nationkey
  and ps.ps_partkey = p.p_partkey
  and s.s_suppkey = ps.ps_suppkey
  and p.p_name LIKE '%green%'
group by n.n_name, EXTRACT(YEAR FROM o.o_orderdate)
order by n.n_name, EXTRACT(YEAR FROM o.o_orderdate) desc
