-- TPC-H Query 10

select c.c_custkey,
       c.c_name,
       sum( l.l_extendedprice * (1 - l.l_discount )) as revenue,
       c.c_acctbal,
       n.n_name,
       c.c_address,
       c.c_phone,
       c.c_comment
  from customer as c,
     orders as o,
     nation as n,
     lineitem as l
where c.c_custkey = o.o_custkey
  and l.l_orderkey = o.o_orderkey
  and o.o_orderdate >= date '1993-10-01'
  and o.o_orderdate < date '1994-01-01'
  and l.l_returnflag = 'R'
  and c.c_nationkey = n.n_nationkey
group by c_custkey,
         c_name,
         c_acctbal,
         c_phone,
         n_name,
         c_address,
         c_comment
order by revenue desc
limit 20
