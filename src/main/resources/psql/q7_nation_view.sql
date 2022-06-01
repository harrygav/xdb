CREATE VIEW pg7_sf100_nation1 AS
SELECT n1.n_nationkey AS n1_nationkey, n2.n_nationkey AS n2_nationkey, n1.n_name AS n1_name, n2.n_name AS n2_name
FROM pg7_sf100_nation AS n1, pg7_sf100_nation AS n2
WHERE  (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY') or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE');

CREATE VIEW pg5_sf10_nation1 AS
SELECT n1.n_nationkey AS n1_nationkey, n2.n_nationkey AS n2_nationkey, n1.n_name AS n1_name, n2.n_name AS n2_name
FROM pg5_sf10_nation AS n1, pg5_sf10_nation AS n2
WHERE  (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY') or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE');

CREATE VIEW pg5_sf20_nation1 AS
SELECT n1.n_nationkey AS n1_nationkey, n2.n_nationkey AS n2_nationkey, n1.n_name AS n1_name, n2.n_name AS n2_name
FROM pg5_sf20_nation AS n1, pg5_sf20_nation AS n2
WHERE  (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY') or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE');

CREATE VIEW pg5_sf30_nation1 AS
SELECT n1.n_nationkey AS n1_nationkey, n2.n_nationkey AS n2_nationkey, n1.n_name AS n1_name, n2.n_name AS n2_name
FROM pg5_sf30_nation AS n1, pg5_sf30_nation AS n2
WHERE  (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY') or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE');

CREATE VIEW pg5_sf40_nation1 AS
SELECT n1.n_nationkey AS n1_nationkey, n2.n_nationkey AS n2_nationkey, n1.n_name AS n1_name, n2.n_name AS n2_name
FROM pg5_sf40_nation AS n1, pg5_sf40_nation AS n2
WHERE  (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY') or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE');

CREATE VIEW pg5_sf50_nation1 AS
SELECT n1.n_nationkey AS n1_nationkey, n2.n_nationkey AS n2_nationkey, n1.n_name AS n1_name, n2.n_name AS n2_name
FROM pg5_sf50_nation AS n1, pg5_sf50_nation AS n2
WHERE  (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY') or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE');
