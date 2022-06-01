SELECT 'DROP VIEW ' || table_name || ';'
FROM information_schema.views
WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
  AND table_name !~ '^pg_';