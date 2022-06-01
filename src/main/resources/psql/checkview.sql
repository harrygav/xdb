CREATE OR REPLACE FUNCTION viewfunc(viewname TEXT)
    RETURNS TABLE
            (
                initschema BIGINT
            )
    language plpgsql
AS
$$
DECLARE
    exists int;
    v_sql  text;
    r_sql  text;
BEGIN
    v_sql = 'SELECT count(1) FROM information_schema.tables WHERE '
                || E'  table_name=\'' || viewname || E'\'';
    r_sql = 'SELECT * FROM db1.public.' || viewname;

    EXECUTE v_sql INTO exists;

    WHILE exists = 0
        LOOP
            EXECUTE 'select pg_sleep(1)';
            EXECUTE v_sql INTO exists;
        END LOOP;

    return query
        EXECUTE r_sql;
END;
$$
;