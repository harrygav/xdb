select
    srvname as name,
    srvowner::regrole as owner,
    fdwname as wrapper,
    srvoptions as options
from pg_foreign_server
         join pg_foreign_data_wrapper w on w.oid = srvfdw;