#!/bin/bash
set -x

#:$1 container name, $2 system_name, $3 scale factor
docker exec -u root $1 bash -c "chmod +x import_tpch.sh"
docker exec $1 bash -c "bash import_tpch.sh $2 $3"
docker exec $1 bash -c "bash create_indexes.sh $2 $3"