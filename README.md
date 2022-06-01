# XDB: In-Situ Cross-Database Query Processing

## Setup

1. Install docker, docker-compose 
2. Create TPCH-Data from the tpch_generator container as volume (for a distributed setup refer to docker-stack.yml)
3. Instantiate example compose file in ```docker-pg-only.yml```
3. Load data into DBMSes for different (register tables in tdX.properties files)

## Executing Queries

1. Compile XDB with maven
2. Run queries through provided test file, e.g. query 3 for scale factor 100 on table distribution 1:
``` mvn test -Dtest=LocGlobOpt#query test -Dq=3 -Dsf=50 -Dtd=1 -Dmode=hybrid```