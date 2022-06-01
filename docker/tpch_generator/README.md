# Data generator

*This is a working documentation for the data generator.*

In general, you only need to work in this directory if you want to build, run or test the data generator in a minimal setup without the other systems in this repo.

### Build
This builds the data generator Docker image based on the Dockerfile in this directory. You only need to run this if you made changes to the Dockerfile or if you have not built the data generator image yet. In any way, Docker caches your images and will not repeat all of the build steps if you have built the images already.

```
make build
```

### Run

```
docker run -it --rm -v test-data:/data --name tpch-generator tpch-generator
```

After the data generator has successfully generated test data and exited (look into the Docker logs), you can load data into the DBMSs with the respective `src/main/resources/import_data/import_DBMSX.sh` scripts.


#### PostgreSQL
Use the script `import_postgres_data.sh`, which accepts 3 parameters. 1: docker container name, 2: db name (1,2,3,...), and 3: scale factor. For example, `./import_postgres_data.sh pg2 2 10` will import data into the container `pg2` with the database pg`2` of scale factor 10, i.e. the tables will look like `pg2_sf10_lineitem` etc.

#### MariaDB
TODO: write script instructions

#### HIVE
TODO: write script instructions

#### EXASOL
TODO: write script instructions