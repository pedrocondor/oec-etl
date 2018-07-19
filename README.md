# OEC ETL

## Setup

### 1. Clone the repo

```commandline
$ https://github.com/observatory-economic-complexity/oec-etl
$ cd oec-etl
```

### 2. Add environment variables

Use the following as a guide/template for a `.env` file:

```
export MONETDB_OEC_HOST=
export MONETDB_OEC_USER=
export MONETDB_OEC_PASSWORD=
export MONETDB_OEC_DB=
export MONETDB_OEC_PORT=

export ORIGINAL_OEC_DB_HOST=
export ORIGINAL_OEC_DB_USER=
export ORIGINAL_OEC_DB_PASSWORD=
export ORIGINAL_OEC_DB_NAME=
export ORIGINAL_OEC_DB_PORT=

export OEC_BASE_DIR=/path/to/this/directory/

```

### 3. Create the database

Make sure you have MonetDB installed. Following the steps of [this tutorial](https://www.monetdb.org/Documentation/UserGuide/Tutorial) create a dbfarm and a database named `oec`

```commandline
$ monetdbd create /path/to/mydbfarm
$ monetdbd start /path/to/mydbfarm
$ monetdb create oec
$ monetdb release oec
$ mclient -u monetdb -d oec
```

Once logged in, create the appropriate schemas:

```
sql> CREATE SCHEMA "atlas";
sql> CREATE SCHEMA "public";
sql> \q
```

### 4. Run the pipelines

To run all of the pipelines defined under `/etl`:

```commandline
$ python pipelines.py
```

You can also choose to run each of them individually. For example:

```commandline
$ python etl/countries.py
```