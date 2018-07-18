# OEC Cube

## Setup

### Clone the repo

```commandline
$ https://github.com/observatory-economic-complexity/oec-etl
$ cd oec-etl
```

### Create the database

Make sure you have MonetDB installed. Following the steps of [this tutorial](https://www.monetdb.org/Documentation/UserGuide/Tutorial) create a dbfarm and a database named `oec`

```commandline
$ monetdbd create /path/to/mydbfarm
$ monetdbd start /path/to/mydbfarm
$ monetdb create oec
$ monetdb release oec
$ mclient -u monetdb -d oec
```

### Run the pipelines

To run all of the pipelines defined under `/etl`:

```commandline
$ python pipelines.py
```