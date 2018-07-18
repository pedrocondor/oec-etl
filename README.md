# OEC Cube

## Setup

### Clone the repo

```commandline
$ git clone https://github.com/Datawheel/oec.git
$ cd oec
```

### Download the raw data

Download from GCP and save each CSV file inside the data folder with the appropriate names:

- hs02_2016.csv
- hs07_2016.csv
- hs92_2016.csv
- hs96_2016.csv

### Create the database

Make sure you have MonetDB installed. Following the steps of [this tutorial](https://www.monetdb.org/Documentation/UserGuide/Tutorial) create a dbfarm and a database named `oec`

```commandline
$ monetdbd create /path/to/mydbfarm
$ monetdbd start /path/to/mydbfarm
$ monetdb create oec
$ monetdb release oec
$ mclient -u monetdb -d oec
```

### Run the pipeline

```commandline
$ cd etl/
$ python pipelines.py
```

After the pipeline has finished running (which will take a few hours)... 

## Not yet implemented:

- STIC Cube
- STIC PCI Table
