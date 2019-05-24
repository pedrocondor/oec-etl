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
export CLICKHOUSE_URL="127.0.0.1"
export CLICKHOUSE_DATABASE="oec_test"
```

### 3. Run an example pipeline

The countries dimension pipeline is super fast to run and great way to test your setup works.

```commandline
$ python etl/dim_countries_pipeline.py
```

## Naming Convention for Tables

When adding a new pipeline script, please use the following naming convention:

### Fact tables

*Format*: `<type>_<depth>_<identifier>_<frequency>_<classification>`

*Params*:

`type`: What the fact table represents (trade, tariffs, services, etc.).
`depth`: `i` for international and `s` subnational data.
`identifier`: For subnational data, this should be the `iso3` for the reporter country. For international data, this should be the organization reporting the data.
`depth`: `a` for annual and `m` for monthly.
`classification`: The classification used by this table.

*Examples*:

`trade_s_bra_a_hs` for annual Brazilian subnational trade data using the HS classification
`trade_i_comtrade_m_hs` for monthly international Comtrade trade data using the HS classification

### Dimension tables

*Format*: `dim_<identifier>_<dimension>`

*Params*:

`identifier`: For subnational data, this should be the `iso3` for the reporter country. For international data, this should say `shared`.
`dimension`: What this dimension table actually represents.

*Examples*:

`dim_shared_countries` for a shared countries table
`dim_rus_regions` for a Russia dimension table representing national regions
