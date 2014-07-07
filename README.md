# tpch-cass-ingest

Load up tpc-h generated data for analysis

## Query types

Total sales by time range
Total returns by part (within time)
Total sales by part/supplier
Top 20 valueable customers (by time)
Top 20 valueable orders (by time)

## Usage

Generate data with [DBGen](https://github.com/electrum/tpch-dbgen) using the following command.

```
dbgen -vf -s 1
```

Choose a different scale factor if you need to.
