# Benchmark results

Clickhouse server running on another machine, with latest version (22.12). 

Insertion of 30M rows, split in 1M inserts with 100k blocks. Result is the best of 3 runs.

|        | LZ4 | no compression |
|--------|-----|----------------|
| chconn | 40s | 108s           |
| ch-go  | 22s | 78s            |