# Benchmark of Go Clickhouse drivers

https://github.com/vahid-sohrabloo/chconn
https://github.com/ClickHouse/ch-go

Clickhouse server running on a remote machine (Gigabits LAN), with latest version (22.12).

Insertion of 30M rows, split in 1M inserts with 100k blocks. Result is the best of 3 runs.

|                      | LZ4 | no compression |
|----------------------|-----|----------------|
| chconn V2            | 37s | 106s           |
| chconn V3 (snapshot) | 22s | 89s            |
| ch-go                | 21s | 78s            |
