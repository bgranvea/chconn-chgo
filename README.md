# Benchmark of Go Clickhouse drivers

Comparison of the insertion of 30M rows, split in 1M inserts with 100k blocks.

https://github.com/vahid-sohrabloo/chconn (V2 and V3)
https://github.com/ClickHouse/ch-go

To run benchmark with docker-compose:

`docker-compose -f docker-compose-local.yml up`

To run benchmark with Clickhouse on a different machine:

Server machine: `docker-compose -f docker-compose-server.yml up`

Client machine: `CLICKHOUSE_SERVER=xxxx docker-compose -f docker-compose-client.yml up`

# Results

| Driver               | Time (s) |
|----------------------|----------|
| ch-go                | 40       |
| chconn V3 (snapshot) | 29       |
| chconn V2            | 73       |

# Results on a remote Clickhouse

Clickhouse server running on a remote machine with a Gigabits LAN.

No compression:

| Driver               | Time (s) |
|----------------------|----------|
| ch-go                | 92       |
| chconn V3 (snapshot) | 88       |
| chconn V2            | 110      |

With LZ4 compression (note: compression results are biased as we always send the same row):

| Driver               | Time (s) |
|----------------------|----------|
| ch-go                | 33       |
| chconn V3 (snapshot) | 27       |
| chconn V2            | 52       |
