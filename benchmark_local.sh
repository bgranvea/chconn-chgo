#!/bin/sh

# give Clickhouse some time to start
sleep 5

echo "Running chgotest"
/chgotest -server clickhouse
/chgotest -server clickhouse
/chgotest -server clickhouse

sleep 5

echo "Running chconn3test with write buffer"
/chconn3test -server clickhouse -write-buffer=true
/chconn3test -server clickhouse -write-buffer=true
/chconn3test -server clickhouse -write-buffer=true

echo "Running chconn3test without write buffer"
/chconn3test -server clickhouse -write-buffer=false
/chconn3test -server clickhouse -write-buffer=false
/chconn3test -server clickhouse -write-buffer=false

sleep 5

echo "Running chconntest"
/chconntest -server clickhouse
/chconntest -server clickhouse
/chconntest -server clickhouse
