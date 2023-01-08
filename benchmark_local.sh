#!/bin/sh

# give Clickhouse some time to start
sleep 5

echo "Running chgotest"
/chgotest -server clickhouse
/chgotest -server clickhouse
/chgotest -server clickhouse

#sleep 5

echo "Running chconn3test"
/chconn3test -server clickhouse
/chconn3test -server clickhouse
/chconn3test -server clickhouse

sleep 5

echo "Running chconntest"
/chconntest -server clickhouse
/chconntest -server clickhouse
/chconntest -server clickhouse
