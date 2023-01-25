#!/bin/sh

echo "Using Clickhouse server ${CLICKHOUSE_SERVER}"

# give Clickhouse some time to start
sleep 5

echo "Running chgotest"
/chgotest -server "${CLICKHOUSE_SERVER}"
/chgotest -server "${CLICKHOUSE_SERVER}"
/chgotest -server "${CLICKHOUSE_SERVER}"

sleep 5

echo "Running chconn3test with write buffer"
/chconn3test -server "${CLICKHOUSE_SERVER}" -write-buffer=true
/chconn3test -server "${CLICKHOUSE_SERVER}" -write-buffer=true
/chconn3test -server "${CLICKHOUSE_SERVER}" -write-buffer=true

sleep 5
echo "Running chconn3test without write buffer"
/chconn3test -server "${CLICKHOUSE_SERVER}" -write-buffer=false
/chconn3test -server "${CLICKHOUSE_SERVER}" -write-buffer=false
/chconn3test -server "${CLICKHOUSE_SERVER}" -write-buffer=false

sleep 5

echo "Running chconntest"
/chconntest -server "${CLICKHOUSE_SERVER}"
/chconntest -server "${CLICKHOUSE_SERVER}"
/chconntest -server "${CLICKHOUSE_SERVER}"

sleep 5

echo "Running chgotest with compression"
/chgotest -server "${CLICKHOUSE_SERVER}" -compression lz4
/chgotest -server "${CLICKHOUSE_SERVER}" -compression lz4
/chgotest -server "${CLICKHOUSE_SERVER}" -compression lz4

sleep 5

echo "Running chconn3test with compression"
/chconn3test -server "${CLICKHOUSE_SERVER}" -compression lz4
/chconn3test -server "${CLICKHOUSE_SERVER}" -compression lz4
/chconn3test -server "${CLICKHOUSE_SERVER}" -compression lz4

sleep 5

echo "Running chconntest with compression"
/chconntest -server "${CLICKHOUSE_SERVER}" -compression lz4
/chconntest -server "${CLICKHOUSE_SERVER}" -compression lz4
/chconntest -server "${CLICKHOUSE_SERVER}" -compression lz4
