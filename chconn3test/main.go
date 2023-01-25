package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/vahid-sohrabloo/chconn/v3"
	"github.com/vahid-sohrabloo/chconn/v3/column"
	"log"
	"strings"
	"time"
)

func main() {
	server := flag.String("server", "clickhouse", "clickhouse server")
	port := flag.Int("port", 9000, "clickhouse port")
	db := flag.String("db", "default", "clickhouse database")
	user := flag.String("user", "default", "clickhouse user")
	password := flag.String("password", "", "clickhouse password")
	compression := flag.String("compression", "", "clickhouse compression")
	totalSize := flag.Int("rows", 30_000_000, "total rows to insert")
	insertSize := flag.Int("insert", 1_000_000, "insert size")
	blockSize := flag.Int("block", 100_000, "block size")
	writeBuffer := flag.Bool("write-buffer", true, "write buffer")
	flag.Parse()

	dsn := fmt.Sprintf("host=%s port=%d dbname=%s user=%s use_write_buffer=%t", *server, *port, *db, *user, *writeBuffer)
	if *compression != "" {
		dsn = fmt.Sprintf("%s compress=%s", dsn, *compression)
	}
	if *password != "" {
		dsn = fmt.Sprintf("%s password=%s", dsn, *password)
	}

	cfg, err := chconn.ParseConfig(dsn)
	checkError(err)

	conn, err := chconn.ConnectConfig(context.Background(), cfg)
	checkError(err)

	err = conn.Exec(context.Background(), "DROP TABLE IF EXISTS bench")
	checkError(err)

	err = conn.Exec(context.Background(), fmt.Sprintf("CREATE TABLE bench(%s,%s,%s) ENGINE=Null",
		repeat(2, "arr", " Array(Int64)"),
		repeat(20, "int", " Nullable(Int64)"),
		repeat(10, "str", " Nullable(String)")))
	checkError(err)

	sql := fmt.Sprintf("INSERT INTO bench (%s,%s,%s) VALUES", repeat(2, "arr", ""), repeat(20, "int", ""), repeat(10, "str", ""))

	arrCols := arrayColumns(2)
	intCols := intColumns(20)
	strCols := strColumns(10)

	cols := make([]column.ColumnBasic, 0)
	for _, c := range arrCols {
		c.SetWriteBufferSize(*blockSize)
		cols = append(cols, c)
	}
	for _, c := range intCols {
		c.SetWriteBufferSize(*blockSize)
		cols = append(cols, c)
	}
	for _, c := range strCols {
		c.SetWriteBufferSize(*blockSize)
		cols = append(cols, c)
	}

	stmt, err := conn.InsertStream(context.Background(), sql)
	checkError(err)

	start := time.Now()

	arrData0 := []int64{1, 2}
	arrData1 := []int64{1, 2, 3}

	blockRows := 0
	insertRows := 0
	for totalRows := 0; totalRows < *totalSize; totalRows++ {
		arrCols[0].Append(arrData0)
		arrCols[1].Append(arrData1)
		intCols[0].Append(0)
		intCols[1].Append(0)
		intCols[2].Append(0)
		intCols[3].Append(0)
		intCols[4].Append(0)
		intCols[5].Append(0)
		intCols[6].Append(0)
		intCols[7].Append(0)
		intCols[8].Append(303087673356115614)
		intCols[9].Append(1810353819)
		intCols[10].Append(1810353819)
		intCols[11].Append(771830462424248461)
		intCols[12].Append(48)
		intCols[13].Append(23)
		intCols[14].Append(time.Now().UnixNano())
		intCols[15].Append(time.Now().UnixNano())
		intCols[16].Append(time.Now().UnixNano())
		intCols[17].AppendNil()
		intCols[18].AppendNil()
		intCols[19].AppendNil()
		strCols[0].Append("START_STOP_SESSION")
		strCols[1].Append("10.41.96.199")
		strCols[2].Append("")
		strCols[3].Append("")
		strCols[4].Append("")
		strCols[5].AppendNil()
		strCols[6].AppendNil()
		strCols[7].AppendNil()
		strCols[8].AppendNil()
		strCols[9].AppendNil()

		blockRows++
		insertRows++

		if insertRows == *insertSize {
			if blockRows > 0 {
				err = stmt.Write(context.Background(), cols...)
				checkError(err)

				blockRows = 0
			}

			err = stmt.Flush(context.Background())
			checkError(err)

			// new insert
			stmt, err = conn.InsertStream(context.Background(), sql)
			checkError(err)

			insertRows = 0
		}

		if blockRows == *blockSize {
			err = stmt.Write(context.Background(), cols...)
			checkError(err)

			for _, c := range cols {
				c.Reset()
			}

			blockRows = 0
		}
	}

	// last block
	if blockRows > 0 {
		err = stmt.Write(context.Background(), cols...)
		checkError(err)

		for _, c := range cols {
			c.Reset()
		}

		err = stmt.Flush(context.Background())
		checkError(err)
	}

	log.Printf("Finished in %d msec", time.Now().Sub(start).Milliseconds())
	checkError(conn.Close())
}

func checkError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func repeat(n int, prefix, suffix string) string {
	sb := strings.Builder{}
	for i := 0; i < n; i++ {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(fmt.Sprintf("%s%d%s", prefix, i, suffix))
	}
	return sb.String()
}

func arrayColumns(n int) []*column.Array[int64] {
	res := make([]*column.Array[int64], n)
	for i := 0; i < n; i++ {
		res[i] = column.New[int64]().Array()
	}
	return res
}

func intColumns(n int) []*column.BaseNullable[int64] {
	res := make([]*column.BaseNullable[int64], n)
	for i := 0; i < n; i++ {
		res[i] = column.New[int64]().Nullable()
	}
	return res
}

func strColumns(n int) []*column.StringNullable[string] {
	res := make([]*column.StringNullable[string], n)
	for i := 0; i < n; i++ {
		res[i] = column.NewString().Nullable()
	}
	return res
}
