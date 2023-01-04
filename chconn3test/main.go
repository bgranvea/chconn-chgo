package main

import (
	"context"
	"fmt"
	"github.com/vahid-sohrabloo/chconn/v3"
	"github.com/vahid-sohrabloo/chconn/v3/column"
	"log"
	"os"
	"strings"
	"time"
)

func main() {
	cfg, err := chconn.ParseConfig(os.Getenv("CLICKHOUSE_DSN"))
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

	blockSize := 100_000
	insertSize := 1_000_000
	totalSize := 30_000_000

	sql := fmt.Sprintf("INSERT INTO bench (%s,%s,%s) VALUES", repeat(2, "arr", ""), repeat(20, "int", ""), repeat(10, "str", ""))

	arrCols := arrayColumns(2)
	intCols := intColumns(20)
	strCols := strColumns(10)

	cols := make([]column.ColumnBasic, 0)
	for _, c := range arrCols {
		c.SetWriteBufferSize(blockSize)
		cols = append(cols, c)
	}
	for _, c := range intCols {
		c.SetWriteBufferSize(blockSize)
		cols = append(cols, c)
	}
	for _, c := range strCols {
		c.SetWriteBufferSize(blockSize)
		cols = append(cols, c)
	}

	stmt, err := conn.InsertStream(context.Background(), sql)
	checkError(err)

	log.Printf("Insert %d rows", totalSize)

	start := time.Now()

	blockRows := 0
	insertRows := 0
	for totalRows := 0; totalRows < totalSize; totalRows++ {
		arrCols[0].Append([]int64{1, 2})
		arrCols[1].Append([]int64{1, 2, 3})
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

		if insertRows == insertSize {
			if blockRows > 0 {
				log.Printf("Write last block")
				err = stmt.Write(context.Background(), cols...)
				checkError(err)

				blockRows = 0
			}

			log.Printf("Flush")
			err = stmt.Flush(context.Background())
			checkError(err)

			// new insert
			stmt, err = conn.InsertStream(context.Background(), sql)
			checkError(err)

			insertRows = 0
		}

		if blockRows == blockSize {
			log.Printf("Write block")
			err = stmt.Write(context.Background(), cols...)
			checkError(err)

			blockRows = 0
		}
	}

	// last block
	if blockRows > 0 {
		log.Printf("Write last block")
		err = stmt.Write(context.Background(), cols...)
		checkError(err)

		log.Printf("Flush")
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
