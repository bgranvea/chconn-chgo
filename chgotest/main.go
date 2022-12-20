package main

import (
	"context"
	"fmt"
	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"io"
	"log"
	"os"
	"strings"
	"time"
)

func main() {
	client, err := ch.Dial(context.Background(), ch.Options{
		Address:     os.Getenv("CLICKHOUSE_URL"),
		User:        os.Getenv("CLICKHOUSE_USERNAME"),
		Password:    os.Getenv("CLICKHOUSE_PASSWORD"),
		Database:    os.Getenv("CLICKHOUSE_DB"),
		Compression: compression(),
	})
	checkError(err)

	err = client.Do(context.Background(), ch.Query{Body: "DROP TABLE IF EXISTS bench"})
	checkError(err)

	err = client.Do(context.Background(), ch.Query{Body: fmt.Sprintf("CREATE TABLE bench(%s,%s,%s) ENGINE=Null",
		repeat(2, "arr", " Array(Int64)"),
		repeat(20, "int", " Nullable(Int64)"),
		repeat(10, "str", " Nullable(String)"))})
	checkError(err)

	blockSize := 100_000
	insertSize := 1_000_000
	totalSize := 30_000_000

	arrCols := arrayColumns(2)
	intCols := intColumns(20)
	strCols := strColumns(10)

	cols := make([]proto.InputColumn, 0)
	for i := range arrCols {
		cols = append(cols, proto.InputColumn{Name: fmt.Sprintf("arr%d", i), Data: arrCols[i]})
	}
	for i := range intCols {
		cols = append(cols, proto.InputColumn{Name: fmt.Sprintf("int%d", i), Data: intCols[i]})
	}
	for i := range strCols {
		cols = append(cols, proto.InputColumn{Name: fmt.Sprintf("str%d", i), Data: strCols[i]})
	}

	log.Printf("Insert %d rows", totalSize)

	start := time.Now()

	for totalRows := 0; totalRows < totalSize; {
		log.Printf("New insert")

		insertRows := 0
		proto.Input(cols).Reset()

		err = client.Do(context.Background(), ch.Query{
			Body:  proto.Input(cols).Into("bench"),
			Input: cols,
			OnInput: func(ctx context.Context) error {
				proto.Input(cols).Reset()

				for blockRows := 0; blockRows < blockSize; blockRows++ {
					arrCols[0].Append([]int64{1, 2})
					arrCols[1].Append([]int64{1, 2, 3})
					intCols[0].Append(proto.Nullable[int64]{Set: true, Value: 0})
					intCols[1].Append(proto.Nullable[int64]{Set: true, Value: 0})
					intCols[2].Append(proto.Nullable[int64]{Set: true, Value: 0})
					intCols[3].Append(proto.Nullable[int64]{Set: true, Value: 0})
					intCols[4].Append(proto.Nullable[int64]{Set: true, Value: 0})
					intCols[5].Append(proto.Nullable[int64]{Set: true, Value: 0})
					intCols[6].Append(proto.Nullable[int64]{Set: true, Value: 0})
					intCols[7].Append(proto.Nullable[int64]{Set: true, Value: 0})
					intCols[8].Append(proto.Nullable[int64]{Set: true, Value: 303087673356115614})
					intCols[9].Append(proto.Nullable[int64]{Set: true, Value: 1810353819})
					intCols[10].Append(proto.Nullable[int64]{Set: true, Value: 1810353819})
					intCols[11].Append(proto.Nullable[int64]{Set: true, Value: 771830462424248461})
					intCols[12].Append(proto.Nullable[int64]{Set: true, Value: 48})
					intCols[13].Append(proto.Nullable[int64]{Set: true, Value: 23})
					intCols[14].Append(proto.Nullable[int64]{Set: true, Value: time.Now().UnixNano()})
					intCols[15].Append(proto.Nullable[int64]{Set: true, Value: time.Now().UnixNano()})
					intCols[16].Append(proto.Nullable[int64]{Set: true, Value: time.Now().UnixNano()})
					intCols[17].Append(proto.Nullable[int64]{})
					intCols[18].Append(proto.Nullable[int64]{})
					intCols[19].Append(proto.Nullable[int64]{})
					strCols[0].Append(proto.Nullable[string]{Set: true, Value: "START_STOP_SESSION"})
					strCols[1].Append(proto.Nullable[string]{Set: true, Value: "10.41.96.199"})
					strCols[2].Append(proto.Nullable[string]{Set: true, Value: ""})
					strCols[3].Append(proto.Nullable[string]{Set: true, Value: ""})
					strCols[4].Append(proto.Nullable[string]{Set: true, Value: ""})
					strCols[5].Append(proto.Nullable[string]{})
					strCols[6].Append(proto.Nullable[string]{})
					strCols[7].Append(proto.Nullable[string]{})
					strCols[8].Append(proto.Nullable[string]{})
					strCols[9].Append(proto.Nullable[string]{})

					insertRows++
					totalRows++
					if insertRows == insertSize || totalRows == totalSize {
						log.Printf("Write last block")
						return io.EOF
					}
				}

				log.Printf("Write block")
				return nil
			},
		})
		checkError(err)
	}

	log.Printf("Finished in %d msec", time.Now().Sub(start).Milliseconds())
	checkError(client.Close())
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

func arrayColumns(n int) []*proto.ColArr[int64] {
	res := make([]*proto.ColArr[int64], n)
	for i := 0; i < n; i++ {
		res[i] = new(proto.ColInt64).Array()
	}
	return res
}

func intColumns(n int) []*proto.ColNullable[int64] {
	res := make([]*proto.ColNullable[int64], n)
	for i := 0; i < n; i++ {
		res[i] = new(proto.ColInt64).Nullable()
	}
	return res
}

func strColumns(n int) []*proto.ColNullable[string] {
	res := make([]*proto.ColNullable[string], n)
	for i := 0; i < n; i++ {
		res[i] = new(proto.ColStr).Nullable()
	}
	return res
}

func compression() ch.Compression {
	switch os.Getenv("CLICKHOUSE_COMPRESSION") {
	case "lz4":
		return ch.CompressionLZ4
	case "zstd":
		return ch.CompressionZSTD
	case "none":
		return ch.CompressionNone
	case "disabled":
		return ch.CompressionDisabled
	default:
		return ch.CompressionDisabled
	}
}
