package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"

	"github.com/tsuna/gohbase"
	// "github.com/tsuna/gohbase/filter"
	"github.com/tsuna/gohbase/hrpc"
)

var client gohbase.Client

func main() {
	client = gohbase.NewClient("ny-tsdb01")
	getMetrics()

	startKey, stopKey := getRangeKeys("os.net.bytes", 1497387618, 1497480000)

	// pFilter := filter.NewPrefixFilter([]byte("0"))
	// family := map[string][]string{"cf": []string{"t"}}
	// pFilter = nil

	scanRequest, _ := hrpc.NewScanRange(context.Background(), []byte("tsdb"), startKey, stopKey)
	scanRsp := client.Scan(scanRequest)

	for {
		row, err := scanRsp.Next()
		if err == io.EOF {
			break
		}
		for _, cell := range row.Cells {
			fmt.Println(hex.Dump(cell.Row))
		}
	}

}

var metrics map[string][]byte = make(map[string][]byte)
var tagks map[string][]byte = make(map[string][]byte)
var tagvs map[string][]byte = make(map[string][]byte)

func getMetrics() {
	family := map[string][]string{"name": []string{"metrics", "tagk", "tagv"}}
	families := hrpc.Families(family)
	scanRequest, _ := hrpc.NewScanStr(context.Background(), "tsdb-uid", families)
	scanRsp := client.Scan(scanRequest)

	for {
		row, err := scanRsp.Next()
		if err == io.EOF {
			break
		}
		for _, cell := range row.Cells {
			value := string(cell.Value)
			switch qual := string(cell.Qualifier); qual {
			case "metrics":
				metrics[string(value)] = cell.Row
			case "tagk":
				tagks[string(value)] = cell.Row
			case "tagv":
				tagvs[string(value)] = cell.Row

			}
		}
	}
	for k, _ := range metrics {
		fmt.Println(k)
	}
}

func baseTimestamp(ts int) int {
	return ts - (ts % 3600)
}

func timestampBytes(ts int) []byte {
	bs := make([]byte, 4)
	binary.BigEndian.PutUint32(bs, uint32(ts))
	return bs
}

func getRangeKeys(metric string, start int, stop int) ([]byte, []byte) {
	var startKey bytes.Buffer
	var stopKey bytes.Buffer

	startKey.Write(metrics[metric])
	stopKey.Write(metrics[metric])

	startTs := timestampBytes(baseTimestamp(start))
	stopTs := timestampBytes(baseTimestamp(stop))
	startKey.Write(startTs)
	stopKey.Write(stopTs)

	return startKey.Bytes(), stopKey.Bytes()
}
