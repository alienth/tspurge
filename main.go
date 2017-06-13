package main

import (
	"context"
	// "encoding/hex"
	"fmt"
	"io"

	"github.com/tsuna/gohbase"
	"github.com/tsuna/gohbase/filter"
	"github.com/tsuna/gohbase/hrpc"
)

var client gohbase.Client

func main() {
	client = gohbase.NewClient("ny-tsdb01")
	getMetrics()

	return
	pFilter := filter.NewPrefixFilter([]byte("0"))
	// family := map[string][]string{"cf": []string{"t"}}
	// pFilter = nil

	maxSize := hrpc.MaxResultSize(10)
	scanRequest, _ := hrpc.NewScanStr(context.Background(), "tsdb", hrpc.Filters(pFilter), maxSize)
	scanRsp := client.Scan(scanRequest)

	data, _ := scanRsp.Next()

	fmt.Println(data)

}

var metrics map[string][]byte = make(map[string][]byte)
var tagks map[string][]byte = make(map[string][]byte)
var tagvs map[string][]byte = make(map[string][]byte)

func getMetrics() {
	// maxSize := hrpc.NumberOfRows(10)
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
