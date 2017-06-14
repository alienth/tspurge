package main

import (
	"bytes"
	"context"
	"encoding/binary"
	// "encoding/hex"
	"fmt"
	"io"
	"os"

	"github.com/tsuna/gohbase"
	// "github.com/tsuna/gohbase/filter"
	"github.com/tsuna/gohbase/hrpc"
	"github.com/urfave/cli"
)

var client gohbase.Client

func main() {
	app := cli.NewApp()
	app.Name = "tspurge"

	app.Flags = []cli.Flag{
		cli.IntFlag{
			Name:  "start, s",
			Usage: "Start time to delete metrics, in unix epoch time. Will be rounded down to the nearest hour.",
		},
		cli.IntFlag{
			Name:  "end, e",
			Usage: "End time to delete metrics, in unix epoch time. Will be rounded up to the nearest hour.",
		},
		cli.StringFlag{
			Name:  "host",
			Usage: "The HBase host.",
		},
		cli.StringFlag{
			Name:  "noop, n",
			Usage: "Run in no-op mode. Iterate through all of the rows, but don't actually delete them.",
		},
		cli.BoolFlag{
			Name:  "help, h",
			Usage: "show help",
		},
	}

	app.Before = func(c *cli.Context) error {
		if c.Bool("help") {
			cli.ShowAppHelp(c)
			os.Exit(-1)
		}
		if c.String("start") == "" {
			return cli.NewExitError("Error: you must specify start timestamp, e.g. --start 1490000000", -1)
		}
		if c.String("end") == "" {
			return cli.NewExitError("Error: you must specify end timestamp, e.g. --end 1490000000", -1)
		}
		if c.String("host") == "" {
			return cli.NewExitError("Error: you must specify hbase host, e.g. --host hbasehost.local", -1)
		}
		if !c.Args().Present() {
			return cli.NewExitError("Error: you must specify at least one metric", -1)
		}
		return nil
	}

	app.ArgsUsage = "<METRIC_NAME>..."
	app.HideHelp = true

	app.Action = purgeMetric

	err := app.Run(os.Args)
	if err != nil {
		fmt.Printf("Error starting app: %s", err)
	}

}

func purgeMetric(c *cli.Context) error {
	client = gohbase.NewClient(c.String("host"))
	getMetrics()

	startKey, stopKey := getRangeKeys(c.Args().Get(0), c.Int("start"), c.Int("end"))

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
			err = deleteKey(string(cell.Row))
			if err != nil {
				return cli.NewExitError("Error deleting row: "+err.Error(), -1)
			}
		}
	}

	return nil
}

var metrics map[string][]byte = make(map[string][]byte)
var tagks map[string][]byte = make(map[string][]byte)
var tagvs map[string][]byte = make(map[string][]byte)

func deleteKey(key string) error {
	deleteRequest, err := hrpc.NewDelStr(context.Background(), "tsdb", key, nil)
	if err != nil {
		return err
	}

	_, err = client.Delete(deleteRequest)
	if err != nil {
		return err
	}
	return nil
}

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
