package main

import (
	"bytes"
	"context"
	"encoding/binary"
	// "encoding/hex"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/tsuna/gohbase"
	// "github.com/tsuna/gohbase/filter"
	log "github.com/Sirupsen/logrus"
	"github.com/tsuna/gohbase/hrpc"
	"github.com/urfave/cli"
)

var client gohbase.Client

func main() {
	log.SetLevel(log.WarnLevel)
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

	metric := c.Args().Get(0)
	metricId, err := getMetricId(metric)
	if err != nil {
		return cli.NewExitError("Error fetching metric ID: "+err.Error(), -1)
	}

	startBase := baseTimestamp(c.Int("start"))
	endBase := baseTimestamp(c.Int("end") + 3600)
	startTs := timestampBytes(startBase)
	endTs := timestampBytes(endBase)

	startKey, endKey := getRangeKeys(metricId, startTs, endTs)

	question := fmt.Sprintf("Will delete all datapoints for metric %s from %s to %s. Confirm?", metric, startBase, endBase)
	proceed, err := prompt(question)
	if err != nil {
		return cli.NewExitError(err, -1)
	}
	if !proceed {
		return cli.NewExitError("Aborting.", -1)
	}

	// pFilter := filter.NewPrefixFilter([]byte("0"))
	// family := map[string][]string{"cf": []string{"t"}}
	// pFilter = nil

	scanRequest, err := hrpc.NewScanRange(context.Background(), []byte("tsdb"), startKey, endKey)
	if err != nil {
		return cli.NewExitError("Error performing range scan: "+err.Error(), -1)
	}
	scanRsp := client.Scan(scanRequest)

	i := 0
	for {
		row, err := scanRsp.Next()
		if err == io.EOF {
			break
		}
		for _, cell := range row.Cells {
			_, ts := parseKey(cell.Row)

			if !c.Bool("noop") {
				err = deleteKey(string(cell.Row))
				if err != nil {
					return cli.NewExitError("Error deleting row: "+err.Error(), -1)
				}
			}
			i++
			fmt.Printf("\r%d rows deleted. Up to %s", i, ts)
		}
	}

	return nil
}

var metrics map[string][]byte = make(map[string][]byte)
var tagks map[string][]byte = make(map[string][]byte)
var tagvs map[string][]byte = make(map[string][]byte)

func parseKey(key []byte) (string, time.Time) {
	ts := binary.BigEndian.Uint32(key[3:7])

	return "", time.Unix(int64(ts), 0)
}

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

func getMetricId(metric string) ([]byte, error) {
	if id, ok := metrics[metric]; ok {
		return id, nil
	}
	family := map[string][]string{"id": []string{"metrics"}}
	families := hrpc.Families(family)

	getRequest, err := hrpc.NewGetStr(context.Background(), "tsdb-uid", metric, families)
	if err != nil {
		return nil, err
	}

	response, err := client.Get(getRequest)
	if err != nil {
		return nil, err
	}

	if len(response.Cells) < 1 {
		return nil, fmt.Errorf("Could not find ID for metric " + metric)
	}

	metrics[metric] = response.Cells[0].Value
	return metrics[metric], nil
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

func baseTimestamp(ts int) time.Time {
	base := time.Unix(int64(ts), 0)
	return base.Truncate(time.Duration(time.Hour * 1))
}

func timestampBytes(ts time.Time) []byte {
	bs := make([]byte, 4)
	binary.BigEndian.PutUint32(bs, uint32(ts.Unix()))
	return bs
}

func getRangeKeys(metricId, start, end []byte) ([]byte, []byte) {
	var startKey bytes.Buffer
	var endKey bytes.Buffer

	startKey.Write(metricId)
	endKey.Write(metricId)

	startKey.Write(start)
	endKey.Write(end)

	return startKey.Bytes(), endKey.Bytes()
}

func prompt(question string) (bool, error) {
	var input string
	for {
		fmt.Printf("%s (y/n): ", question)
		if _, err := fmt.Scanln(&input); err != nil {
			return false, err
		}
		if input == "y" {
			return true, nil
		} else if input == "n" {
			return false, nil
		} else {
			fmt.Printf("Invalid input: %s", input)
		}
	}
}
