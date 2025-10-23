package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	mmmath "github.com/FGasper/mongo-speedcam/math"
	"github.com/olekukonko/tablewriter"
	"github.com/samber/lo"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func _runServerStatusLoop(ctx context.Context, connstr string, window time.Duration) error {
	interval := window / 10

	client, err := getClient(connstr)
	if err != nil {
		return err
	}

	type serverStatus struct {
		Opcounters struct {
			Insert int
			Update int
			Delete int
		}
		Metrics struct {
			Document struct {
				Inserted int
				Updated  int
				Deleted  int
			}
		}
	}

	type ssSample struct {
		time   time.Time
		status serverStatus
	}

	var samples []ssSample

	for {
		ss := client.Database("admin").RunCommand(
			ctx,
			bson.D{{"serverStatus", 1}},
		)
		if ss.Err() != nil {
			return fmt.Errorf("fetching server status: %w", ss.Err())
		}

		var newSS serverStatus

		if err := ss.Decode(&newSS); err != nil {
			return fmt.Errorf("parsing server status: %w", err)
		}

		now := time.Now()

		minSampleTime := now.Add(-window)

		samples = append(samples, ssSample{now, newSS})
		for samples[0].time.Before(minSampleTime) {
			samples = samples[1:]
		}

		if len(samples) == 1 {
			fmt.Printf("Compiling stats every %s …\n", interval)
		} else {
			newestSample := lo.LastOrEmpty(samples)
			timeElapsed := newestSample.time.Sub(samples[0].time).Round(
				10 * time.Millisecond,
			)

			docsInserted := newestSample.status.Metrics.Document.Inserted - samples[0].status.Metrics.Document.Inserted
			docsUpdated := newestSample.status.Metrics.Document.Updated - samples[0].status.Metrics.Document.Updated
			docsDeleted := newestSample.status.Metrics.Document.Deleted - samples[0].status.Metrics.Document.Deleted

			totalDocsChanges := docsInserted + docsUpdated + docsDeleted

			fmt.Print("\n" + strings.Repeat("-", 70) + "\n")
			fmt.Printf("Write statistics from the past %s:\n\n", timeElapsed.String())

			fmt.Printf(
				"Metrics (%s total, %s/sec):\n",
				FmtReal(totalDocsChanges),
				FmtReal(mmmath.DivideToF64(
					totalDocsChanges,
					timeElapsed.Seconds(),
				)),
			)

			table := tablewriter.NewWriter(os.Stdout)
			table.Header([]string{
				"Event Type",
				"Count",
				"% of total count",
			})

			lo.Must0(table.Append(
				"delete",
				FmtReal(docsDeleted),
				FmtReal(mmmath.DivideToF64(100*docsDeleted, totalDocsChanges)),
			))

			lo.Must0(table.Append(
				"insert",
				FmtReal(docsInserted),
				FmtReal(mmmath.DivideToF64(100*docsInserted, totalDocsChanges)),
			))

			lo.Must0(table.Append(
				"update",
				FmtReal(docsUpdated),
				FmtReal(mmmath.DivideToF64(100*docsUpdated, totalDocsChanges)),
			))
			lo.Must0(table.Render())

			insertOps := newestSample.status.Opcounters.Insert - samples[0].status.Opcounters.Insert
			updateOps := newestSample.status.Opcounters.Update - samples[0].status.Opcounters.Update
			deleteOps := newestSample.status.Opcounters.Delete - samples[0].status.Opcounters.Delete

			totalOps := insertOps + updateOps + deleteOps

			fmt.Printf(
				"\nOpcounter (%s total, %s/sec):\n",
				FmtReal(totalOps),
				FmtReal(mmmath.DivideToF64(
					totalOps,
					timeElapsed.Seconds(),
				)),
			)
			table = tablewriter.NewWriter(os.Stdout)
			table.Header([]string{
				"Event Type",
				"Count",
				"% of total count",
			})

			lo.Must0(table.Append(
				"delete",
				FmtReal(deleteOps),
				FmtReal(mmmath.DivideToF64(100*deleteOps, totalOps)),
			))

			lo.Must0(table.Append(
				"insert",
				FmtReal(insertOps),
				FmtReal(mmmath.DivideToF64(100*insertOps, totalOps)),
			))

			lo.Must0(table.Append(
				"update",
				FmtReal(updateOps),
				FmtReal(mmmath.DivideToF64(100*updateOps, totalOps)),
			))
			lo.Must0(table.Render())
		}

		time.Sleep(interval)
	}
}
