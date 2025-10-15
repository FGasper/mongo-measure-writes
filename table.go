package main

import (
	"fmt"
	"maps"
	"os"
	"slices"
	"time"

	mmmath "github.com/FGasper/mongo-measure-writes/math"
	"github.com/olekukonko/tablewriter"
	"github.com/samber/lo"
)

func displayTable(
	eventCountsByType map[string]int,
	eventSizesByType map[string]int,
	delta time.Duration,
) {
	if delta == 0 {
		panic("nonzero delta is nonsensical!")
	}

	if len(eventCountsByType) == 0 {
		fmt.Printf("No writes seen.\n")
		return
	}

	allEventsCount := lo.Sum(slices.Collect(maps.Values(eventCountsByType)))
	totalSize := lo.Sum(slices.Collect(maps.Values(eventSizesByType)))

	fmt.Printf(
		"\n%s ops/sec (%s/sec; avg: %s)\n",
		FmtReal((mmmath.DivideToF64(allEventsCount, delta.Seconds()))),
		FmtBytes(mmmath.DivideToF64(totalSize, delta.Seconds())),
		FmtBytes(mmmath.DivideToF64(totalSize, allEventsCount)),
	)

	table := tablewriter.NewWriter(os.Stdout)
	table.Header([]string{
		"Event",
		"Count",
		"Total Size",
		"Avg Size",
	})

	eventTypes := slices.Sorted(maps.Keys(eventCountsByType))

	for _, eventType := range eventTypes {
		countFraction := mmmath.DivideToF64(eventCountsByType[eventType], allEventsCount)
		sizeFraction := mmmath.DivideToF64(eventSizesByType[eventType], totalSize)

		lo.Must0(table.Append([]string{
			eventType,
			fmt.Sprintf(
				"%s (%s%%)",
				FmtReal(eventCountsByType[eventType]),
				FmtReal(100*countFraction),
			),
			fmt.Sprintf(
				"%s (%s%%)",
				FmtBytes(eventSizesByType[eventType]),
				FmtReal(100*sizeFraction),
			),
			FmtBytes(mmmath.DivideToF64(eventSizesByType[eventType], eventCountsByType[eventType])),
		}))
	}

	lo.Must0(table.Render())
}
