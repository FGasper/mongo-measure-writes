package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/FGasper/mongo-measure-writes/mmongo"
	"github.com/urfave/cli/v3"
)

func main() {
	getURI := func(c *cli.Command) (string, error) {
		uri := c.Args().First()
		if uri == "" {
			return "", fmt.Errorf("connection string required")
		}

		altered, uri, err := mmongo.MaybeAddDirectConnection(uri)
		if err != nil {
			return "", fmt.Errorf("parsing connection string: %w", err)
		}

		if altered {
			fmt.Fprint(os.Stderr, "NOTE: Defaulting to direct connection.\n")
		}

		return uri, nil
	}

	durationFlag := cli.DurationFlag{
		Name:    "duration",
		Aliases: sliceOf("d"),
		Usage:   "interval over which to compile metrics",
		Value:   time.Minute,
	}

	cmd := cli.Command{
		Name:  os.Args[0],
		Usage: "Measure MongoDB document writes per second",
		Commands: []*cli.Command{
			{
				Name:    "aggregate-oplog",
				Aliases: sliceOf("ao"),
				Usage:   "measure by reading the oplog once",
				Flags: []cli.Flag{
					&durationFlag,
				},
				Action: func(ctx context.Context, c *cli.Command) error {
					uri, err := getURI(c)
					if err != nil {
						return err
					}

					return _runOplogMode(ctx, uri, c.Duration(durationFlag.Name))
				},
			},
			{
				Name:    "tail-oplog",
				Aliases: sliceOf("to"),
				Usage:   "measure by tailing the oplog",
				Flags: []cli.Flag{
					&durationFlag,
				},
				Action: func(ctx context.Context, c *cli.Command) error {
					uri, err := getURI(c)
					if err != nil {
						return err
					}

					return _runTailOplogMode(ctx, uri, c.Duration(durationFlag.Name))
				},
			},
			{
				Name:    "changestream",
				Aliases: sliceOf("cs"),
				Usage:   "measure by reading a change stream (once)",
				Flags: []cli.Flag{
					&durationFlag,
				},
				Action: func(ctx context.Context, c *cli.Command) error {
					uri, err := getURI(c)
					if err != nil {
						return err
					}

					return _runChangeStream(ctx, uri, c.Duration(durationFlag.Name))
				},
			},
			{
				Name:    "tail-changestream",
				Aliases: sliceOf("tcs"),
				Usage:   "measure by tailing a change stream",
				Flags: []cli.Flag{
					&durationFlag,
				},
				Action: func(ctx context.Context, c *cli.Command) error {
					uri, err := getURI(c)
					if err != nil {
						return err
					}

					return _runChangeStreamLoop(ctx, uri, c.Duration(durationFlag.Name))
				},
			},
			{
				Name:    "serverstatusloop",
				Aliases: sliceOf("ssl"),
				Usage:   "measure via serverStatus (continually)",
				Flags: []cli.Flag{
					&durationFlag,
				},
				Action: func(ctx context.Context, c *cli.Command) error {
					uri, err := getURI(c)
					if err != nil {
						return err
					}

					return _runServerStatusLoop(ctx, uri, c.Duration(durationFlag.Name))
				},
			},
		},
	}

	if err := cmd.Run(context.Background(), os.Args); err != nil {
		fmt.Fprint(os.Stderr, err.Error()+"\n")
		os.Exit(1)
	}
}
