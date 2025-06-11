package commands

import (
	"context"
	"fmt"
	"pgdistbench/pkg/client"
	"maps"
	"slices"
	"sort"

	"github.com/spf13/cobra"

	brun "pgdistbench/pkg/client"

	dto "github.com/prometheus/client_model/go"
)

func runnerCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "runner",
		Short: "Test runner setup commands",
	}

	cmd.AddCommand(runnerListCmd())
	cmd.AddCommand(runnerApplyCmd())
	cmd.AddCommand(runnerDeleteCmd())
	cmd.AddCommand(runnerStatusCmd())
	cmd.AddCommand(runnerCancelTaskCmd())
	cmd.AddCommand(runnerRestartCmd())
	cmd.AddCommand(runnerMetrics())

	return cmd
}

func runnerListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List test drivers",
		RunE: func(cmd *cobra.Command, args []string) error {
			br, err := NewBRun()
			if err != nil {
				return err
			}
			defer br.Close()

			cfg, err := readRunnerConfig(args)
			if err != nil {
				return err
			}

			list, err := br.Runners().Access(cfg).List(context.Background())
			if err != nil {
				return fmt.Errorf("list resources: %w", err)
			}

			fmt.Printf("Runners: (%d)\n", len(list))
			for _, r := range list {
				fmt.Printf("%s: ready=%v\n", r.Name, r.Ready)
			}

			return nil
		},
	}
}

func runnerApplyCmd() *cobra.Command {
	var sslmode string

	cmd := &cobra.Command{
		Use:   "apply",
		Short: "Apply a test driver setup",
		RunE: func(cmd *cobra.Command, args []string) error {
			br, err := NewBRun()
			if err != nil {
				return err
			}

			cfg, err := readRunnerConfig(args)
			if err != nil {
				return err
			}

			_, err = br.Runners().Access(cfg).Apply(context.Background(),
				client.ApplyConfig{
					SSLMode: sslmode,
				})
			return err
		},
	}
	cmd.Flags().StringVar(&sslmode, "sslmode", "", "Postgres SSL mode")

	return cmd
}

func runnerDeleteCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "delete",
		Short: "Delete a test driver setup",
		RunE: func(cmd *cobra.Command, args []string) error {
			br, err := NewBRun()
			if err != nil {
				return err
			}

			cfg, err := readRunnerConfig(args)
			if err != nil {
				return err
			}

			return br.Runners().Access(cfg).Delete(context.Background())
		},
	}
}

func runnerStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Get the status of a test driver setup",
		RunE: func(cmd *cobra.Command, args []string) error {
			br, err := NewBRun()
			if err != nil {
				return err
			}
			defer br.Close()

			cfg, err := readRunnerConfig(args)
			if err != nil {
				return err
			}

			err = br.Runners().Access(cfg).Status(context.Background())
			if err != nil {
				return err
			}

			fmt.Println("All runners are ready")
			return nil
		},
	}
}

func runnerCancelTaskCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "cancel",
		Short: "Cancel active runner task",
		RunE: func(cmd *cobra.Command, args []string) error {
			br, err := NewBRun()
			if err != nil {
				return err
			}
			defer br.Close()

			cfg, err := readRunnerConfig(args)
			if err != nil {
				return err
			}

			return br.Runners().Access(cfg).CancelTask(context.Background())
		},
	}
}

func runnerRestartCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "restart",
		Short: "Restart runners",
		RunE: func(cmd *cobra.Command, args []string) error {
			br, err := NewBRun()
			if err != nil {
				return err
			}
			defer br.Close()

			cfg, err := readRunnerConfig(args)
			if err != nil {
				return err
			}
			br.Runners()

			return br.Runners().Access(cfg).Restart(context.Background())
		},
	}
}

func runnerMetrics() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "metrics",
		Short: "Get active benchmark metrics",
		RunE: func(cmd *cobra.Command, args []string) error {
			br, err := NewBRun()
			if err != nil {
				return err
			}
			defer br.Close()

			cfg, err := readRunnerConfig(args)
			if err != nil {
				return err
			}

			result, err := br.Runners().Access(cfg).Metrics(context.Background())
			if err != nil {
				return fmt.Errorf("get metrics: %w", err)
			}

			for i, systemMetrics := range result {
				fmt.Printf("System %d\n", i)
				keys := slices.Collect(maps.Keys(systemMetrics))
				sort.Strings(keys)
				for _, key := range keys {
					mf := systemMetrics[key]
					reportMetric(mf, "	")
				}
			}
			return nil
		},
	}
	return cmd
}

func reportMetric(mf *dto.MetricFamily, indent string) {
	metrics := mf.GetMetric()
	if len(metrics) == 0 {
		return
	}

	name := mf.GetName()
	if unit := mf.GetUnit(); unit != "" {
		fmt.Printf("	%s (%s)", name, unit)
	} else {
		fmt.Printf("  %s", name)
	}

	withIndent := false
	if len(metrics) > 1 {
		withIndent = true
		fmt.Println()
	}

	mt := mf.GetType()
	for _, metric := range mf.GetMetric() {
		if withIndent {
			fmt.Print(indent)
		}

		if labels := metric.GetLabel(); len(labels) > 0 {
			fmt.Printf("{")
			for i, pair := range labels {
				if i > 0 {
					fmt.Printf(", ")
				}
				if key := pair.GetName(); key != "" {
					fmt.Printf("%s: %s", key, pair.GetValue())
				} else {
					fmt.Printf("%s", pair.GetValue())
				}

			}
			fmt.Print("}")
		}
		fmt.Print(": ")

		switch mt {
		case dto.MetricType_COUNTER:
			fmt.Printf("%v", metric.GetCounter().GetValue())
		case dto.MetricType_GAUGE:
			fmt.Printf("%v", metric.GetGauge().GetValue())
		case dto.MetricType_HISTOGRAM:
			hist := metric.GetHistogram()
			fmt.Println()
			fmt.Print(indent)
			fmt.Print(indent)

			samples := hist.GetSampleCount()
			sum := hist.GetSampleSum()
			fmt.Printf("samples=%v, sum=%v, avg=%v", samples, sum, sum/float64(samples))

			buckets := hist.GetBucket()
			lowerBound := 0.0
			lastCount := uint64(0)
			for _, bucket := range buckets {
				count := bucket.GetCumulativeCount() - lastCount
				if count > 0 {
					fmt.Println()
					fmt.Print(indent, indent)
					fmt.Printf("lower=%v, upper=%v, count=%v", lowerBound, bucket.GetUpperBound(), count)
				}
				lowerBound = bucket.GetUpperBound()
				lastCount = bucket.GetCumulativeCount()

			}

		default:
			fmt.Printf("		%s: %v\n", metric.GetLabel(), metric.GetGauge().GetValue())
			return
		}
		fmt.Println()
	}
}

func readRunnerConfig(args []string) (brun.RunnerConfig, error) {
	runner := "default"
	if len(args) > 0 {
		runner = args[0]
	}

	cfg, err := readConfigFile[brun.RunnerConfig]("runners." + runner)
	if err != nil {
		return cfg, fmt.Errorf("read runner config: %w", err)
	}
	return cfg, nil
}
