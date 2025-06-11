package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	brun "pgdistbench/pkg/client"
)

type ExecConfig struct{}

func execCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "exec",
		Short: "Execute benchmark runner tasks",
	}
	cmd.AddCommand(listBenchmarkPods())
	cmd.AddCommand(execCheckPods())
	cmd.AddCommand(execPrepare())
	cmd.AddCommand(execRun())
	cmd.AddCommand(execCheck())
	cmd.AddCommand(execCleanup())
	cmd.AddCommand(execResult())

	return cmd
}

func listBenchmarkPods() *cobra.Command {
	return &cobra.Command{
		Use:   "listpods",
		Short: "List benchmarking pods",
		RunE: func(cmd *cobra.Command, args []string) error {
			br, err := NewBRun()
			if err != nil {
				return err
			}

			cfg, err := readExecConfig(args)
			if err != nil {
				return err
			}

			podGroups, err := br.BenchmarkExec().Pods(context.Background(), cfg)
			if err != nil {
				return err
			}

			for _, grp := range podGroups {
				fmt.Println(grp.Deployment.Name)
				for _, pod := range grp.Pods {
					fmt.Println("   ", pod.Name, pod.Status.Phase)
				}
			}
			return nil
		},
	}
}

func execCheckPods() *cobra.Command {
	return &cobra.Command{
		Use:   "check-pods",
		Short: "Check benchmarking pods being ready",
		RunE: benchExecCommand(func(inst *brun.BenchmarkInstance, ctx context.Context) error {
			idle, err := inst.Healthcheck(ctx)
			if err != nil {
				return err
			}

			if idle {
				fmt.Println("Pods are healthy and ready")
			} else {
				fmt.Println("Pods are healthy, but not ready")
			}
			return nil
		}),
	}
}

func execPrepare() *cobra.Command {
	var wait bool
	cmd := &cobra.Command{
		Use:   "prepare",
		Short: "Prepare benchmark database state",
		RunE: benchExecCommand(func(inst *brun.BenchmarkInstance, ctx context.Context) error {
			if err := inst.Prepare(ctx); err != nil {
				return err
			}
			fmt.Println("Started prepare task")

			if wait {
				fmt.Println("Waiting for prepare task to finish")
				return inst.WaitIdle(ctx)
			}
			return nil
		}),
	}
	cmd.Flags().BoolVar(&wait, "wait", false, "Wait for the benchmark to finish")
	return cmd
}

func execRun() *cobra.Command {
	var wait bool
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run benchmark",
		RunE: benchExecCommand(func(inst *brun.BenchmarkInstance, ctx context.Context) error {
			if err := inst.Run(ctx); err != nil {
				return err
			}

			fmt.Println("Benchmark started")
			if wait {
				return execGetAndReportResults(inst, ctx, wait, false)
			}
			return nil
		}),
	}
	cmd.Flags().BoolVar(&wait, "wait", false, "Wait for the benchmark to finish")
	return cmd
}

func execCheck() *cobra.Command {
	return &cobra.Command{
		Use:   "check",
		Short: "Check benchmark database state",
		RunE:  benchExecCommand((*brun.BenchmarkInstance).Check),
	}
}

func execCleanup() *cobra.Command {
	var wait bool
	cmd := &cobra.Command{
		Use:   "cleanup",
		Short: "Cleanup benchmark database state",
		RunE: benchExecCommand(func(inst *brun.BenchmarkInstance, ctx context.Context) error {
			if err := inst.Cleanup(ctx); err != nil {
				return err
			}
			fmt.Println("Started cleanup task")

			if wait {
				fmt.Println("Waiting for cleanup task to finish")
				return inst.WaitIdle(ctx)
			}
			return nil
		}),
	}
	cmd.Flags().BoolVar(&wait, "wait", false, "Wait for the benchmark to finish")
	return cmd
}

func execResult() *cobra.Command {
	var wait bool
	var allowErr bool

	cmd := &cobra.Command{
		Use:   "results",
		Short: "Get benchmark result",
		RunE: benchExecCommand(func(inst *brun.BenchmarkInstance, ctx context.Context) error {
			return execGetAndReportResults(inst, ctx, wait, allowErr)
		}),
	}

	cmd.Flags().BoolVar(&wait, "wait", false, "Wait for the benchmark to finish")
	cmd.Flags().BoolVar(&allowErr, "allow-error", false, "Allow error in the benchmark result")
	return cmd
}

func execGetAndReportResults(inst *brun.BenchmarkInstance, ctx context.Context, wait bool, allowErr bool) error {
	var result any
	var err error

	switch name := strings.ToLower(inst.Config().Benchmark); name {
	case "tpcc":
		result, err = inst.TPCC().Result(ctx, true, allowErr)
	case "tpch":
		result, err = inst.TPCH().Result(ctx, true, allowErr)
	case "chbench":
		result, err = inst.CHBench().Result(ctx, true, allowErr)
	case "k8stress":
		result, err = inst.K8Stress().Result(ctx, true)
	default:
		return fmt.Errorf("unknown benchmark: %s", name)
	}
	if err != nil {
		return err
	}

	// print json document
	tmp, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", tmp)
	return nil
}

type runE func(*cobra.Command, []string) error

func benchExecCommand(fn func(*brun.BenchmarkInstance, context.Context) error) runE {
	return func(cmd *cobra.Command, args []string) error {
		br, err := NewBRun()
		if err != nil {
			return err
		}

		cfg, err := readExecConfig(args)
		if err != nil {
			return err
		}

		inst, err := br.BenchmarkExec().Access(context.Background(), cfg)
		if err != nil {
			return err
		}

		return fn(inst, context.Background())
	}
}

func readExecConfig(args []string) (cfg brun.ExecConfig, err error) {
	benchmark := "default"
	if len(args) > 0 {
		benchmark = args[0]
	}

	cfg, err = readConfigFile[brun.ExecConfig]("benchmarks." + benchmark)
	if err != nil {
		return cfg, fmt.Errorf("read benchmark config: %w", err)
	}
	return cfg, nil
}
