package gotpcutil

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sort"
	"sync"

	"pgdistbench/api/benchdriverapi"
	"pgdistbench/internal/worker"

	"github.com/pingcap/go-tpc/pkg/measurement"
	"github.com/pingcap/go-tpc/pkg/workload"
	"github.com/pingcap/go-tpc/tpcc"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"
)

type CLIExec struct {
	cfg  worker.Config
	cmd  []string
	args []string
}

type CLICommand []string

var (
	CmdTPCCRun CLICommand = []string{"tpcc", "run"}
	CmdTPCHRUn CLICommand = []string{"tpch", "run"}
)

func NewCLIExec(cfg worker.Config, cmd CLICommand, args ...string) *CLIExec {
	return &CLIExec{
		cfg:  cfg,
		cmd:  cmd,
		args: args,
	}
}

func (g *CLIExec) AddArgs(args ...string) {
	g.args = append(g.args, args...)
}

func ExecGoTPC[T any](
	g *CLIExec,
	ctx context.Context,
	stdoutFn func(context.Context, io.Reader) (T, error),
) (report T, err error) {
	args := make([]string, 0, len(g.cmd)+len(g.args)+8)
	args = append(args, g.cmd...)
	args = append(args,
		"-d", "postgres",
		"-H", g.cfg.PGHost,
		"-P", g.cfg.PGPort,
		"-U", g.cfg.PGUser,
		"-p", g.cfg.PGPass,
		"-D", g.cfg.PGDatabase,
		"--conn-params", "sslmode="+g.cfg.PGSSLMode,
		"--output", "json",
	)
	args = append(args, g.args...)

	cmd := exec.CommandContext(ctx, "go-tpc", args...)
	fmt.Print("run command: go-tpc")
	for _, arg := range args {
		fmt.Printf(" %s", arg)
	}
	fmt.Println()

	cmd.Stderr = os.Stderr

	eg, processCtx := errgroup.WithContext(ctx)

	var stdout io.ReadCloser
	if stdoutFn != nil {
		stdout, err = cmd.StdoutPipe()
		if err != nil {
			return report, fmt.Errorf("create stdout pipe: %w", err)
		}
		defer stdout.Close()
	}

	if err := cmd.Start(); err != nil {
		return report, fmt.Errorf("start benchmark runner: %w", err)
	}

	if stdout != nil {
		eg.Go(func() error {
			v, err := stdoutFn(processCtx, stdout)
			if err != nil {
				return err
			}
			report = v
			return nil
		})
	}

	eg.Go(func() error {
		if err = cmd.Wait(); err != nil {
			return fmt.Errorf("run: %w", err)
		}
		return nil
	})

	if err = eg.Wait(); err != nil {
		return report, err
	}
	return report, ctx.Err()
}

func IsolationFromAPI(level benchdriverapi.IsolationLevel) (sql.IsolationLevel, error) {
	switch level {
	case benchdriverapi.IsolationLevelDefault:
		return sql.LevelDefault, nil
	case benchdriverapi.IsoaltionLevelReadCommitted:
		return sql.LevelReadCommitted, nil
	case benchdriverapi.IsoaltionLevelReadUncommited:
		return sql.LevelReadUncommitted, nil
	case benchdriverapi.IsoaltionLevelRepeatableRead:
		return sql.LevelRepeatableRead, nil
	case benchdriverapi.IsoaltionLevelSnapshot:
		return sql.LevelSnapshot, nil
	case benchdriverapi.IsolationLevelSerializable:
		return sql.LevelSerializable, nil
	case benchdriverapi.IsolationLevelLinearizable:
		return sql.LevelLinearizable, nil
	default:
		return 0, fmt.Errorf("unknown isolation level %v", level)
	}
}

func PartitionTypeFromAPI(pt benchdriverapi.PartitionType) (int, error) {
	switch pt {
	case benchdriverapi.PartitionHash:
		return tpcc.PartitionTypeHash, nil
	case benchdriverapi.PatitionRange:
		return tpcc.PartitionTypeRange, nil
	case benchdriverapi.PatitionListHash:
		return tpcc.PartitionTypeListAsHash, nil
	case benchdriverapi.PatitionListRange:
		return tpcc.PartitionTypeListAsRange, nil
	default:
		return 0, fmt.Errorf("unknown partition type %v", pt)
	}
}

func ExecThreadFuncs(ctx context.Context, w workload.Workloader, threads int, fn func(context.Context, int) error) error {
	eg, threadCtx := errgroup.WithContext(ctx)
	for i := 0; i < threads; i++ {
		index := i
		eg.Go(func() (err error) {
			ctx := w.InitThread(threadCtx, index)
			defer w.CleanupThread(ctx, index)
			return fn(ctx, index)
		})
	}
	return eg.Wait()
}

// ScanFinished reads and consumes the scanner until it finds the "Finished"
// line emitted by go-tpc right before the final stats are printed.
// The lines function can be configured to receive and process each of the lines before the "Finished" line.
func ScanFinished(scanner *bufio.Scanner, lines func(string)) error {
	for scanner.Scan() {
		line := scanner.Text()
		if line == "Finished" {
			return nil
		}
		if lines != nil {
			lines(line)
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return errors.New("no 'Finished' line found")
}

func EmitLinesTo(out io.Writer) func(string) {
	return func(line string) {
		fmt.Fprintln(out, line)
	}
}

func ReadOpStats(m *measurement.Measurement, summaryReport bool) []benchdriverapi.OpStats {
	opStats := make([]benchdriverapi.OpStats, 0, 5)
	m.Output(summaryReport, "", func(_ string, prefix string, opMeasurement map[string]*measurement.Histogram) {
		keys := make([]string, 0, len(opMeasurement))
		for k := range opMeasurement {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, op := range keys {
			if stat, ok := OpStatsFromHist(prefix, op, opMeasurement[op]); ok {
				opStats = append(opStats, stat)
			}
		}
	})
	return opStats
}

func OpStatsFromHist(prefix, op string, hist *measurement.Histogram) (stats benchdriverapi.OpStats, ok bool) {
	if hist.Empty() {
		return stats, false
	}

	info := hist.GetInfo()
	return benchdriverapi.OpStats{
		Prefix:    prefix,
		Operation: op,
		Count:     info.Count,
		Sum:       info.Sum,
		Avg:       info.Avg,
		Median:    info.P50,
		Max:       info.Max,
	}, true
}

type MetricsGroup interface {
	RegisterMetrics(reg prometheus.Registerer)
}

type LazyMetrics[T MetricsGroup] struct {
	init             sync.Once
	singleton        T
	metricsRegistrar prometheus.Registerer
}

func NewLazyMetrics[T MetricsGroup](
	group T,
	metricsRegistrar prometheus.Registerer,
) *LazyMetrics[T] {
	return &LazyMetrics[T]{
		singleton:        group,
		metricsRegistrar: metricsRegistrar,
	}
}

func (m *LazyMetrics[T]) WithRegistrar(r prometheus.Registerer) *LazyMetrics[T] {
	m.metricsRegistrar = r
	return m
}

func (m *LazyMetrics[T]) Register() T {
	m.init.Do(func() {
		if m.metricsRegistrar != nil {
			m.singleton.RegisterMetrics(m.metricsRegistrar)
		}
	})
	return m.singleton
}
