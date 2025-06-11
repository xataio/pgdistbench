package tpcc

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"pgdistbench/api/benchdriverapi"
	"pgdistbench/internal/worker"
	"pgdistbench/internal/worker/gotpcutil"
	"pgdistbench/pkg/ctxutil"

	"github.com/pingcap/go-tpc/pkg/measurement"
	"github.com/pingcap/go-tpc/pkg/workload"
	"github.com/pingcap/go-tpc/tpcc"
)

// var tpccTestRunner = (*benchTPCC).runExec
var tpccTestRunner = (*Tester).runDirect

type Tester struct {
	db           *sql.DB
	workerConfig worker.Config

	mu        sync.Mutex
	liveStats func() []benchdriverapi.OpStats
}

func New(cfg worker.Config) (Tester, error) {
	db, err := worker.OpenDB(cfg)
	if err != nil {
		return Tester{}, err
	}

	return Tester{
		db:           db,
		workerConfig: cfg,
	}, nil
}

func (b *Tester) Close() error {
	return b.db.Close()
}

func (b *Tester) Ping(ctx context.Context) (bool, error) {
	err := b.db.PingContext(ctx)
	if err != nil {
		return false, err
	}
	return true, err
}

func (b *Tester) LiveStats() (stats []benchdriverapi.OpStats) {
	b.mu.Lock()
	fn := b.liveStats
	b.mu.Unlock()

	if fn == nil {
		return nil
	}
	return fn()
}

func (b *Tester) Prepare(ctx context.Context, cfg tpcc.Config, check bool) (err error) {
	ctx, cancel := ctxutil.WithFuncContext(ctx, func() {
		log.Printf("TPCC Prepare: stop signal received")
	})
	defer cancel()

	cfg.DBName = b.workerConfig.PGDatabase
	w, err := tpcc.NewWorkloader(b.db, &cfg)
	if err != nil {
		return err
	}

	err = gotpcutil.ExecThreadFuncs(context.Background(), w, cfg.Threads, func(threadCtx context.Context, index int) error {
		log.Printf("TPCC Prepare: prepare thread %d", index)
		defer log.Printf("TPCC Prepare: prepare thread %d finished", index)

		err := w.Prepare(threadCtx, index)
		if err != nil {
			return fmt.Errorf("prepare thread %d: %w", index, err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("prepare: %w", err)
	}

	if check && ctx.Err() == nil {
		err = gotpcutil.ExecThreadFuncs(ctx, w, cfg.Threads, func(ctx context.Context, index int) error {
			return w.CheckPrepare(ctx, index)
		})
		if err != nil {
			return fmt.Errorf("check prepare: %w", err)
		}
	}

	return ctx.Err()
}

func (b *Tester) Cleanup(ctx context.Context) (err error) {
	tpccConfig := tpcc.Config{
		Driver:        "postgres",
		DBName:        b.workerConfig.PGDatabase,
		Threads:       1,
		PartitionType: tpcc.PartitionTypeHash,
	}
	w, err := tpcc.NewWorkloader(b.db, &tpccConfig)
	if err != nil {
		return err
	}

	ctx = w.InitThread(ctx, 1)
	defer w.CleanupThread(ctx, 1)
	return w.Cleanup(ctx, 0)
}

func (b *Tester) Run(
	ctx context.Context, cfg tpcc.Config, count int, duration time.Duration, check bool,
) (summary benchdriverapi.TPCCRunStats, err error) {
	log.Printf("Running TPCC test with %d warehouses and %d threads, duration %s", cfg.Warehouses, cfg.Threads, duration)
	defer log.Printf("TPCC Run: finished")
	ctx, cancel := ctxutil.WithFuncContext(ctx, func() {
		log.Printf("TPCC Run: stop signal received")
	})
	defer cancel()

	cfg.DBName = b.workerConfig.PGDatabase
	w, err := tpcc.NewWorkloader(b.db, &cfg)
	if err != nil {
		return summary, err
	}

	if summary, err = tpccTestRunner(b, ctx, cfg, count, duration); err != nil {
		return summary, err
	}

	if check && ctx.Err() == nil {
		err = gotpcutil.ExecThreadFuncs(ctx, w, cfg.Threads, w.Check)
		if err != nil {
			return summary, fmt.Errorf("check: %w", err)
		}
	}
	return summary, nil
}

func (b *Tester) runDirect(ctx context.Context, cfg tpcc.Config, count int, duration time.Duration) (summary benchdriverapi.TPCCRunStats, err error) {
	w, err := tpcc.NewWorkloader(b.db, &cfg)
	if err != nil {
		return summary, err
	}

	liveStats := func() []benchdriverapi.OpStats {
		return LiveStatsFromWorkloader(w)
	}
	b.mu.Lock()
	if b.liveStats == nil {
		b.liveStats = liveStats
		defer func() {
			b.mu.Lock()
			defer b.mu.Unlock()
			b.liveStats = nil
		}()
	}
	b.mu.Unlock()

	if duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, duration)
		defer cancel()
	}

	err = gotpcutil.ExecThreadFuncs(ctx, w, cfg.Threads, func(ctx context.Context, index int) error {
		for i := 0; ctx.Err() == nil && (count <= 0 || i < count); i++ {
			err := w.Run(ctx, index)
			if err != nil {
				isTimeoutOrCancel := errors.Is(err, context.Canceled) ||
					errors.Is(err, context.DeadlineExceeded) ||

					// go-tpc does return unwrapp'able errors. We need to check the error message in this case.
					strings.Contains(err.Error(), context.Canceled.Error()) ||
					strings.Contains(err.Error(), context.DeadlineExceeded.Error()) ||

					// the connection might be closed due to the cancellation. Ignore that error as well if the context is marked as cancelled.
					(ctx.Err() != nil && strings.Contains(err.Error(), "connection is already closed"))

				if !isTimeoutOrCancel {
					log.Printf("Error in iteration %d: %v", i, err)
				}
			}
		}
		return nil
	})
	return StatsFromWorkloader(w, cfg.Warehouses), err
}

func (b *Tester) runExec(ctx context.Context, cfg tpcc.Config, count int, duration time.Duration) (summary benchdriverapi.TPCCRunStats, err error) {
	cmd := gotpcutil.NewCLIExec(b.workerConfig, gotpcutil.CmdTPCCRun,
		"-T", fmt.Sprintf("%d", cfg.Threads),
		"--parts", fmt.Sprintf("%d", cfg.Parts),
		"--partition-type", fmt.Sprintf("%d", cfg.PartitionType),
		"--warehouses", fmt.Sprintf("%d", cfg.Warehouses),
		"--isolation", fmt.Sprintf("%d", cfg.Isolation),
		"--count", fmt.Sprintf("%d", count),
		"--time", duration.String(),
		"--interval", (2 * duration).String(), // disable intermediate report by setting interval to 2x duration
	)
	if cfg.CheckAll {
		cmd.AddArgs("--check-all")
	}
	if cfg.Wait {
		cmd.AddArgs("--wait")
	}

	// We use the external go-tpc tool to run the benchmark. This makes it easier
	// for us to collect the test results.

	summary, err = gotpcutil.ExecGoTPC(cmd, ctx, parseTPCCRunStdout)
	if err != nil {
		return summary, err
	}

	return summary, nil
}

func parseTPCCRunStdout(ctx context.Context, r io.Reader) (summary benchdriverapi.TPCCRunStats, err error) {
	scanner := bufio.NewScanner(r)

	// skip input until end of test
	if err = gotpcutil.ScanFinished(scanner, gotpcutil.EmitLinesTo(os.Stderr)); err != nil {
		return summary, err
	}

	// read finished stats
	var statsDocs []benchdriverapi.OpStats
	var summaryDoc benchdriverapi.TPCCSummary
summaryLoop:
	for scanner.Scan() {
		line := scanner.Bytes()
		fmt.Println(string(line))

		var tmp []map[string]any
		if err := json.Unmarshal(line, &tmp); err != nil {
			return summary, fmt.Errorf("parse stats: %w", err)
		}

		// is last summary line?
		for _, doc := range tmp {
			if _, ok := doc["tpmC"]; ok {
				var docArray []benchdriverapi.TPCCSummary
				if err := json.Unmarshal(line, &docArray); err != nil {
					return summary, fmt.Errorf("parse summary: %w", err)
				}
				summaryDoc = docArray[0]
				break summaryLoop
			}
		}

		// measurement docs
		for _, doc := range tmp {
			count, _ := strconv.Atoi(doc["Count"].(string))
			sum, _ := strconv.ParseFloat(doc["Sum(ms)"].(string), 64)
			avg, _ := strconv.ParseFloat(doc["Avg(ms)"].(string), 64)
			p50, _ := strconv.ParseFloat(doc["P50(ms)"].(string), 64)
			max, _ := strconv.ParseFloat(doc["Max(ms)"].(string), 64)
			statsDocs = append(statsDocs, benchdriverapi.OpStats{
				Prefix:    doc["Prefix"].(string),
				Operation: doc["Operation"].(string),
				Count:     int64(count),
				Sum:       sum,
				Avg:       avg,
				Median:    p50,
				Max:       max,
			})
		}
	}
	err = scanner.Err()

	return benchdriverapi.TPCCRunStats{
		Stats:   statsDocs,
		Summary: summaryDoc,
	}, err
}

func LiveStatsFromWorkloader(w workload.Workloader) []benchdriverapi.OpStats {
	stats := w.Stats()
	return gotpcutil.ReadOpStats(stats, false)
}

func StatsFromWorkloader(w workload.Workloader, warehouses int) benchdriverapi.TPCCRunStats {
	stats := w.Stats()
	opStats := gotpcutil.ReadOpStats(stats, true)

	var (
		newOrderHist *measurement.Histogram
		totalOps     float64
		summaryDoc   benchdriverapi.TPCCSummary
	)
	for name, hist := range stats.OpSumMeasurement {
		if name == "new_order" {
			newOrderHist = hist
		}
		if !strings.HasSuffix(name, "_ERR") {
			totalOps += hist.GetInfo().Ops
		}
	}
	if newOrderHist != nil && !newOrderHist.Empty() {
		result := newOrderHist.GetInfo()
		const specWarehouseFactor = 12.86
		tpmC := result.Ops * 60
		tpmTotal := totalOps * 60
		efc := 100 * tpmC / (specWarehouseFactor * float64(warehouses))

		summaryDoc = benchdriverapi.TPCCSummary{
			Efficiency: benchdriverapi.Percentage(efc),
			TPMC:       benchdriverapi.Sample(tpmC),
			TPMTotal:   benchdriverapi.Sample(tpmTotal),
		}
	}

	return benchdriverapi.TPCCRunStats{
		Stats:   opStats,
		Summary: summaryDoc,
	}
}

func TPCCConfigFromAPI(cfg *benchdriverapi.BenchmarkGoTPCCConfig) (tc tpcc.Config, err error) {
	errs := make([]error, 0, 2)
	isolation, err := gotpcutil.IsolationFromAPI(benchdriverapi.GetOptValue(cfg.IsolationLevel, benchdriverapi.IsolationLevelDefault))
	errs = append(errs, err)
	partitionType, err := gotpcutil.PartitionTypeFromAPI(benchdriverapi.GetOptValue(cfg.PartitionType, benchdriverapi.PartitionHash))
	errs = append(errs, err)
	if err := errors.Join(errs...); err != nil {
		return tc, err
	}

	warehouses := benchdriverapi.GetOptValue(cfg.Warehouses, 1)
	return tpcc.Config{
		Driver:        "postgres",
		Isolation:     int(isolation),
		Parts:         benchdriverapi.GetOptValue(cfg.Partitions, 1),
		PartitionType: partitionType,
		CheckAll:      benchdriverapi.GetOptValue(cfg.Check, false),
		UseFK:         benchdriverapi.GetOptValue(cfg.UseForeignKeys, true),
		Wait:          benchdriverapi.GetOptValue(cfg.WaitThinking, true),
		Warehouses:    warehouses,
		Threads:       benchdriverapi.GetOptValue(cfg.Threads, warehouses*10),
		OutputStyle:   "json",
	}, nil
}
