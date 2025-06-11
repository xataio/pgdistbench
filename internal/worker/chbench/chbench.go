package chbench

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/go-tpc/ch"
	"github.com/pingcap/go-tpc/pkg/workload"
	"github.com/pingcap/go-tpc/tpcc"
	"golang.org/x/sync/errgroup"

	"pgdistbench/api/benchdriverapi"
	"pgdistbench/internal/worker"
	"pgdistbench/internal/worker/gotpcutil"
	tpccworker "pgdistbench/internal/worker/tpcc"
	tpchworker "pgdistbench/internal/worker/tpch"
	"pgdistbench/pkg/stats"
)

type Tester struct {
	db           *sql.DB
	workerConfig worker.Config
}

type RunConfig struct {
	TPCC        tpcc.Config
	OLAP        ch.Config
	OlapThreads int
	Check       bool

	Duration      time.Duration
	WaitOLAP      bool
	WaitOLAPCount int
}

func newBenchCHBench(cfg worker.Config) (Tester, error) {
	db, err := worker.OpenDB(cfg)
	if err != nil {
		return Tester{}, err
	}
	if err := db.Ping(); err != nil {
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

func (b *Tester) Prepare(ctx context.Context, tpccConfig tpcc.Config, olapConfig ch.Config, check, dropData bool) (err error) {
	if dropData {
		if err := b.cleanupOLAPTables(ctx); err != nil {
			return nil
		}
	}

	// prepare TPCC based schema
	tpccConfig.DBName = b.workerConfig.PGDatabase
	tpccWorkloader, err := tpcc.NewWorkloader(b.db, &tpccConfig)
	if err != nil {
		return err
	}

	start := time.Now()
	log.Println("Prepare OLTP data")
	err = gotpcutil.ExecThreadFuncs(ctx, tpccWorkloader, tpccConfig.Threads, func(ctx context.Context, threadID int) error {
		if dropData {
			if err := tpccWorkloader.Cleanup(ctx, threadID); err != nil {
				return fmt.Errorf("drop data: %w", err)
			}
		}
		return tpccWorkloader.Prepare(ctx, threadID)
	})
	if err != nil {
		return fmt.Errorf("prepare OLTP schema: %w", err)
	}
	log.Printf("Done: Prepare OLTP data. took=%v\n", time.Since(start))

	// Add OLAP schema
	olapConfig.DBName = b.workerConfig.PGDatabase
	olapWorkloader := ch.NewWorkloader(b.db, &olapConfig)
	log.Println("Prepare OLAP data")
	start = time.Now()
	err = gotpcutil.ExecThreadFuncs(ctx, olapWorkloader, 1, func(ctx context.Context, threadID int) error {
		return olapWorkloader.Prepare(ctx, threadID)
	})
	if err != nil {
		return fmt.Errorf("prepare OLAP data: %w", err)
	}
	log.Printf("Done: Prepare OLAP data. took=%v\n", time.Since(start))

	if check {
		err = gotpcutil.ExecThreadFuncs(ctx, tpccWorkloader, tpccConfig.Threads, func(ctx context.Context, index int) error {
			return tpccWorkloader.CheckPrepare(ctx, index)
		})
		if err != nil {
			return fmt.Errorf("check prepare: %w", err)
		}
	}

	return nil
}

func (b *Tester) Cleanup(ctx context.Context) error {
	if err := b.cleanupOLAPTables(ctx); err != nil {
		return fmt.Errorf("cleanup OLAP tables: %w", err)
	}

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

func (b *Tester) cleanupOLAPTables(ctx context.Context) error {
	conn, err := b.db.Conn(ctx)
	if err != nil {
		return err
	}

	for _, view := range []string{"revenue0", "revenue1"} {
		_, err := conn.ExecContext(ctx, fmt.Sprintf("DROP VIEW IF EXISTS %s CASCADE", view))
		if err != nil {
			return fmt.Errorf("drop view %s: %w", view, err)
		}
	}

	for _, table := range []string{"lineitem", "orders", "part", "partsupp", "supplier", "customer", "nation", "region"} {
		_, err := conn.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", table))
		if err != nil {
			return fmt.Errorf("drop table %s: %w", table, err)
		}
	}

	return nil
}

func (b *Tester) Run(
	ctx context.Context,
	cfg RunConfig,
) (summary benchdriverapi.CHBenchRunStats, err error) {
	cfg.TPCC.DBName = b.workerConfig.PGDatabase
	cfg.OLAP.DBName = b.workerConfig.PGDatabase
	olapWorkloader := ch.NewWorkloader(b.db, &cfg.OLAP)
	tpccWorkloader, err := tpcc.NewWorkloader(b.db, &cfg.TPCC)
	if err != nil {
		return summary, err
	}

	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()

	eg, runCtx := errgroup.WithContext(runCtx)
	if !cfg.WaitOLAP && cfg.WaitOLAPCount <= 0 {
		var cancel context.CancelFunc
		runCtx, cancel = context.WithTimeout(runCtx, cfg.Duration)
		defer cancel()
	}

	// OLTP
	eg.Go(func() (err error) {
		return gotpcutil.ExecThreadFuncs(runCtx, tpccWorkloader, cfg.TPCC.Threads, func(ctx context.Context, index int) error {
			if err := tpccWorkloader.Run(ctx, index); err != nil {
				return fmt.Errorf("run: %w", err)
			}
			return nil
		})
	})

	// OLAP. Use 'channel' to trigger work to directly distribute the work to available threads.
	workDist := make(chan struct{}, cfg.OlapThreads)
	var olapErrors atomic.Int64

	var wgOLAP sync.WaitGroup
	eg.Go(func() error {
		return gotpcutil.ExecThreadFuncs(runCtx, olapWorkloader, cfg.OlapThreads, func(ctx context.Context, index int) error {
			for range workDist {
				if ctx.Err() != nil {
					break
				}

				func() {
					defer wgOLAP.Done()
					if err := olapWorkloader.Run(ctx, index); err != nil {
						fmt.Fprintln(os.Stderr, "OLAP query error:", err)
						olapErrors.Add(1)
					}
				}()
			}
			return nil
		})
	})

	// Controller. Responsible for timing and stopping the test run.
	eg.Go(func() error {
		workDist := workDist
		defer func() {
			if workDist != nil {
				close(workDist)
			}
		}()

		end := time.Now().Add(cfg.Duration)
		timeoutCtx, cancel := context.WithDeadline(runCtx, end)
		defer cancel()

		olapCount := -1
		if cfg.WaitOLAPCount > 0 {
			olapCount = cfg.WaitOLAPCount
		}

		timeoutCh := timeoutCtx.Done()
		if !cfg.WaitOLAP {
			timeoutCh = nil
		}

		for i := 0; i < olapCount || olapCount < 0; i++ {
			wgOLAP.Add(1)
			select {
			case <-runCtx.Done():
				wgOLAP.Done()
				return nil
			case <-timeoutCh:
				wgOLAP.Done()
				olapCount = 0
				if cfg.WaitOLAP {
					olapCount = (i / len(cfg.OLAP.QueryNames)) * len(cfg.OLAP.QueryNames)
					if olapCount < i {
						olapCount += len(cfg.OLAP.QueryNames)
					}
					timeoutCh = nil
				}
			case workDist <- struct{}{}:
			}
		}
		close(workDist)
		workDist = nil

		if runCtx.Err() == nil && cfg.WaitOLAP {
			wgOLAP.Wait()
		}
		if timeoutCtx.Err() != nil {
			runCancel()
		}

		return nil
	})

	if err := eg.Wait(); err != nil {
		return summary, err
	}

	return benchdriverapi.CHBenchRunStats{
		OLTPStats: tpccworker.StatsFromWorkloader(tpccWorkloader, cfg.TPCC.Warehouses),
		OLAPStats: olapQueryStatsFromWorkloader(olapWorkloader),
	}, nil
}

func (b *Tester) runOLAP(ctx context.Context, workloader workload.Workloader) ([]benchdriverapi.QueryStats, error) {
	// workloader advances to next query on each call to run.

	panic("implement me")
}

func CHBenchConfigFromAPI(req *benchdriverapi.BenchmarkCHBenchConfig) (ch.Config, error) {
	queries := tpchworker.OLAPQueryList(req.Queries)
	cfg := ch.Config{
		Driver:     "postgres",
		RawQueries: strings.Join(queries, ","),
		QueryNames: queries,
	}
	if benchdriverapi.GetOptValue(req.Analyze, true) {
		cfg.AnalyzeTable.Enable = true
	}
	cfg.OutputStyle = "json"
	return cfg, nil
}

func TPCCConfigFromCHBenchAPI(cfg *benchdriverapi.BenchmarkCHBenchConfig) (tc tpcc.Config, err error) {
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
		Wait:          false,
		Warehouses:    warehouses,
		Threads:       benchdriverapi.GetOptValue(cfg.Threads, warehouses*10),
		OutputStyle:   "json",
	}, nil
}

func olapQueryStatsFromWorkloader(w workload.Workloader) benchdriverapi.CHBenchOlapStats {
	measurements := w.Stats()
	rtStats := gotpcutil.ReadOpStats(measurements, true)

	// Extract OLAP based performance metrics. See: https://pdfs.semanticscholar.org/835d/ad44179bf856c63b98cc1440b60cd6a3283a.pdf

	useAvg := func(s benchdriverapi.OpStats) float64 { return s.Avg }
	geom := stats.SlicesGeometricMeanFunc(rtStats, useAvg)
	querySetDuration := stats.SlicesSumOfFunc(rtStats, useAvg) / 1000.0

	var count int64
	var sum float64
	var queriesPerHour float64
	for _, m := range measurements.OpSumMeasurement {
		if !m.Empty() {
			r := m.GetInfo()
			count += r.Count
			sum += r.Sum
		}
	}
	if sum != 0 && count != 0 {
		queriesPerHour = 3600 * 1000 / sum * float64(count)
	}

	return benchdriverapi.CHBenchOlapStats{
		Queries:          rtStats,
		GeometricMean:    geom,
		QuerySetDuration: querySetDuration,
		QueryPerHour:     queriesPerHour,
	}
}
