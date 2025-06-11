package tpch

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
	"strings"
	"time"

	"github.com/pingcap/go-tpc/tpch"

	"pgdistbench/api/benchdriverapi"
	"pgdistbench/internal/worker"
	"pgdistbench/internal/worker/gotpcutil"
)

var defaultQueries = []string{
	"q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q10",
	"q11", "q12", "q13", "q14", "q15", "q16", "q17", "q18", "q19", "q20",
	"q21", "q22",
}

type Tester struct {
	db           *sql.DB
	workerConfig worker.Config
}

func New(cfg worker.Config) (Tester, error) {
	db, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s", cfg.PGHost, cfg.PGPort, cfg.PGUser, cfg.PGPass, cfg.PGDatabase, cfg.PGSSLMode))
	if err != nil {
		return Tester{}, err
	}
	return Tester{db: db, workerConfig: cfg}, nil
}

func (b *Tester) Close() error {
	return b.db.Close()
}

func (b *Tester) Prepare(ctx context.Context, cfg tpch.Config, dropData bool) (err error) {
	cfg.DBName = b.workerConfig.PGDatabase
	w := tpch.NewWorkloader(b.db, &cfg)
	return gotpcutil.ExecThreadFuncs(ctx, w, 1, func(ctx context.Context, threadID int) error {
		if dropData {
			if err := w.Cleanup(ctx, threadID); err != nil {
				return fmt.Errorf("drop data: %w", err)
			}
		}
		return w.Prepare(ctx, threadID)
	})
}

func (b *Tester) Cleanup(ctx context.Context) (err error) {
	tpchConfig := tpch.Config{
		Driver:     "postgres",
		DBName:     b.workerConfig.PGDatabase,
		QueryNames: defaultQueries,
	}
	w := tpch.NewWorkloader(b.db, &tpchConfig)
	ctx = w.InitThread(ctx, 1)
	defer w.CleanupThread(ctx, 1)
	return w.Cleanup(ctx, 0)
}

func (b *Tester) Run(
	ctx context.Context, cfg tpch.Config, count int,
) (stats benchdriverapi.TPCHRunStats, err error) {
	queries := defaultQueries
	if len(cfg.QueryNames) > 0 {
		queries = cfg.QueryNames
	}

	statsTable := map[string]benchdriverapi.QueryStats{}
	for i := 0; i < count; i++ {
		for _, q := range queries {
			d, err := b.runQuery(ctx, cfg, q)
			if err != nil {
				return stats, err
			}

			if d == 0 {
				log.Printf("query %s returned 0 duration", q)
				continue
			}

			stat := statsTable[q]
			stat.Query = q
			stat.Count++
			stat.Measurements = append(stat.Measurements, benchdriverapi.Duration{d})
			statsTable[q] = stat
		}
	}

	lst := make([]benchdriverapi.QueryStats, 0, len(statsTable))
	for _, q := range queries {
		lst = append(lst, statsTable[q])
	}

	return benchdriverapi.TPCHRunStats{List: lst}, nil
}

func (b *Tester) runQuery(ctx context.Context, cfg tpch.Config, query string) (time.Duration, error) {
	// return b.runQueryExec(ctx, cfg, query)
	return b.runQueryDirect(ctx, cfg, query)
}

func (b *Tester) runQueryDirect(ctx context.Context, cfg tpch.Config, query string) (time.Duration, error) {
	cfg.QueryNames = []string{query}
	cfg.RawQueries = query
	w := tpch.NewWorkloader(b.db, &cfg)
	err := w.Exec(`
		create or replace view revenue0 (supplier_no, total_revenue) as
		select
			l_suppkey,
			sum(l_extendedprice * (1 - l_discount))
		from
			lineitem
		where
			l_shipdate >= '1997-07-01'
			and l_shipdate < date_add('1997-07-01', interval '3' month)
		group by
			l_suppkey;`,
	)
	if err != nil {
		return 0, err
	}

	tries := 0
	for {
		ctx = w.InitThread(ctx, 1)
		defer w.CleanupThread(ctx, 1)
		if err = w.Run(ctx, 0); err != nil {
			return 0, err
		}

		stats := w.Stats()
		var d time.Duration
		if m, ok := stats.OpSumMeasurement[query]; ok {
			// Time is measures in ms using a float64. Convert to time.Duration.
			d = time.Duration(int64(m.GetInfo().Sum * float64(time.Millisecond)))
			fmt.Printf("extract time: query %s: %s\n", query, d)
		}

		if d == 0 && tries < 3 {
			tries += 1
			continue
		}
		return d, nil
	}
}

func (b *Tester) runQueryExec(ctx context.Context, cfg tpch.Config, query string) (time.Duration, error) {
	cmd := gotpcutil.NewCLIExec(b.workerConfig, gotpcutil.CmdTPCHRUn,
		"--count", "1",
		"--queries", strings.ToLower(query),
		"--sf", fmt.Sprintf("%d", cfg.ScaleFactor),
	)
	if cfg.EnableQueryTuning {
		cmd.AddArgs("--enable-query-tuning")
	}
	if cfg.EnableOutputCheck {
		cmd.AddArgs("--check")
	}

	return gotpcutil.ExecGoTPC(cmd, ctx, func(ctx context.Context, r io.Reader) (time.Duration, error) {
		type tpchStat struct {
			Avg       string `json:"Avg(s)"`
			Operation string `json:"Operation"`
			Prefix    string `json:"Prefix"`
		}

		scanner := bufio.NewScanner(r)
		if err := gotpcutil.ScanFinished(scanner, gotpcutil.EmitLinesTo(os.Stderr)); err != nil {
			return 0, err
		}

		for ctx.Err() == nil && scanner.Scan() {
			var stats []tpchStat
			line := scanner.Bytes()
			if err := json.Unmarshal(line, &stats); err != nil {
				if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
					return 0, err
				}
				log.Printf("failed to unmarshal line: %s", line)
				continue
			}

			for _, stat := range stats {
				if !strings.EqualFold(stat.Operation, query) {
					continue
				}
				if !strings.Contains(stat.Prefix, "Summary") {
					continue
				}

				d, err := time.ParseDuration(stat.Avg)
				if err != nil {
					return 0, fmt.Errorf("failed to parse duration %s: %w", stat.Avg, err)
				}

				return d, nil
			}
		}
		if err := scanner.Err(); err != nil {
			return 0, err
		}

		return 0, fmt.Errorf("query %s not found", query)
	})
}

func TPCHConfigFromAPI(req *benchdriverapi.BenchmarkGoTPCHConfig) (tpch.Config, error) {
	queries := OLAPQueryList(req.Queries)
	cfg := tpch.Config{
		Driver:             "postgres",
		RawQueries:         strings.Join(queries, ","),
		QueryNames:         queries,
		ScaleFactor:        benchdriverapi.GetOptValue(req.ScaleFactor, 1),
		EnableOutputCheck:  benchdriverapi.GetOptValue(req.Check, false),
		ExecExplainAnalyze: false,
		PrepareThreads:     8,
		EnablePlanReplayer: false,
		EnableQueryTuning:  benchdriverapi.GetOptValue(req.EnableQueryTuning, false),
	}
	if benchdriverapi.GetOptValue(req.Analyze, true) {
		cfg.AnalyzeTable.Enable = true
	}
	return cfg, nil
}

func OLAPQueryList(userConfig []string) []string {
	if len(userConfig) == 0 {
		return defaultQueries
	}

	seen := make(map[string]struct{}, len(userConfig))
	uniq := make([]string, 0, min(len(defaultQueries), len(userConfig)))
	for _, q := range userConfig {
		q = strings.TrimSpace(strings.ToLower(q))
		if _, ok := seen[q]; !ok {
			uniq = append(uniq, q)
			seen[q] = struct{}{}
		}
	}
	return uniq
}
