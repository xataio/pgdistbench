package tpcc

import (
	"pgdistbench/api/benchdriverapi"
	"pgdistbench/pkg/stats"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

type TPCCSummaryStats struct {
	Efficiency      prometheus.Histogram
	TPMC            prometheus.Histogram
	TPMTotal        prometheus.Histogram
	OpMetricsCount  *prometheus.HistogramVec
	OpMetricsAvg    *prometheus.HistogramVec
	OpMetricsMedian *prometheus.HistogramVec
	OpMetricsMax    *prometheus.HistogramVec
}

func (m *TPCCSummaryStats) Register(r prometheus.Registerer) {
	name := func(n string) string { return "tpcc_summary_" + n }
	tpmBuckets := stats.ExpBuckets(1, 1.3, 200000)

	m.Efficiency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    name("efficiency"),
		Help:    "TPCC efficiency",
		Buckets: prometheus.LinearBuckets(0, 5, 100/5), // Up to 100% in 5% increments
	})
	r.MustRegister(m.Efficiency)

	m.TPMC = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    name("tpmc"),
		Help:    "TPCC tpmC",
		Buckets: tpmBuckets,
	})
	r.MustRegister(m.TPMC)

	m.TPMTotal = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    name("tpm_total"),
		Help:    "TPCC total tpm",
		Buckets: tpmBuckets,
	})
	r.MustRegister(m.TPMTotal)

	opLabels := []string{"op"}

	m.OpMetricsCount = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    name("op_count"),
		Help:    "TPCC operation metrics",
		Buckets: tpmBuckets,
	}, opLabels)
	r.MustRegister(m.OpMetricsCount)

	m.OpMetricsAvg = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    name("op_avg"),
		Help:    "TPCC operation metrics",
		Buckets: tpmBuckets,
	}, opLabels)
	r.MustRegister(m.OpMetricsAvg)

	m.OpMetricsMedian = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    name("op_median"),
		Help:    "TPCC operation metrics",
		Buckets: tpmBuckets,
	}, opLabels)
	r.MustRegister(m.OpMetricsMedian)

	m.OpMetricsMax = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    name("op_max"),
		Help:    "TPCC operation metrics",
		Buckets: tpmBuckets,
	}, opLabels)
	r.MustRegister(m.OpMetricsMax)
}

type TPCCLiveStats struct {
	OpMetricsCount  *prometheus.HistogramVec
	OpMetricsAvg    *prometheus.HistogramVec
	OpMetricsMedian *prometheus.HistogramVec
	OpMetricsMax    *prometheus.HistogramVec
}

func (m *TPCCLiveStats) Register(r prometheus.Registerer) {
	opLabels := []string{"op"}

	tpmBuckets := stats.ExpBuckets(1, 1.3, 200000)
	name := func(n string) string { return "tpcc_live_" + n }

	m.OpMetricsCount = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    name("op_count"),
		Help:    "TPCC operation metrics",
		Buckets: tpmBuckets,
	}, opLabels)
	r.MustRegister(m.OpMetricsCount)

	m.OpMetricsAvg = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    name("op_avg"),
		Help:    "TPCC operation metrics",
		Buckets: tpmBuckets,
	}, opLabels)
	r.MustRegister(m.OpMetricsAvg)

	m.OpMetricsMedian = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    name("op_median"),
		Help:    "TPCC operation metrics",
		Buckets: tpmBuckets,
	}, opLabels)
	r.MustRegister(m.OpMetricsMedian)

	m.OpMetricsMax = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    name("op_max"),
		Help:    "TPCC operation metrics",
		Buckets: tpmBuckets,
	}, opLabels)
	r.MustRegister(m.OpMetricsMax)
}

func (m *TPCCLiveStats) Observe(stats []benchdriverapi.OpStats) {
	for _, op := range stats {
		labels := []string{op.Operation}
		m.OpMetricsCount.WithLabelValues(labels...).Observe(float64(op.Count))
		m.OpMetricsAvg.WithLabelValues(labels...).Observe(float64(op.Avg))
		m.OpMetricsMedian.WithLabelValues(labels...).Observe(float64(op.Median))
		m.OpMetricsMax.WithLabelValues(labels...).Observe(float64(op.Max))
	}
}

type tpccMetrics struct {
	Prepare managementOps
	Cleanup managementOps
	Run     runMetrics
}

func (m *tpccMetrics) RegisterMetrics(r prometheus.Registerer) {
	m.Prepare.Register(r, "prepare", "tpcc_prepare_")
	m.Cleanup.Register(r, "cleanup", "tpcc_cleanup_")
	m.Run.Register(r)
}

type managementOps struct {
	Ok       prometheus.Counter
	Err      prometheus.Counter
	Duration prometheus.Histogram
}

func (m *managementOps) Register(r prometheus.Registerer, operation string, prefix string) {
	if prefix == "" {
		panic("prefix is empty")
	}
	if !strings.HasSuffix(prefix, "_") {
		prefix = prefix + "_"
	}

	m.Ok = prometheus.NewCounter(prometheus.CounterOpts{
		Name: prefix + "ok",
		Help: "Total number of " + operation + " operations",
	})
	r.MustRegister(m.Ok)

	m.Err = prometheus.NewCounter(prometheus.CounterOpts{
		Name: prefix + "err",
		Help: "Total number of failed " + operation + " operations",
	})
	r.MustRegister(m.Err)

	m.Duration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: prefix + "duration",
		Help: "Duration of " + operation + " operations",
	})
	r.MustRegister(m.Duration)
}

type runMetrics struct {
	LiveStats    TPCCLiveStats
	SummaryStats TPCCSummaryStats
}

func (m *runMetrics) Register(r prometheus.Registerer) {
	m.LiveStats.Register(r)
	m.SummaryStats.Register(r)
}
