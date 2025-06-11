package k8stress

import (
	"pgdistbench/api/benchdriverapi"
	"pgdistbench/pkg/stats"

	"github.com/prometheus/client_golang/prometheus"
)

type stressMetrics struct {
	Setup   setupMetrics
	Prepare prepareMetrics
	Run     runMetrics
}

func (m *stressMetrics) RegisterMetrics(r prometheus.Registerer) {
	m.Setup.Register(r)
	m.Prepare.Register(r)
	m.Run.Register(r)
}

type setupMetrics struct{}

func (m *setupMetrics) Register(r prometheus.Registerer) {
}

type prepareMetrics struct{}

func (m *prepareMetrics) Register(r prometheus.Registerer) {
}

type runMetrics struct {
	ActiveUsers prometheus.Gauge
	UsersErrors prometheus.Counter

	CreateInstanceOps *prometheus.CounterVec
	DeleteInstanceOps *prometheus.CounterVec
	ActiveInstances   *prometheus.GaugeVec
	OpPanics          *prometheus.CounterVec
	OpErrors          *prometheus.CounterVec

	// number of active stressInstance runners
	ActiveTestRunners *prometheus.GaugeVec

	// Per Postgres cluster run metrics
	ClusterDBPrepare    *prometheus.CounterVec
	ClusterDBRun        *prometheus.CounterVec
	ErrClusterCreate    *prometheus.CounterVec
	ErrClusterDelete    *prometheus.CounterVec
	ErrClusterPrepare   *prometheus.CounterVec
	ErrClusterDBPrepare *prometheus.CounterVec
	ErrClusterDBRun     *prometheus.CounterVec

	// TPCC benchmark metrics
	SummaryStats TPCCSummaryStats
	LiveStats    TPCCLiveStats
}

func (m *runMetrics) Register(r prometheus.Registerer) {
	m.ActiveUsers = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "k8stress_active_users",
		Help: "Number of active users driving the stress test",
	})
	r.MustRegister(m.ActiveUsers)

	m.UsersErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "k8stress_users_errors",
		Help: "Number of user processes stopped due to errors",
	})
	r.MustRegister(m.UsersErrors)

	m.CreateInstanceOps = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "k8stress_planned_op_create_total",
		Help: "Total number of panned cluster creation operations",
	}, []string{"user"})
	r.MustRegister(m.CreateInstanceOps)

	m.DeleteInstanceOps = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "k8stress_planned_op_delete_total",
		Help: "Total number of planned cluster deletion operations",
	}, []string{"user"})
	r.MustRegister(m.DeleteInstanceOps)

	m.ActiveInstances = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "k8stress_test_active",
		Help: "Number of active test runners creating instances and running benchmarks",
	}, []string{"user"})
	r.MustRegister(m.ActiveInstances)

	m.OpPanics = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "k8stress_planned_op_panic_total",
		Help: "Total number of go panics caught during test runner create/delete operations",
	}, []string{"user", "op"})
	r.MustRegister(m.OpPanics)

	m.OpErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "k8stress_planned_op_error_total",
		Help: "Total number of errors caught during test runner create/delete operations",
	}, []string{"user", "op"})
	r.MustRegister(m.OpErrors)

	m.ActiveTestRunners = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "k8stress_active_test_runners",
		Help: "Number of active stress test runners",
	}, []string{"name"})
	r.MustRegister(m.ActiveTestRunners)

	m.ClusterDBPrepare = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "k8stress_cluster_db_prepare_total",
		Help: "Total number of cluster prepare operations",
	}, []string{"user"})
	r.MustRegister(m.ClusterDBPrepare)

	m.ClusterDBRun = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "k8stress_cluster_db_run_total",
		Help: "Total number of cluster run operations",
	}, []string{"user"})
	r.MustRegister(m.ClusterDBRun)

	m.ErrClusterCreate = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "k8stress_cluster_create_error_total",
		Help: "Total number of cluster create errors",
	}, []string{"user"})
	r.MustRegister(m.ErrClusterCreate)

	m.ErrClusterDelete = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "k8stress_cluster_delete_error_total",
		Help: "Total number of cluster delete errors",
	}, []string{"user"})
	r.MustRegister(m.ErrClusterDelete)

	m.ErrClusterPrepare = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "k8stress_cluster_prepare_error_total",
		Help: "Total number of cluster prepare errors",
	}, []string{"user"})
	r.MustRegister(m.ErrClusterPrepare)

	m.ErrClusterDBPrepare = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "k8stress_cluster_db_prepare_error_total",
		Help: "Total number of cluster db prepare errors",
	}, []string{"user"})
	r.MustRegister(m.ErrClusterDBPrepare)

	m.ErrClusterDBRun = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "k8stress_cluster_db_run_error_total",
		Help: "Total number of cluster db run errors",
	}, []string{"user"})
	r.MustRegister(m.ErrClusterDBRun)

	m.SummaryStats.Register(r)
	m.LiveStats.Register(r)
}

type TPCCSummaryStats struct {
	Efficiency      *prometheus.HistogramVec
	TPMC            *prometheus.HistogramVec
	TPMTotal        *prometheus.HistogramVec
	OpMetricsCount  *prometheus.HistogramVec
	OpMetricsAvg    *prometheus.HistogramVec
	OpMetricsMedian *prometheus.HistogramVec
	OpMetricsMax    *prometheus.HistogramVec
}

func (m *TPCCSummaryStats) Register(r prometheus.Registerer) {
	tpmBuckets := stats.ExpBuckets(1, 1.3, 200000)

	name := func(n string) string {
		return "k8stress_summary_tpcc_" + n
	}

	m.Efficiency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    name("efficiency"),
		Help:    "TPCC efficiency",
		Buckets: prometheus.LinearBuckets(0, 5, 100/5), // Up to 100% in 5% increments
	}, []string{"user"})
	r.MustRegister(m.Efficiency)

	m.TPMC = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    name("tpmc"),
		Help:    "TPCC tpmC",
		Buckets: tpmBuckets,
	}, []string{"user"})
	r.MustRegister(m.TPMC)

	m.TPMTotal = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    name("tpm_total"),
		Help:    "TPCC total tpm",
		Buckets: tpmBuckets,
	}, []string{"user"})
	r.MustRegister(m.TPMTotal)

	opLabels := []string{"user", "op"}

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
	opLabels := []string{"user", "op"}

	tpmBuckets := stats.ExpBuckets(1, 1.3, 200000)

	name := func(n string) string {
		return "k8stress_live_tpcc_" + n
	}

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

func (m *TPCCLiveStats) Observe(stats []benchdriverapi.OpStats, user string) {
	for _, op := range stats {
		labels := []string{user, op.Operation}
		m.OpMetricsCount.WithLabelValues(labels...).Observe(float64(op.Count))
		m.OpMetricsAvg.WithLabelValues(labels...).Observe(float64(op.Avg))
		m.OpMetricsMedian.WithLabelValues(labels...).Observe(float64(op.Median))
		m.OpMetricsMax.WithLabelValues(labels...).Observe(float64(op.Max))
	}
}
