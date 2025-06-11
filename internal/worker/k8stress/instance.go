package k8stress

import (
	"context"
	"errors"
	"fmt"
	"log"
	"runtime/debug"
	"sync"
	"time"

	"pgdistbench/api/benchdriverapi"
	"pgdistbench/internal/worker"
	"pgdistbench/internal/worker/tpcc"
	"pgdistbench/pkg/client/systems"
	"pgdistbench/pkg/client/systems/syscnpg"
	"pgdistbench/pkg/cnpgutil"
	"pgdistbench/pkg/ctxutil"
	"pgdistbench/pkg/timeutil"

	"github.com/cenkalti/backoff/v5"
	cnpgapi "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type stressInstance struct {
	name     string
	cancel   context.CancelFunc
	owner    *stressUser
	config   *benchdriverapi.K8StressPostgresConfig
	registry *stressInstanceRegistry

	mu         sync.Mutex
	createTime time.Time
	readyTime  time.Time
}

func (si *stressInstance) mustDelete() bool {
	maxLifetime := si.config.MaxLifetime
	if maxLifetime == nil || maxLifetime.Duration <= 0 {
		return false
	}
	return time.Since(si.createTime) >= maxLifetime.Duration
}

func (si *stressInstance) canDelete() bool {
	min := si.config.MinLifetime
	if min == nil || min.Duration <= 0 {
		return true
	}

	si.mu.Lock()
	readyTime := si.readyTime
	defer si.mu.Unlock()

	if readyTime.IsZero() {
		return false
	}
	return time.Since(readyTime) >= min.Duration
}

type stressInstanceRegistry struct {
	mu        sync.Mutex
	wg        sync.WaitGroup
	instances map[string]*stressInstance
	starting  map[string]struct{}
	active    map[string]struct{}
}

func newStressInstanceRegistry() *stressInstanceRegistry {
	return &stressInstanceRegistry{
		instances: map[string]*stressInstance{},
		starting:  map[string]struct{}{},
		active:    map[string]struct{}{},
	}
}

func (r *stressInstanceRegistry) CancelAll() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, si := range r.instances {
		si.cancel()
	}
}

func (r *stressInstanceRegistry) Wait() {
	r.wg.Wait()
}

func (r *stressInstanceRegistry) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, si := range r.instances {
		si.cancel()
	}
	r.instances = map[string]*stressInstance{}
	r.starting = map[string]struct{}{}
	r.active = map[string]struct{}{}
}

const LabelStressTestInstance = "bench.maki.tech/stress-test-instance"

var durationInfinite = benchdriverapi.Duration{Duration: 0} // infinite

func (si *stressInstance) Cancel() {
	si.cancel()
}

func (si *stressInstance) Start(ctx context.Context) {
	si.registry.wg.Add(1)
	go func() {
		defer si.registry.wg.Done()

		if err := si.Run(ctx, clusterOpCreate); err != nil {
			log.Printf("ERROR Instance %s: running stress instance: %v", si.name, err)
		}
	}()
}

func (si *stressInstance) Run(ctx context.Context, op clusterOp) (err error) {
	metrics := &si.owner.tester.metrics.Run

	liveStatsPeriod := benchdriverapi.GetOptValue(si.config.Test.LiveStatsPeriod, benchdriverapi.Duration{Duration: 10 * time.Second}).Duration
	liveMinAge := benchdriverapi.GetOptValue(si.config.Test.LiveMinAge, benchdriverapi.Duration{Duration: 1 * time.Minute}).Duration

	activeRunners := si.owner.tester.metrics.Run.ActiveTestRunners.WithLabelValues(si.owner.name)
	activeRunners.Inc()
	defer activeRunners.Dec()
	defer si.deregister()

	log.Printf("Starting stress instance %s", si.name)
	defer log.Printf("Stopped stress instance %s", si.name)
	ctx, cancel := ctxutil.WithFuncContext(ctx, func() {
		log.Printf("Instance %s: stop signal received", si.name)
	})
	defer cancel()

	defer func() {
		err := ctx.Err()
		if r := recover(); r != nil {
			log.Printf("Recovered from panic in stress instance %s: %v", si.name, r)
			debug.PrintStack()
			err = fmt.Errorf("panic occurred: %v", r)
		}
		if err != nil {
			log.Printf("Stopping stress instance %s: reason: %s", si.name, err)
		} else {
			log.Printf("Stopping stress instance %s: reason: benchmark finished", si.name)
		}
	}()

	// TODO: remove cluster on defer

	if op == clusterOpDelete || op == clusterOpNoop {
		return nil
	}

	cluster, err := si.createCluster(ctx)
	if err != nil {
		incNotCancelled(err, metrics.ErrClusterCreate.WithLabelValues(si.owner.name))
		return fmt.Errorf("create cluster: %w", err)
	}
	defer func() {
		if err := si.destroy(context.Background(), cluster); err != nil {
			metrics.ErrClusterDelete.WithLabelValues(si.owner.name).Inc()
			log.Printf("ERROR destroying cluster: %v", err)
		}
	}()

	if cluster, err = si.waitClusterReady(ctx, cluster); err != nil {
		incNotCancelled(err, metrics.ErrClusterCreate.WithLabelValues(si.owner.name))
		return fmt.Errorf("wait cluster ready: %w", err)
	}
	log.Printf("Instance %s: cluster ready", si.name)

	si.mu.Lock()
	si.readyTime = time.Now()
	si.mu.Unlock()

	k8client, err := si.owner.tester.getClient()
	if err != nil {
		return fmt.Errorf("get k8 client: %w", err)
	}

	endpoint, err := fetchClusterEndpoint(ctx, k8client, cluster)
	if err != nil {
		incNotCancelled(err, metrics.ErrClusterPrepare.WithLabelValues(si.owner.name))
		return fmt.Errorf("fetch cluster endpoint: %w", err)
	}

	bTrue := true
	bFalse := false
	duration := si.config.Test.Duration
	if duration == nil {
		duration = &durationInfinite
	}

	// wait for cluster to be reachable

	// Create prepare and run tasks from test configuration
	factory := tpccTaskFactoryFromEndpoint(endpoint)
	config := benchdriverapi.BenchmarkGoTPCCConfig{
		DropData:       &bTrue,
		Check:          &bFalse,
		IsolationLevel: si.config.Test.IsolationLevel,
		Warehouses:     si.config.Test.Warehouses,
		Threads:        si.config.Test.Threads,
		Partitions:     si.config.Test.Partitions,
		PartitionType:  si.config.Test.PartitionType,
		UseForeignKeys: si.config.Test.UseForeignKeys,
		WaitThinking:   si.config.Test.WaitThinking,
		Duration:       duration,
	}
	prepareTask, err := factory.Prepare(config)
	if err != nil {
		incNotCancelled(err, metrics.ErrClusterPrepare.WithLabelValues(si.owner.name))
		return fmt.Errorf("init prepare task: %w", err)
	}
	checkReady := func(ctx context.Context) error {
		operation := func() (any, error) {
			ready, err := prepareTask.IsReady(ctx)
			if err != nil {
				return nil, err
			}
			if !ready {
				return nil, errors.New("cluster not ready")
			}
			if err := ctx.Err(); err != nil {
				return nil, backoff.Permanent(err)
			}
			return nil, nil
		}

		expBackoff := backoff.NewExponentialBackOff()
		expBackoff.MaxInterval = 1 * time.Minute

		_, err := backoff.Retry(ctx, operation, backoff.WithMaxTries(5))
		if err != nil {
			return fmt.Errorf("check ready failed: %w", err)
		}
		return nil
	}

	if err := checkReady(ctx); err != nil {
		return fmt.Errorf("check ready: %w", err)
	}

	config.Check = &bFalse
	runTask, err := factory.RunStress(config)
	if err != nil {
		incNotCancelled(err, metrics.ErrClusterPrepare.WithLabelValues(si.owner.name))
		return fmt.Errorf("init run task: %w", err)
	}

	cleanupTask, err := factory.Cleanup()
	if err != nil {
		return fmt.Errorf("init cleanup task: %w", err)
	}

	// Run benchmarks
	si.markActive()
	for ctx.Err() == nil {
		err := runStressStep(ctx, si, metrics, checkReady, cleanupTask, prepareTask, runTask, liveStatsPeriod, liveMinAge)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				break
			}

			// In case of errors during runtime we assume it is due to the DB state.
			// Instead of killing the test machine we try to prepare the DB state
			// with new data and continue running the benchmark. Errors during
			// prepare still can kill this instance.
			metrics.ErrClusterDBRun.WithLabelValues(si.owner.name).Inc()
			log.Printf("ERROR running run task: %v", err)
			continue
		}
		log.Printf("Instance %s: tpcc test finished", si.name)
	}

	return nil
}

func runStressStep(
	ctx context.Context,
	si *stressInstance,
	metrics *runMetrics,
	checkReady func(context.Context) error,
	cleanupTask, prepareTask worker.Task,
	runTask *tpcc.BenchRunTask,
	liveStatsPeriod time.Duration,
	liveMinAge time.Duration,
) (err error) {
	ctx, cancel := ctxutil.WithFuncContext(ctx, func() {
		log.Printf("Instance %s: stop signal received during stress step", si.name)
	})
	defer cancel()

	log.Printf("Instance %s: prepare tpcc test", si.owner.name)
	metrics.ClusterDBPrepare.WithLabelValues(si.owner.name).Inc()
	if err := checkReady(ctx); err != nil {
		return fmt.Errorf("check ready pre-prepare: %w", err)
	}

	if _, err := cleanupTask.Task(ctx); err != nil {
		if !errors.Is(err, context.Canceled) {
			metrics.ErrClusterDBPrepare.WithLabelValues(si.owner.name).Inc()
			log.Printf("ERROR running cleanup task: %v", err)
		}
		return fmt.Errorf("exec cleanup during prepare phase: %w", err)
	}

	if _, err := prepareTask.Task(ctx); err != nil {
		if !errors.Is(err, context.Canceled) {
			metrics.ErrClusterDBPrepare.WithLabelValues(si.owner.name).Inc()
			log.Printf("ERROR running prepare task: %v", err)
			return fmt.Errorf("exec prepare phase: %w", err)
		}

		log.Printf("Instance %s: prepare task cancelled", si.name)
		return nil
	}

	log.Printf("Instance %s: running tpcc test", si.name)
	metrics.ClusterDBRun.WithLabelValues(si.owner.name).Inc()

	var summary benchdriverapi.TPCCRunStats
	var cancelCollect context.CancelFunc

	eg, ctx := errgroup.WithContext(ctx)
	ctx, cancelCollect = context.WithCancel(ctx)
	defer cancelCollect()

	if err := checkReady(ctx); err != nil {
		return fmt.Errorf("check ready pre-run: %w", err)
	}

	eg.Go(func() error {
		for ctx.Err() == nil {
			func() {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("Stats collector recovered from panic: %v", r)
					}
				}()
				for range timeutil.IterTick(ctx, liveStatsPeriod) {
					if time.Since(si.readyTime) >= liveMinAge {
						metrics.LiveStats.Observe(runTask.LiveStats(), si.owner.name)
					}
				}
			}()
		}
		return nil
	})
	eg.Go(func() (err error) {
		log.Printf("Instance %s: running tpcc test", si.name)
		defer log.Printf("Instance %s: tpcc test finished", si.name)

		defer cancelCollect()
		summary, err = func() (summary benchdriverapi.TPCCRunStats, err error) {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("Recovered from panic in runTask.Run: %v", r)
					err = fmt.Errorf("panic occurred: %v", r)
				}
			}()
			return runTask.Run(ctx)
		}()
		return err
	})
	eg.Wait()

	if len(summary.Stats) > 0 && summary.Summary.TPMC > 0 && summary.Summary.TPMTotal > 0 && summary.Summary.Efficiency > 0 {
		metrics.SummaryStats.Efficiency.WithLabelValues(si.owner.name).Observe(float64(summary.Summary.Efficiency))
		metrics.SummaryStats.TPMC.WithLabelValues(si.owner.name).Observe(float64(summary.Summary.TPMC))
		metrics.SummaryStats.TPMTotal.WithLabelValues(si.owner.name).Observe(float64(summary.Summary.TPMTotal))
		for _, op := range summary.Stats {
			metrics.SummaryStats.OpMetricsCount.WithLabelValues(si.owner.name, op.Operation).Observe(float64(op.Count))
			metrics.SummaryStats.OpMetricsAvg.WithLabelValues(si.owner.name, op.Operation).Observe(float64(op.Avg))
			metrics.SummaryStats.OpMetricsMedian.WithLabelValues(si.owner.name, op.Operation).Observe(float64(op.Median))
			metrics.SummaryStats.OpMetricsMax.WithLabelValues(si.owner.name, op.Operation).Observe(float64(op.Max))
		}
	}

	return err
}

func (si *stressInstance) createCluster(ctx context.Context) (*cnpgapi.Cluster, error) {
	log.Printf("Instance %s: creating cluster", si.name)

	client, err := syscnpg.NewSystemClientForConfig(si.owner.tester.restConfig, StressTestNamespace)
	if err != nil {
		return nil, err
	}

	config := si.config
	if config.Instance.Metadata == nil {
		config.Instance.Metadata = map[string]interface{}{}
	}
	if labels, ok := config.Instance.Metadata["labels"]; ok || labels == nil {
		config.Instance.Metadata["labels"] = map[string]any{}
	}
	labels := config.Instance.Metadata["labels"].(map[string]any)
	labels[LabelStressTestInstance] = "true"
	labels[LabelStressUser] = si.owner.name

	systemConfig := systems.SystemsConfig{
		Name:      si.name,
		Namespace: StressTestNamespace,
		Kind:      "cnpg",
		Count:     1,
		Metadata:  config.Instance.Metadata,
		Spec:      config.Instance.Spec,
	}

	insts, err := client.Apply(ctx, systemConfig)
	if err != nil {
		return nil, fmt.Errorf("create cluster: %w", err)
	}
	return &insts[0], nil
}

func (si *stressInstance) waitClusterReady(ctx context.Context, cluster *cnpgapi.Cluster) (*cnpgapi.Cluster, error) {
	log.Printf("Instance %s: waiting for cluster ready", si.name)

	if cnpgutil.ClusterReady(cluster) {
		return cluster, nil
	}

	clusterClient, err := si.owner.tester.getClustersClient()
	if err != nil {
		return cluster, err
	}

	createTimeut := benchdriverapi.GetOptValue(si.config.Instance.CreateTimeout, benchdriverapi.Duration{Duration: 5 * time.Minute}).Duration
	if createTimeut > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, createTimeut)
		defer cancel()
	}

	if err = cnpgutil.WaitClusterReady(ctx, clusterClient, cluster.Name, cluster.Namespace, createTimeut); err != nil {
		return cluster, err
	}
	return cluster, nil
}

func (si *stressInstance) markActive() {
	si.registry.mu.Lock()
	defer si.registry.mu.Unlock()
	si.registry.active[si.name] = struct{}{}
	delete(si.registry.starting, si.name)
}

func (si *stressInstance) deregister() {
	si.registry.mu.Lock()
	defer si.registry.mu.Unlock()
	delete(si.registry.active, si.name)
	delete(si.registry.instances, si.name)
	delete(si.registry.starting, si.name)
}

func (si *stressInstance) destroy(ctx context.Context, cluster *cnpgapi.Cluster) error {
	log.Printf("Instance %s: destroying cluster", si.name)

	client, err := si.owner.tester.getCNPGClient()
	if err != nil {
		return fmt.Errorf("get cnpg client: %w", err)
	}

	log.Printf("Deleting cluster %s", cluster.Name)
	if err := client.Clusters(cluster.Namespace).Delete(ctx, cluster.Name, metav1.DeleteOptions{}); err != nil {
		return fmt.Errorf("delete operation: %w", err)
	}
	return nil
}

func incNotCancelled(err error, counter prometheus.Counter) {
	if !errors.Is(err, context.Canceled) {
		counter.Inc()
	}
}
