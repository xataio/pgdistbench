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
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
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

	// Set up force deletion that triggers 30 seconds after cancellation
	//
	// Note: We register the 'destroy' cluster operation before the 'create' operation.
	//       This is to ensure that the cluster is destroyed if the 'create'
	//       operation fails, but the resource was created anyways. For example
	//       due to a network error after the instance was created within
	//       Kubernetes.
	//
	//       The force deletion is triggered by a stop signal received during
	//       the benchmark run. This is to ensure that the test really stops in case
	//       the postgres clients are blocking the runner from completing the
	//       cleanup. By deleting the DB we hopefully force an error on the
	//       clients.
	cleanupStarted := make(chan struct{})
	ctx, cancel := ctxutil.WithFuncContext(ctx, func() {
		log.Printf("Instance %s: stop signal received, starting force deletion timer", si.name)

		go func() {
			// Wait 30 seconds for normal cleanup to start
			select {
			case <-time.After(30 * time.Second):
				log.Printf("Instance %s: normal cleanup timeout, starting force deletion", si.name)
			case <-cleanupStarted:
				log.Printf("Instance %s: normal cleanup started, waiting for completion", si.name)
			}

			log.Printf("Instance %s: performing cluster cleanup and deletion", si.name)
			if err := si.destroyWithRetry(context.Background(), StressTestNamespace, si.name); err != nil {
				metrics.ErrClusterDelete.WithLabelValues(si.owner.name).Inc()
				log.Printf("ERROR destroying cluster: %v", err)
			} else {
				log.Printf("Instance %s: cluster cleanup completed successfully", si.name)
			}
		}()
	})
	defer cancel()
	defer close(cleanupStarted) // notify context handler that we are handling a normal cleanup

	cluster, err := si.createCluster(ctx, si.name)
	if err != nil {
		incNotCancelled(err, metrics.ErrClusterCreate.WithLabelValues(si.owner.name))
		return fmt.Errorf("create cluster: %w", err)
	}

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

func (si *stressInstance) createCluster(ctx context.Context, name string) (*cnpgapi.Cluster, error) {
	log.Printf("Instance %s: creating cluster", name)

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
		Name:      name,
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

func (si *stressInstance) destroyWithRetry(ctx context.Context, namespace, name string) error {
	log.Printf("Instance %s: destroying cluster with retry logic", si.name)

	operation := func() (any, error) {
		err := si.destroy(ctx, namespace, name)
		if err != nil {
			log.Printf("Instance %s: destroy attempt failed, will retry: %v", si.name, err)
			return nil, err
		}
		return nil, nil
	}

	expBackoff := backoff.NewExponentialBackOff()
	expBackoff.InitialInterval = 5 * time.Second
	expBackoff.MaxInterval = 1 * time.Minute
	expBackoff.Multiplier = 2.0

	// Retry for up to 20 attempts with exponential backoff
	_, err := backoff.Retry(ctx, operation, backoff.WithMaxTries(20))
	if err != nil {
		return fmt.Errorf("destroy with retry failed after all attempts: %w", err)
	}

	log.Printf("Instance %s: cluster destroyed successfully", si.name)
	return nil
}

func (si *stressInstance) destroy(ctx context.Context, namespace, name string) error {
	log.Printf("Instance %s: destroying cluster", si.name)

	client, err := si.owner.tester.getCNPGClient()
	if err != nil {
		return fmt.Errorf("get cnpg client: %w", err)
	}

	log.Printf("Deleting cluster %s", name)
	if err := client.Clusters(namespace).Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
		if k8serrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("delete operation: %w", err)
	}

	clusterClient, err := si.owner.tester.getClustersClient()
	if err != nil {
		return fmt.Errorf("get clusters client for deletion wait: %w", err)
	}

	// Set timeout for deletion - shorter if we're in a hurry (force deletion context)
	deleteTimeout := 5 * time.Minute
	if deadline, hasDeadline := ctx.Deadline(); hasDeadline {
		// If the context already has a deadline, respect it and use a shorter timeout
		remaining := time.Until(deadline)
		if remaining < deleteTimeout {
			deleteTimeout = remaining - (2 * time.Second) // Leave 2s buffer
			if deleteTimeout <= 0 {
				deleteTimeout = 5 * time.Second // Minimum timeout
			}
		}
	}

	// Wait for the cluster to actually be deleted from Kubernetes
	log.Printf("Instance %s: waiting for cluster deletion to complete (timeout: %v)", si.name, deleteTimeout)
	deleteCtx, cancel := context.WithTimeout(ctx, deleteTimeout)
	defer cancel()

	if err := cnpgutil.WaitClusterDeleted(deleteCtx, clusterClient, name, namespace, deleteTimeout); err != nil {
		return fmt.Errorf("wait for cluster deletion: %w", err)
	}

	log.Printf("Instance %s: cluster deletion confirmed", si.name)
	return nil
}

func incNotCancelled(err error, counter prometheus.Counter) {
	if !errors.Is(err, context.Canceled) {
		counter.Inc()
	}
}
