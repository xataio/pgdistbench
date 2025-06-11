package k8stress

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"pgdistbench/api/benchdriverapi"
	"pgdistbench/internal/worker"

	"golang.org/x/sync/errgroup"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	cnpgapi "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cnpgclient "pgdistbench/pkg/clientset/cnpg/v1"
	"pgdistbench/pkg/clientset/csutil"
)

type Tester struct {
	restConfig *rest.Config
	metrics    *stressMetrics

	k8mu     sync.Mutex
	k8Client *kubernetes.Clientset

	cnpgmu     sync.Mutex
	cnpgClient *cnpgclient.CNPGV1Client
}

type clusterOp int

const (
	clusterOpCreate clusterOp = iota
	clusterOpDelete
	clusterOpBranch
	clusterOpNoop
)

func (op clusterOp) String() string {
	switch op {
	case clusterOpCreate:
		return "create"
	case clusterOpDelete:
		return "delete"
	case clusterOpBranch:
		return "branch"
	case clusterOpNoop:
		return "noop"
	default:
		panic(fmt.Errorf("unknown clusterOp: %d", op))
	}
}

func New(restConfig *rest.Config, metrics *stressMetrics) *Tester {
	return &Tester{
		restConfig: restConfig,
		metrics:    metrics,
	}
}

func (t *Tester) Prepare(ctx context.Context, cfg benchdriverapi.BenchmarkK8StressConfig) error {
	// Nothing to do, but let's try to cleanup the cluster before to ensure we have a pristine environment
	if err := t.Cleanup(ctx); err != nil {
		return err
	}

	return t.checkNamespace(ctx)
}

func (t *Tester) checkNamespace(ctx context.Context) error {
	// check if namespace is available
	k8client, err := t.getClient()
	if err != nil {
		return err
	}

	_, err = k8client.CoreV1().Namespaces().Get(ctx, StressTestNamespace, metav1.GetOptions{})
	return err
}

func (t *Tester) getClient() (*kubernetes.Clientset, error) {
	t.k8mu.Lock()
	defer t.k8mu.Unlock()

	if t.k8Client == nil {
		// let's create a stress test namespace
		k8client, err := kubernetes.NewForConfig(t.restConfig)
		if err != nil {
			return nil, err
		}
		t.k8Client = k8client
	}
	return t.k8Client, nil
}

func (t *Tester) getCNPGClient() (*cnpgclient.CNPGV1Client, error) {
	t.cnpgmu.Lock()
	defer t.cnpgmu.Unlock()

	if t.cnpgClient == nil {
		client, err := cnpgclient.NewForConfig(t.restConfig)
		if err != nil {
			return nil, err
		}
		t.cnpgClient = client
	}
	return t.cnpgClient, nil
}

func (t *Tester) getClustersClient() (cnpgclient.ClustersInterface, error) {
	client, err := t.getCNPGClient()
	if err != nil {
		return nil, err
	}
	return client.Clusters(StressTestNamespace), nil
}

func (t *Tester) Cleanup(ctx context.Context) error {
	// We only delete all instances, but keep the namespace for now.

	clusters, err := t.getClustersClient()
	if err != nil {
		return err
	}

	gracePeriodImmediate := int64(0)
	delOpts := metav1.DeleteOptions{
		GracePeriodSeconds: &gracePeriodImmediate,
	}

	listOpts := metav1.ListOptions{
		LabelSelector: metav1.FormatLabelSelector(&metav1.LabelSelector{
			MatchLabels: map[string]string{
				LabelStressTestInstance: "true",
			},
		}),
	}

	if err := clusters.DeleteCollection(ctx, delOpts, listOpts); err != nil {
		return fmt.Errorf("delete all instances: %w", err)
	}

	return err
}

func (t *Tester) initRun(ctx context.Context, requ benchdriverapi.BenchmarkK8StressConfig) (*clusterState, error) {
	state, err := t.readClusterState(ctx)
	if err != nil {
		return nil, err
	}
	if len(state.Instances) == 0 {
		return state, nil
	}

	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()

	eg, ctx := errgroup.WithContext(ctx)
	work := make(chan worker.Task, len(state.Instances))
	for range 32 {
		eg.Go(func() error {
			for ctx.Err() == nil {
				select {
				case task, ok := <-work:
					if !ok {
						return nil
					}
					if _, err := task.Task(ctx); err != nil {
						return err
					}
				case <-ctx.Done():
					return nil
				}
			}
			return nil
		})
	}

	k8client, err := t.getClient()
	if err != nil {
		return nil, err
	}

	for _, inst := range state.Instances {
		endpoint, err := fetchClusterEndpoint(ctx, k8client, &inst)
		if err != nil {
			return nil, fmt.Errorf("get endpoint for cluster %s: %w", inst.Name, err)
		}
		factory := tpccTaskFactoryFromEndpoint(endpoint)
		cleanupTask, err := factory.Cleanup()
		if err != nil {
			return nil, fmt.Errorf("create cleanup task: %w", err)
		}
		bTrue := true
		prepareTask, err := factory.Prepare(benchdriverapi.BenchmarkGoTPCCConfig{
			Check:    &bTrue,
			DropData: &bTrue,
		})
		if err != nil {
			return nil, fmt.Errorf("create prepare task: %w", err)
		}

		if err := chTrySend(ctx, work, cleanupTask); err != nil {
			return nil, err
		}
		if err := chTrySend(ctx, work, prepareTask); err != nil {
			return nil, err
		}
	}

	return state, eg.Wait()
}

func (t *Tester) Run(ctx context.Context, cfg benchdriverapi.BenchmarkK8StressConfig) (stats benchdriverapi.StressRunStats, err error) {
	if len(cfg.Users) == 0 {
		return stats, errors.New("no users configured")
	}
	// Try to cleanup resources on shutdown
	defer t.Cleanup(ctx)

	if d := cfg.Duration; d != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, d.Duration)
		defer cancel()
	}

	eg, ctx := errgroup.WithContext(ctx)
	for i, userCfg := range cfg.Users {
		for j := range benchdriverapi.GetOptValue(userCfg.Copies, 1) {
			name := fmt.Sprintf("%d-%d", i, j)
			if userCfg.Name != "" {
				name = fmt.Sprintf("%s-%d", userCfg.Name, j)
			}
			user := newStressUser(t, name, userCfg)
			eg.Go(func() error {
				t.metrics.Run.ActiveUsers.Inc()
				defer t.metrics.Run.ActiveUsers.Dec()
				err := user.Run(ctx)
				if err != nil {
					if !errors.Is(err, context.Canceled) {
						t.metrics.Run.UsersErrors.Inc()
					}
				}
				return nil
			})
		}
	}
	return stats, eg.Wait()
}

func (t *Tester) readClusterState(ctx context.Context) (*clusterState, error) {
	instances, err := t.listCNPGClusters(ctx)
	if err != nil {
		return nil, err
	}

	return &clusterState{
		Instances: instances,
	}, nil
}

func (t *Tester) listCNPGClusters(ctx context.Context) ([]cnpgapi.Cluster, error) {
	clusters, err := t.getClustersClient()
	if err != nil {
		return nil, err
	}

	listOpts := metav1.ListOptions{
		LabelSelector: metav1.FormatLabelSelector(&metav1.LabelSelector{
			MatchLabels: map[string]string{
				LabelStressTestInstance: "true",
			},
		}),
	}

	return csutil.CollectList(ctx, clusters, listOpts,
		func(l *cnpgapi.ClusterList) []cnpgapi.Cluster {
			return l.Items
		},
	)
}

type clusterState struct {
	Instances []cnpgapi.Cluster
}
