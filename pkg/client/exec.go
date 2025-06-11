package client

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"net/url"
	"sync"
	"time"

	"pgdistbench/api/benchdriverapi"

	dto "github.com/prometheus/client_model/go"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type Benchmarks struct {
	parent     *Client
	restConfig *rest.Config
}

type ExecConfig struct {
	Name      string
	Runner    RunnerRef
	Benchmark string
	Config    map[string]any
}

type RunnerRef struct {
	Name      string
	Namespace string
}

type BenchmarkInstance struct {
	parent     *Client
	config     ExecConfig
	restConfig *rest.Config
	podGroups  []PodGroup
	clusterURL *url.URL
}

func (b *Benchmarks) Access(ctx context.Context, cfg ExecConfig) (*BenchmarkInstance, error) {
	grps, err := b.listPodGroups(ctx, cfg.Runner)
	if err != nil {
		return nil, err
	}

	clusterURL, _, err := rest.DefaultServerUrlFor(b.restConfig)
	if err != nil {
		return nil, fmt.Errorf("get cluster URL: %w", err)
	}

	return &BenchmarkInstance{
		parent:     b.parent,
		restConfig: b.restConfig,
		podGroups:  grps,
		config:     cfg,
		clusterURL: clusterURL,
	}, nil
}

func (b *Benchmarks) Pods(ctx context.Context, cfg ExecConfig) ([]PodGroup, error) {
	return b.listPodGroups(ctx, cfg.Runner)
}

func (inst *BenchmarkInstance) NumGroups() int {
	return len(inst.podGroups)
}

func (inst *BenchmarkInstance) NumPods() int {
	n := 0
	for i := range inst.podGroups {
		n += len(inst.podGroups[i].Pods)
	}
	return n
}

func (inst *BenchmarkInstance) Config() ExecConfig {
	return inst.config
}

func (inst *BenchmarkInstance) podProxyURL(pod *corev1.Pod, path ...string) *url.URL {
	return podProxyUrl(inst.clusterURL, pod.Namespace, pod.Name, path...)
}

func (inst *BenchmarkInstance) groupProxyURL(grp *PodGroup, path ...string) *url.URL {
	if len(grp.Pods) == 0 {
		return nil
	}
	return inst.podProxyURL(&grp.Pods[0], path...)
}

func (inst *BenchmarkInstance) Healthcheck(ctx context.Context) (idle bool, err error) {
	client, err := rest.HTTPClientFor(inst.restConfig)
	if err != nil {
		return false, fmt.Errorf("init http client: %w", err)
	}

	idle = true
	var mu sync.Mutex

	err = eachParallel(inst.EachPodProxyURL).Do(ctx, func(ctx context.Context, url *url.URL) error {
		url = url.JoinPath("healthz")
		resp, err := client.Get(url.String())
		if err != nil {
			return fmt.Errorf("get %s: %w", url, err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			return fmt.Errorf("healthcheck failed: %s", resp.Status)
		}

		var workerStatus benchdriverapi.StatusCode
		if err := json.NewDecoder(resp.Body).Decode(&workerStatus); err != nil {
			return fmt.Errorf("decode status: %w", err)
		}

		if workerStatus != benchdriverapi.StatusIdle {
			mu.Lock()
			defer mu.Unlock()
			idle = false
		}

		return nil
	})

	return idle, err
}

func (inst *BenchmarkInstance) Metrics(ctx context.Context) ([]map[string]*dto.MetricFamily, error) {
	type metricsCollector = runResultCollector[map[string]*dto.MetricFamily]
	collector := metricsCollector{
		restConfig: inst.restConfig,
		path:       []string{"metrics"},
		Decoder:    PromDecoder,
	}
	return collector.Collect(ctx, inst.EachPodProxyURL, false)
}

func (inst *BenchmarkInstance) Prepare(ctx context.Context) error {
	return inst.postGroupWork(ctx, "prepare", inst.config.Config)
}

func (inst *BenchmarkInstance) Run(ctx context.Context) error {
	return inst.postPodWork(ctx, "run", inst.config.Config)
}

func (inst *BenchmarkInstance) Check(ctx context.Context) error {
	return inst.postPodWork(ctx, "check", inst.config.Config)
}

func (inst *BenchmarkInstance) Cleanup(ctx context.Context) error {
	return inst.postGroupWork(ctx, "cleanup", nil)
}

func (inst *BenchmarkInstance) WaitIdle(ctx context.Context) error {
	return eachParallel(inst.EachPodProxyURL).Do(ctx, func(ctx context.Context, url *url.URL) error {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for ctx.Err() == nil {
			idle, err := inst.Healthcheck(ctx)
			if err != nil || idle {
				return err
			}

			select {
			case <-ticker.C:
			case <-ctx.Done():
				return nil
			}
		}
		return nil
	})
}

func (inst *BenchmarkInstance) postPodWork(ctx context.Context, path string, body any) error {
	return inst.postWork(ctx, inst.EachPodProxyURL, path, body)
}

func (inst *BenchmarkInstance) postGroupWork(ctx context.Context, path string, body any) error {
	return inst.postWork(ctx, inst.EachGroupProxyURL, path, body)
}

func (inst *BenchmarkInstance) postWork(ctx context.Context, iterURLs iter.Seq[*url.URL], path string, body any) error {
	urls := make([]*url.URL, 0, inst.NumPods())
	for url := range iterURLs {
		urls = append(urls, url)
	}

	mpc, err := multiproxyClientForURLS(inst.restConfig, urls)
	if err != nil {
		return err
	}

	path = fmt.Sprintf("work/%s/%s", inst.config.Benchmark, path)
	return mpc.Post(ctx, path, body)
}

func (e *Benchmarks) listPodGroups(ctx context.Context, ref RunnerRef) ([]PodGroup, error) {
	deployments, err := e.parent.Runners().ListDeploymentsByName(ctx, ref.Namespace, ref.Name)
	if err != nil {
		return nil, err
	}

	// fetch replica sets for deployments
	k8client, err := kubernetes.NewForConfig(e.restConfig)
	if err != nil {
		return nil, err
	}

	var pods []PodGroup
	for i := range deployments {
		dep := &deployments[i]
		grp, err := findDeploymentPods(ctx, k8client, dep)
		if err != nil {
			return nil, err
		}
		pods = append(pods, grp)
	}

	return pods, nil
}

func (inst *BenchmarkInstance) EachGroup(fn func(*PodGroup) bool) {
	for i := range inst.podGroups {
		if !fn(&inst.podGroups[i]) {
			return
		}
	}
}

func (inst *BenchmarkInstance) EachPod(fn func(*corev1.Pod) bool) {
	iterGroupsPods(inst.podGroups)(fn)
}

func (inst *BenchmarkInstance) EachPodProxyURL(fn func(*url.URL) bool) {
	inst.EachPod(func(pod *corev1.Pod) bool {
		return fn(inst.podProxyURL(pod))
	})
}

func (inst *BenchmarkInstance) EachGroupProxyURL(fn func(*url.URL) bool) {
	inst.EachGroup(func(grp *PodGroup) bool {
		url := inst.groupProxyURL(grp)
		if url != nil {
			return fn(url)
		}
		return true
	})
}
