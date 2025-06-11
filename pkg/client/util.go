package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"net/http"
	"net/url"
	"slices"
	"sync"
	"time"

	"pgdistbench/api/benchdriverapi"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"golang.org/x/sync/errgroup"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type multiProxyClient struct {
	client     *http.Client
	restConfig *rest.Config
	urls       []*url.URL
}

func multiproxyClientForURLS(restConfig *rest.Config, urls []*url.URL) (*multiProxyClient, error) {
	client, err := rest.HTTPClientFor(restConfig)
	if err != nil {
		return nil, err
	}
	return &multiProxyClient{
		client:     client,
		restConfig: restConfig,
		urls:       urls,
	}, nil
}

var ErrBusy = errors.New("worker is busy")

func multiProxyClientForPodGroupsPods(restConfig *rest.Config, podGroups []PodGroup, path ...string) (*multiProxyClient, error) {
	clusterURL, _, err := rest.DefaultServerUrlFor(restConfig)
	if err != nil {
		return nil, err
	}

	var urls []*url.URL
	for _, grp := range podGroups {
		for _, pod := range grp.Pods {
			urls = append(urls, podProxyUrl(clusterURL, pod.Namespace, pod.Name, path...))
		}
	}
	return multiproxyClientForURLS(restConfig, urls)
}

func multiProxyClientForPodGroupsProxy(restConfig *rest.Config, podGroups []PodGroup, path ...string) (*multiProxyClient, error) {
	clusterURL, _, err := rest.DefaultServerUrlFor(restConfig)
	if err != nil {
		return nil, err
	}

	var urls []*url.URL
	for _, grp := range podGroups {
		for _, pod := range grp.Pods {
			urls = append(urls, podProxyUrl(clusterURL, pod.Namespace, pod.Name, path...))
			break
		}
	}
	return multiproxyClientForURLS(restConfig, urls)
}

func (mpc *multiProxyClient) Post(ctx context.Context, path string, body any) error {
	var rawBody []byte
	if body != nil {
		var err error
		rawBody, err = json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal body: %w", err)
		}
	}

	// TODO: slices.Values
	urls := func(yield func(*url.URL) bool) {
		for _, u := range mpc.urls {
			if !yield(u) {
				break
			}
		}
	}

	return eachParallel(urls).Do(ctx, func(ctx context.Context, url *url.URL) error {
		url = url.JoinPath(path)
		var bodyReader io.Reader
		if rawBody != nil {
			bodyReader = bytes.NewReader(rawBody)
		}
		resp, err := mpc.client.Post(url.String(), "application/json", bodyReader)
		if err != nil {
			return fmt.Errorf("post %s: %w", url, err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			respBody, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("post request failed failed (%s): %s", resp.Status, respBody)
		}
		return nil
	})
}

type parallelExec[T any] struct {
	iter   iter.Seq[T]
	active int
}

func (e parallelExec[T]) Active(i int) parallelExec[T] {
	e.active = i
	return e
}

func (e parallelExec[T]) Do(ctx context.Context, fn func(context.Context, T) error) error {
	var errs []error
	var mu sync.Mutex
	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(e.active)

	for item := range e.iter {
		eg.Go(func() error {
			err := fn(ctx, item)
			if err != nil {
				mu.Lock()
				defer mu.Unlock()
				errs = append(errs, err)
			}
			return err
		})
	}

	eg.Wait()
	return errors.Join(errs...)
}

func eachParallel[T any](iter iter.Seq[T]) parallelExec[T] {
	return parallelExec[T]{iter: iter, active: -1}
}

func podProxyUrl(clusterURL *url.URL, namespace, name string, path ...string) *url.URL {
	proxyURL := clusterURL.JoinPath("/api/v1/namespaces", namespace, "pods", name, "proxy")
	if len(path) == 0 {
		return proxyURL
	}
	return proxyURL.JoinPath(path...)
}

func findDeploymentPods(ctx context.Context, client kubernetes.Interface, deployment *appsv1.Deployment) (grp PodGroup, err error) {
	rs, err := findDeploymentReplicaSet(ctx, client, deployment)
	if err != nil {
		return grp, err
	}

	rsPods, err := findReplicasetPods(ctx, client, &rs)
	if err != nil {
		return grp, err
	}

	return PodGroup{
		Deployment: deployment,
		Pods:       rsPods,
	}, nil
}

// findDeploymentReplicaSet finds the newest replica set for a deployment. The
// implementation is based on the `kubectl describe deployment` implementation.
// We fetch the newest replicaset similar to how it is read by the describe
// command.
func findDeploymentReplicaSet(ctx context.Context, client kubernetes.Interface, dep *appsv1.Deployment) (rs appsv1.ReplicaSet, err error) {
	selector, err := metav1.LabelSelectorAsSelector(dep.Spec.Selector)
	if err != nil {
		return rs, err
	}
	all, err := client.AppsV1().ReplicaSets(dep.GetNamespace()).List(ctx, metav1.ListOptions{
		LabelSelector: selector.String(),
	})
	if err != nil {
		return rs, err
	}

	owned := make([]*appsv1.ReplicaSet, 0, len(all.Items))
	for i := range all.Items {
		rs := &all.Items[i]
		if metav1.IsControlledBy(rs, dep) {
			owned = append(owned, rs)
		}
	}

	if newest := findNewReplicaSet(dep, owned); newest != nil {
		return *newest, nil
	}
	return rs, fmt.Errorf("no new replica set found for deployment %s", dep.Name)
}

func findNewReplicaSet(dep *appsv1.Deployment, owned []*appsv1.ReplicaSet) *appsv1.ReplicaSet {
	slices.SortFunc(owned, func(a, b *appsv1.ReplicaSet) int {
		if a.CreationTimestamp.Equal(&b.CreationTimestamp) {
			if a.Name < b.Name {
				return -1
			}
			return 1
		}
		return a.CreationTimestamp.Compare(b.CreationTimestamp.Time)
	})
	for _, rs := range owned {
		if equalIgnoreHash(&rs.Spec.Template, &dep.Spec.Template) {
			return rs
		}
	}
	return nil
}

func findReplicasetPods(ctx context.Context, client kubernetes.Interface, rs *appsv1.ReplicaSet) ([]corev1.Pod, error) {
	selector, err := metav1.LabelSelectorAsSelector(rs.Spec.Selector)
	if err != nil {
		return nil, err
	}

	all, err := client.CoreV1().Pods(rs.GetNamespace()).List(ctx, metav1.ListOptions{
		LabelSelector: selector.String(),
	})
	if err != nil {
		return nil, err
	}

	// filter by owner reference
	pods := slices.DeleteFunc(all.Items, func(pod corev1.Pod) bool {
		keep := false
		for _, ref := range pod.GetOwnerReferences() {
			if ref.UID == rs.UID {
				keep = true
				break
			}
		}
		return !keep
	})
	if len(pods) == 0 {
		return nil, fmt.Errorf("no pods found for replicaset %s", rs.Name)
	}
	return pods, nil
}

func equalIgnoreHash(template1, template2 *corev1.PodTemplateSpec) bool {
	t1Copy := template1.DeepCopy()
	t2Copy := template2.DeepCopy()
	// Remove hash labels from template.Labels before comparing
	delete(t1Copy.Labels, appsv1.DefaultDeploymentUniqueLabelKey)
	delete(t2Copy.Labels, appsv1.DefaultDeploymentUniqueLabelKey)
	return apiequality.Semantic.DeepEqual(t1Copy, t2Copy)
}

type runResultCollector[T any] struct {
	restConfig *rest.Config
	name       benchdriverapi.TaskName
	path       []string
	Validate   func(T) error
	Decoder    func(io.Reader) (T, error)
}

func JSONDecoder[T any](body io.Reader) (v T, err error) {
	err = json.NewDecoder(body).Decode(&v)
	return v, err
}

func PromDecoder(body io.Reader) (map[string]*dto.MetricFamily, error) {
	var parser expfmt.TextParser
	metricFamilies, err := parser.TextToMetricFamilies(body)
	return metricFamilies, err
}

func (c *runResultCollector[T]) Collect(ctx context.Context, proxyURLs iter.Seq[*url.URL], wait bool) (report []T, err error) {
	client, err := rest.HTTPClientFor(c.restConfig)
	if err != nil {
		return report, fmt.Errorf("init http client: %w", err)
	}

	eg, ctx := errgroup.WithContext(ctx)
	ch := make(chan T, 1)

	var urls []*url.URL
	for url := range proxyURLs {
		urls = append(urls, url)
	}
	report = make([]T, 0, len(urls))
	eg.Go(func() error {
		for ctx.Err() == nil && len(report) < cap(report) {
			select {
			case <-ctx.Done():
				return nil
			case r := <-ch:
				report = append(report, r)
			}
		}
		return nil
	})

	eg.Go(func() error {
		execStatus := func(ctx context.Context, url *url.URL) error {
			if len(c.path) > 0 {
				url = url.JoinPath(c.path...)
			}
			resp, err := client.Get(url.String())
			if err != nil {
				return fmt.Errorf("get %s: %w", url, err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != 200 {
				return fmt.Errorf("request failed: %s", resp.Status)
			}

			var status T
			if c.Decoder == nil {
				status, err = JSONDecoder[T](resp.Body)
			} else {
				status, err = c.Decoder(resp.Body)
			}
			if err != nil {
				return fmt.Errorf("decode response: %w", err)
			}

			if c.Validate != nil {
				if err := c.Validate(status); err != nil {
					return err
				}
			}

			select {
			case ch <- status:
			case <-ctx.Done():
			}

			return nil
		}

		fetchRunner := eachParallel(proxyURLs)
		if !wait {
			return fetchRunner.Do(ctx, execStatus)
		}
		return fetchRunner.Do(ctx, func(ctx context.Context, url *url.URL) error {
			ticker := time.NewTicker(10 * time.Second)
			defer ticker.Stop()
			for {
				err := execStatus(ctx, url)
				if err != ErrBusy {
					return err
				}

				select {
				case <-ticker.C:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		})
	})

	err = eg.Wait()
	return report, err
}

func ValidateStatus[T any](opName benchdriverapi.TaskName, allowError bool) func(benchdriverapi.WorkerStatus[benchdriverapi.Result[T]]) error {
	return func(status benchdriverapi.WorkerStatus[benchdriverapi.Result[T]]) error {
		if status.Task == "" {
			return errors.New("no benchmark task results found")
		}
		if status.Code == benchdriverapi.StatusBusy {
			return ErrBusy
		}
		if status.Task != opName {
			return fmt.Errorf("no %s status found, last task is %s", opName, status.Task)
		}
		if status.Last == nil {
			panic(fmt.Errorf("%v benchmark run without results?", opName))
		}
		if status.Last.Error != nil {
			if !allowError {
				return fmt.Errorf("op %v: last task failed: %s", opName, status.Last.Error)
			}
		}
		return nil
	}
}
