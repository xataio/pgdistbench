package client

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"log"
	"net/url"
	"time"

	"pgdistbench/pkg/client/systems"

	dto "github.com/prometheus/client_model/go"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	appsv1ifc "k8s.io/client-go/kubernetes/typed/apps/v1"
	"k8s.io/client-go/rest"
)

// Runners provides access to the test runner executing the actual tests
// against an instance. Runners are managed as k8s Deployments. We create one
// deployment per system (instance) under test. Runners are associated with
// their system by marking the system as the owner of the deployment.
type Runners struct {
	parent     *Client
	restConfig *rest.Config
}

type Runner struct {
	runners *Runners
	cfg     RunnerConfig
}

type runnersClient struct {
	appsv1ifc.DeploymentInterface
	runner *Runner
}

type RunnerConfig struct {
	Name            string            `yaml:"name" json:"name"`
	Namespace       string            `yaml:"namespace" json:"namespace"`
	Metadata        map[string]any    `yaml:"metadata" json:"metadata"`
	Systems         []SystemRef       `yaml:"systems" json:"systems"`
	Spec            map[string]any    `yaml:"spec" json:"spec"`
	Resources       map[string]any    `yaml:"resources" json:"resources"`
	Image           string            `yaml:"image" json:"image"`
	ImagePullPolicy corev1.PullPolicy `yaml:"imagePullPolicy" json:"imagePullPolicy"`
}

type SystemRef struct {
	Name      string `yaml:"name" json:"name"`
	Namespace string `yaml:"namespace" json:"namespace"`
	Kind      string `yaml:"kind" json:"kind"`
}

type RunnerSystemInfo struct {
	Name   string
	System *systems.SystemInstanceInfo
	Ready  bool
}

type RunnerSystemStatus struct {
	RawSystems            [][]systems.SystemInstance
	ActiveSystems         int
	TotalSystems          int
	RunnerDeployments     []appsv1.Deployment
	Systems               map[types.UID]*systems.SystemInstance
	StandaloneDeployments []*appsv1.Deployment
	OrphanedDeployments   []*appsv1.Deployment
	UnassociatedSystems   []*systems.SystemInstance
	Assocs                map[types.UID]types.UID // map deployment UID to system UID
}

func (st *RunnerSystemStatus) SystemForDeployment(dep *appsv1.Deployment) *systems.SystemInstance {
	if sysID, ok := st.Assocs[dep.UID]; ok {
		if sys, ok := st.Systems[sysID]; ok {
			return sys
		}
	}
	return nil
}

type PodGroup struct {
	System     *systems.SystemInstance
	Deployment *appsv1.Deployment
	Pods       []corev1.Pod
}

func iterGroupsPods(grps []PodGroup) iter.Seq[*corev1.Pod] {
	return func(yield func(*corev1.Pod) bool) {
		for _, grp := range grps {
			for j := range grp.Pods {
				if !yield(&grp.Pods[j]) {
					return
				}
			}
		}
	}
}

func iterGroupProxyURL(clusterURL *url.URL, grps []PodGroup, path ...string) iter.Seq[*url.URL] {
	return func(yield func(*url.URL) bool) {
		for _, grp := range grps {
			if len(grp.Pods) > 0 {
				url := podProxyUrl(clusterURL, grp.Pods[0].Namespace, grp.Pods[0].Name, path...)
				if !yield(url) {
					return
				}
			}
		}
	}
}

func iterGroupPodProxyURL(clusterURL *url.URL, grps []PodGroup, path ...string) iter.Seq[*url.URL] {
	return func(yield func(*url.URL) bool) {
		for pod := range iterGroupsPods(grps) {
			url := podProxyUrl(clusterURL, pod.Namespace, pod.Name, path...)
			if !yield(url) {
				return
			}
		}
	}
}

const (
	LabelBenchmarkRoleRunner = "bench.maki.tech/runner"
	LabelRunnerStandalone    = "bench.maki.tech/standalone_runner"
)

func (r *Runners) Access(cfg RunnerConfig) *Runner {
	return &Runner{r, cfg}
}

func (r *Runners) ListDeploymentsByName(ctx context.Context, namespace, name string) ([]appsv1.Deployment, error) {
	client, err := deploymentForConfig(r.restConfig, namespace)
	if err != nil {
		return nil, err
	}

	list, err := client.List(ctx, metav1.ListOptions{
		LabelSelector: metav1.FormatLabelSelector(&metav1.LabelSelector{
			MatchLabels: map[string]string{
				LabelBenchmarkRoleRunner: name,
			},
		}),
	})
	if err != nil {
		return nil, err
	}
	return list.Items, nil
}

func (r *Runners) GetPodGroups(ctx context.Context, ref RunnerRef) ([]PodGroup, error) {
	deployments, err := r.ListDeploymentsByName(ctx, ref.Namespace, ref.Name)
	if err != nil {
		return nil, err
	}

	// fetch replica sets for deployments
	k8client, err := kubernetes.NewForConfig(r.restConfig)
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

func (r *Runners) Metrics(ctx context.Context, ref RunnerRef) ([]map[string]*dto.MetricFamily, error) {
	type metricsCollector = runResultCollector[map[string]*dto.MetricFamily]
	collector := metricsCollector{
		restConfig: r.restConfig,
		path:       []string{"metrics"},
		Decoder:    PromDecoder,
	}

	clusterURL, _, err := rest.DefaultServerUrlFor(r.restConfig)
	if err != nil {
		return nil, fmt.Errorf("get cluster URL: %w", err)
	}

	pods, err := r.GetPodGroups(ctx, RunnerRef{Namespace: ref.Namespace, Name: ref.Name})
	if err != nil {
		return nil, err
	}

	urls := iterGroupPodProxyURL(clusterURL, pods)
	return collector.Collect(ctx, urls, false)
}

func (r *Runner) GetPodGroups(ctx context.Context) ([]PodGroup, error) {
	return r.runners.GetPodGroups(ctx, RunnerRef{Namespace: r.cfg.Namespace, Name: r.cfg.Name})
}

func (r *Runner) Metrics(ctx context.Context) ([]map[string]*dto.MetricFamily, error) {
	return r.runners.Metrics(ctx, RunnerRef{Namespace: r.cfg.Namespace, Name: r.cfg.Name})
}

type ApplyConfig struct {
	SSLMode string
}

func (r *Runner) Apply(ctx context.Context, cfg ApplyConfig) ([]RunnerSystemInfo, error) {
	if len(r.cfg.Systems) == 0 {
		return r.applyStandalone(ctx)
	}

	sys, systemsCount, systemWantCount, err := lookupSystems(r.runners.parent, r.cfg.Systems)
	if err != nil {
		return nil, err
	}

	if systemWantCount < 0 || systemsCount < systemWantCount {
		log.Println("System count mismatch. Run `create` command to create missing instances")
	}

	systemsMap := make(map[types.UID]*systems.SystemInstance)
	for _, list := range sys {
		for i := range list {
			sys := &list[i]
			systemsMap[sys.Info.UID] = sys
		}
	}

	client, err := r.client()
	if err != nil {
		return nil, err
	}

	// lookup runners
	deployments, err := client.ListDeployments(ctx)
	if err != nil {
		return nil, err
	}

	sysDeployments := make(map[types.UID]*appsv1.Deployment)
	var orphaned []*appsv1.Deployment
	for i := range deployments {
		dep := &deployments[i]
		var owner *systems.SystemInstance
		for _, o := range dep.GetOwnerReferences() {
			if sys, ok := systemsMap[o.UID]; ok {
				owner = sys
				break
			}
		}

		if owner == nil {
			orphaned = append(orphaned, dep)
		} else {
			sysDeployments[owner.Info.UID] = dep
		}
	}

	for _, dep := range orphaned {
		if err := client.DeleteInstance(ctx, dep.Name); err != nil {
			log.Printf("Failed to delete orphaned deployment %s: %v\n", dep.Name, err)
		}
	}

	var errs []error

	for uid, sys := range systemsMap {
		if _, ok := sysDeployments[uid]; ok {
			continue
		}

		dep, err := client.CreateInstance(ctx, sys, cfg)
		if err != nil {
			log.Printf("Failed to create runner for system %s: %v\n", sys.Info.Name, err)
			errs = append(errs, err)
		} else {
			i := len(deployments)
			deployments = append(deployments, *dep)
			sysDeployments[uid] = &deployments[i]
		}
	}

	return nil, errors.Join(errs...)
}

func (r *Runner) applyStandalone(ctx context.Context) ([]RunnerSystemInfo, error) {
	// lookup runners
	client, err := r.client()
	if err != nil {
		return nil, err
	}
	deployments, err := client.ListDeployments(ctx)
	if err != nil {
		return nil, err
	}

	if len(deployments) > 0 {
		return nil, nil
	}

	deployment, err := client.CreateStandaloneInstance(ctx)
	if err != nil {
		return nil, err
	}

	return []RunnerSystemInfo{{Name: deployment.Name, Ready: false}}, nil
}

func (r *Runner) Delete(ctx context.Context) error {
	client, err := r.client()
	if err != nil {
		return err
	}
	return client.DeleteAll(ctx)
}

func (r *Runner) List(ctx context.Context) ([]RunnerSystemInfo, error) {
	st, err := r.fetchRunnerSystemStatus(ctx)
	if err != nil {
		return nil, err
	}

	if st.TotalSystems < 0 || st.ActiveSystems < st.TotalSystems {
		log.Println("System count mismatch. Run `create` command to create missing instances")
	}

	var list []RunnerSystemInfo
	for _, dep := range st.RunnerDeployments {
		var owner *systems.SystemInstance
		if sysID, ok := st.Assocs[dep.UID]; ok {
			owner = st.Systems[sysID]
		}

		if owner == nil {
			list = append(list, RunnerSystemInfo{
				Name:  dep.Name,
				Ready: dep.Status.ReadyReplicas == dep.Status.Replicas,
			})
		} else {
			list = append(list, RunnerSystemInfo{
				Name:   dep.Name,
				System: &owner.Info,
				Ready:  dep.Status.ReadyReplicas == dep.Status.Replicas,
			})
		}
	}
	return list, nil
}

func (r *Runner) Status(ctx context.Context) error {
	st, err := r.fetchRunnerSystemStatus(ctx)
	if err != nil {
		return err
	}

	return checkRunnerStatus(&st)
}

func (r *Runner) CancelTask(ctx context.Context) error {
	podGroups, err := r.Pods(ctx)
	if err != nil {
		return err
	}
	mpcs, err := multiProxyClientForPodGroupsPods(r.runners.restConfig, podGroups, "work/stop")
	if err != nil {
		return err
	}
	return mpcs.Post(ctx, "", nil)
}

func (r *Runner) Restart(ctx context.Context) error {
	st, err := r.fetchRunnerSystemStatus(ctx)
	if err != nil {
		return err
	}

	k8client, err := kubernetes.NewForConfig(r.runners.restConfig)
	if err != nil {
		return err
	}

	var errs []error
	for i := range st.RunnerDeployments {
		dep := &st.RunnerDeployments[i]

		// Restart by updating the deployment
		data := fmt.Sprintf(`{"spec": {"template": {"metadata": {"annotations": {"kubectl.kubernetes.io/restartedAt": "%s"}}}}}`, time.Now().Format("20060102150405"))
		_, err := k8client.AppsV1().Deployments(dep.Namespace).Patch(ctx, dep.Name, types.StrategicMergePatchType, []byte(data), metav1.PatchOptions{})
		errs = append(errs, err)
	}

	return errors.Join(errs...)
}

func (r *Runner) Pods(ctx context.Context) ([]PodGroup, error) {
	st, err := r.fetchRunnerSystemStatus(ctx)
	if err != nil {
		return nil, err
	}
	if err = checkRunnerStatus(&st); err != nil {
		return nil, err
	}

	// fetch replica sets for deployments
	k8client, err := kubernetes.NewForConfig(r.runners.restConfig)
	if err != nil {
		return nil, err
	}

	var pods []PodGroup
	for i := range st.RunnerDeployments {
		dep := &st.RunnerDeployments[i]
		rs, err := findDeploymentReplicaSet(ctx, k8client, dep)
		if err != nil {
			return nil, err
		}

		rsPods, err := findReplicasetPods(ctx, k8client, &rs)
		if err != nil {
			return nil, err
		}

		pods = append(pods, PodGroup{
			Deployment: dep,
			System:     st.SystemForDeployment(dep),
			Pods:       rsPods,
		})
	}

	return pods, nil
}

func lookupSystems(br *Client, refs []SystemRef) ([][]systems.SystemInstance, int, int, error) {
	var systemList [][]systems.SystemInstance
	var systemCount int
	var systemWantCount int

	for _, ref := range refs {
		list, count, err := lookupSystemRef(br, ref)
		if err != nil {
			return nil, -1, -1, err
		}

		systemList = append(systemList, list)
		systemCount += len(list)

		if systemWantCount >= 0 {
			if count < 0 {
				systemWantCount = -1
			} else {
				systemWantCount += count
			}
		}
	}

	return systemList, systemCount, systemWantCount, nil
}

func lookupSystemRef(br *Client, ref SystemRef) ([]systems.SystemInstance, int, error) {
	list, count, err := br.Systems().ListByName(context.Background(), ref.Namespace, ref.Name, ref.Kind)
	return list, count, err
}

func (r *Runner) client() (*runnersClient, error) {
	dc, err := deploymentForConfig(r.runners.restConfig, r.cfg.Namespace)
	if err != nil {
		return nil, err
	}

	return &runnersClient{dc, r}, nil
}

func (r *Runner) fetchRunnerSystemStatus(ctx context.Context) (st RunnerSystemStatus, err error) {
	sys, systemsCount, systemWantCount, err := lookupSystems(r.runners.parent, r.cfg.Systems)
	if err != nil {
		return st, err
	}

	fmt.Printf("Systems: %d/%d\n", systemsCount, systemWantCount)

	if systemWantCount < 0 || systemsCount < systemWantCount {
		log.Println("System count mismatch. Run `create` command to create missing instances")
	}

	systemsMap := make(map[types.UID]*systems.SystemInstance)
	assocMap := make(map[types.UID]types.UID)
	reverseAssocMap := make(map[types.UID]types.UID)
	for _, list := range sys {
		for i := range list {
			sys := &list[i]
			systemsMap[sys.Info.UID] = sys
		}
	}

	client, err := r.client()
	if err != nil {
		return st, err
	}

	deployments, err := client.ListDeployments(ctx)
	if err != nil {
		return st, err
	}

	var orphaned []*appsv1.Deployment
	var standalone []*appsv1.Deployment
	for i := range deployments {
		dep := &deployments[i]

		var owner *systems.SystemInstance
		for _, o := range dep.GetOwnerReferences() {
			if sys, ok := systemsMap[o.UID]; ok {
				owner = sys
				break
			}
		}

		if owner == nil {
			isStandalone := false
			if labels := dep.GetLabels(); labels != nil {
				if _, ok := labels[LabelRunnerStandalone]; ok {
					isStandalone = true
				}
			}
			if isStandalone {
				standalone = append(standalone, dep)
			} else {
				orphaned = append(orphaned, dep)
			}
		} else {
			assocMap[dep.UID] = owner.Info.UID
			reverseAssocMap[owner.Info.UID] = dep.UID
		}
	}

	var withoutRunners []*systems.SystemInstance
	for uid, sys := range systemsMap {
		if _, ok := reverseAssocMap[uid]; !ok {
			withoutRunners = append(withoutRunners, sys)
		}
	}

	return RunnerSystemStatus{
		RawSystems:            sys,
		ActiveSystems:         systemsCount,
		TotalSystems:          systemWantCount,
		RunnerDeployments:     deployments,
		Systems:               systemsMap,
		OrphanedDeployments:   orphaned,
		StandaloneDeployments: standalone,
		UnassociatedSystems:   withoutRunners,
		Assocs:                assocMap,
	}, nil
}

func (c *runnersClient) CreateStandaloneInstance(ctx context.Context) (*appsv1.Deployment, error) {
	return c.CreateInstance(ctx, nil, ApplyConfig{})
}

func (c *runnersClient) CreateInstance(ctx context.Context, sys *systems.SystemInstance, cfg ApplyConfig) (*appsv1.Deployment, error) {
	dep, err := c.initDeploymentConfig(sys, cfg)
	if err != nil {
		return nil, err
	}
	return c.Create(ctx, &dep, metav1.CreateOptions{})
}

func (c *runnersClient) initDeploymentConfig(
	sys *systems.SystemInstance,
	cfg ApplyConfig,
) (dep appsv1.Deployment, err error) {
	err = runtime.DefaultUnstructuredConverter.FromUnstructured(map[string]any{
		"metadata": c.runner.cfg.Metadata,
		"spec":     c.runner.cfg.Spec,
	}, &dep)
	if err != nil {
		return dep, fmt.Errorf("parse runner metadata: %w", err)
	}

	labels := dep.GetLabels()
	if labels == nil {
		labels = make(map[string]string)
		dep.SetLabels(labels)
	}

	systemName := "x_standalone_x"
	if sys != nil {
		systemName = sys.Info.Name
	}

	labels[LabelBenchmarkRoleRunner] = c.runner.cfg.Name
	labels[systems.LabelBenchmarkSystem] = systemName
	if sys == nil {
		labels[LabelRunnerStandalone] = "true"
	}

	dep.SetNamespace(c.runner.cfg.Namespace)
	dep.SetGenerateName(c.runner.cfg.Name + "-")
	if sys != nil {
		gvk := sys.Client.GroupVersionKind()
		bTrue := true
		dep.SetOwnerReferences(append(dep.GetOwnerReferences(), metav1.OwnerReference{
			APIVersion: gvk.GroupVersion().String(),
			Kind:       gvk.Kind,
			Name:       sys.Info.Name,
			UID:        sys.Info.UID,
			Controller: &bTrue,
		}))
	}

	podLabels := dep.Spec.Template.GetLabels()
	if podLabels == nil {
		podLabels = make(map[string]string)
		dep.Spec.Template.SetLabels(podLabels)
	}
	podLabels[LabelBenchmarkRoleRunner] = c.runner.cfg.Name
	podLabels[systems.LabelBenchmarkSystem] = systemName
	if sys == nil {
		podLabels[LabelRunnerStandalone] = "true"
	}

	var envVars []corev1.EnvVar
	if sys != nil {
		envVars = sys.EnvVars()
	} else {
		envVars = []corev1.EnvVar{{
			Name:  "STANDALONE",
			Value: "true",
		}}
	}
	if mode := cfg.SSLMode; mode != "" {
		envVars = append(envVars, corev1.EnvVar{
			Name:  "PGSSLMODE",
			Value: mode,
		})
	}

	driverImage := "benchdriver:latest"
	if img := c.runner.cfg.Image; img != "" {
		driverImage = img
	}

	pullPolicy := corev1.PullAlways
	if policy := c.runner.cfg.ImagePullPolicy; policy != "" {
		pullPolicy = policy
	}

	var resources corev1.ResourceRequirements
	err = runtime.DefaultUnstructuredConverter.FromUnstructured(c.runner.cfg.Resources, &resources)
	if err != nil {
		return dep, fmt.Errorf("parse runner resources: %w", err)
	}

	podSpec := &dep.Spec.Template.Spec
	podSpec.Containers = []corev1.Container{
		{
			Name:            "benchdriver",
			Image:           driverImage,
			ImagePullPolicy: pullPolicy,
			Resources:       resources,
			Ports: []corev1.ContainerPort{
				{
					Name:          "api",
					ContainerPort: 8080,
					Protocol:      corev1.ProtocolTCP,
				},
				{
					Name:          "dlv",
					ContainerPort: 2345,
					Protocol:      corev1.ProtocolTCP,
				},
			},
			Env: envVars,
		},
	}

	// copy generated pod labels to deployment selector
	if dep.Spec.Selector == nil {
		dep.Spec.Selector = &metav1.LabelSelector{}
	}
	selectorLabels := dep.Spec.Selector.MatchLabels
	if selectorLabels == nil {
		selectorLabels = make(map[string]string)
		dep.Spec.Selector.MatchLabels = selectorLabels
	}
	for k, v := range podLabels {
		selectorLabels[k] = v
	}

	return dep, nil
}

func (c *runnersClient) DeleteInstance(ctx context.Context, name string) error {
	return c.Delete(ctx, name, metav1.DeleteOptions{})
}

func (c *runnersClient) DeleteAll(ctx context.Context) error {
	return c.DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{
		LabelSelector: metav1.FormatLabelSelector(&metav1.LabelSelector{
			MatchLabels: map[string]string{
				LabelBenchmarkRoleRunner: c.runner.cfg.Name,
			},
		}),
	})
}

func (c *runnersClient) ListDeployments(ctx context.Context) ([]appsv1.Deployment, error) {
	listOpts := metav1.ListOptions{
		LabelSelector: metav1.FormatLabelSelector(&metav1.LabelSelector{
			MatchLabels: map[string]string{
				LabelBenchmarkRoleRunner: c.runner.cfg.Name,
			},
		}),
	}
	list, err := c.List(ctx, listOpts)
	if err != nil {
		return nil, err
	}
	return list.Items, nil
}

func deploymentForConfig(config *rest.Config, namespace string) (appsv1ifc.DeploymentInterface, error) {
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	return client.AppsV1().Deployments(namespace), nil
}

func checkRunnerStatus(st *RunnerSystemStatus) error {
	if st.TotalSystems < 0 || st.ActiveSystems < st.TotalSystems {
		return fmt.Errorf("system count mismatch want=%d, have=%d", st.TotalSystems, st.ActiveSystems)
	}

	if len(st.OrphanedDeployments) > 0 {
		return fmt.Errorf("found %d orphaned deployments", len(st.OrphanedDeployments))
	}

	if len(st.UnassociatedSystems) > 0 {
		return fmt.Errorf("found %d system(s) without runners", len(st.UnassociatedSystems))
	}

	var errs []error
	for _, dep := range st.RunnerDeployments {
		if dep.Status.ReadyReplicas != dep.Status.Replicas {
			errs = append(errs, fmt.Errorf("runner %s is not ready", dep.Name))
		}
	}

	return errors.Join(errs...)
}
