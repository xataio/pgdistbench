package syscnpg

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"maps"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	cnpgapi "github.com/cloudnative-pg/cloudnative-pg/api/v1"

	"pgdistbench/pkg/client/systems"
	cnpgclient "pgdistbench/pkg/clientset/cnpg/v1"
	"pgdistbench/pkg/clientset/csutil"
	"pgdistbench/pkg/cnpgutil"
	"pgdistbench/pkg/k8util"
)

type cnpgSystem struct {
	restConfig *rest.Config
	system     systems.SystemsConfig
}

type SystemClient struct {
	cnpgclient.ClustersInterface
}

type cnpgInstanceClient struct {
	client  *SystemClient
	cluster cnpgapi.Cluster
}

func init() {
	systems.Register("cnpg", newCNPGSystem)
}

func NewSystemsInstance(client cnpgclient.ClustersInterface, cluster cnpgapi.Cluster) systems.SystemKindClient {
	return &cnpgInstanceClient{
		client:  &SystemClient{client},
		cluster: cluster,
	}
}

func NewSystemClientForConfig(restConfig *rest.Config, namespace string) (*SystemClient, error) {
	ifc, err := cnpgClustersForConfig(restConfig, namespace)
	if err != nil {
		return nil, err
	}
	return &SystemClient{ifc}, nil
}

func (c *cnpgInstanceClient) GroupVersionKind() schema.GroupVersionKind {
	return cnpgapi.GroupVersion.WithKind(cnpgapi.ClusterKind)
}

func (c *cnpgInstanceClient) EnvVars(info systems.SystemInstanceInfo) []corev1.EnvVar {
	secretName := cnpgutil.GuessClusterSecretName(&c.cluster)
	return CreateClusterConnEnvVars(secretName)
}

func newCNPGSystem(restConfig *rest.Config, cfg systems.SystemsConfig) (systems.SystemKind, error) {
	return &cnpgSystem{
		restConfig: restConfig,
		system:     cfg,
	}, nil
}

func (c *cnpgSystem) Apply(ctx context.Context) ([]systems.SystemInstance, error) {
	client, err := c.client(c.system.Namespace)
	if err != nil {
		return nil, err
	}
	clusters, err := client.Apply(ctx, c.system)
	if err != nil {
		return nil, err
	}

	return newInstancesList(client, clusters), nil
}

func newInstancesList(client *SystemClient, clusters []cnpgapi.Cluster) []systems.SystemInstance {
	instances := make([]systems.SystemInstance, len(clusters))
	for i, cluster := range clusters {
		instances[i] = systems.SystemInstance{
			Info: instanceInfoFromCNPGCluster(&cluster),
			Client: &cnpgInstanceClient{
				client:  client,
				cluster: cluster,
			},
		}
	}
	return instances
}

func (c *cnpgSystem) Delete(ctx context.Context) error {
	clusters, err := c.client(c.system.Namespace)
	if err != nil {
		return err
	}
	return clusters.DeleteAll(ctx, c.system.Name)
}

func (c *cnpgSystem) Status(ctx context.Context) (any, error) {
	return nil, fmt.Errorf("not implemented")
}

func (c *cnpgSystem) List(ctx context.Context, namespace, name string) ([]systems.SystemInstance, int, error) {
	fmt.Println("List", namespace, name)

	client, err := c.client(namespace)
	if err != nil {
		return nil, -1, err
	}

	list, err := client.ListClusters(ctx, name)
	if err != nil {
		return nil, -1, err
	}
	if len(list) == 0 {
		return nil, -1, nil
	}

	info := make([]systems.SystemInstance, len(list))
	for i, entry := range list {
		info[i] = systems.SystemInstance{
			Info: instanceInfoFromCNPGCluster(&entry),
			Client: &cnpgInstanceClient{
				client:  client,
				cluster: entry,
			},
		}
	}

	wantCount := -1
	if labels := list[0].GetLabels(); labels != nil {
		countLabel := labels[systems.LabelBenchmarkSystemCount]
		i, err := strconv.Atoi(countLabel)
		if err == nil {
			wantCount = i
		}
	}

	return info, wantCount, nil
}

func (c *cnpgSystem) client(namspace string) (*SystemClient, error) {
	cc, err := cnpgClustersForConfig(c.restConfig, namspace)
	if err != nil {
		return nil, err
	}
	return &SystemClient{cc}, nil
}

func (c *cnpgSystem) hasCNPG(ctx context.Context) error {
	k8client, err := kubernetes.NewForConfig(c.restConfig)
	if err != nil {
		return err
	}
	return cnpgutil.ClusterHasCNPG(ctx, k8client)
}

func (c *SystemClient) Apply(ctx context.Context, config systems.SystemsConfig) ([]cnpgapi.Cluster, error) {
	instanceList, _, err := c.List(ctx, config.Name)
	if err != nil {
		return nil, fmt.Errorf("fetch existing systems (namespace: '%v', name: '%v'): %w",
			config.Namespace, config.Name, err)
	}

	if len(instanceList) == config.Count {
		return instanceList, nil
	}

	if len(instanceList) > config.Count {
		// need to delete some
		for i := config.Count; i < len(instanceList); i++ {
			err = c.DeleteInstance(ctx, instanceList[i].Name)
			if err != nil {
				return nil, fmt.Errorf("delete obsolete cluster %s: %w", instanceList[i].Name, err)
			}
			instanceList[i].Status.Phase = string(systems.SystemStatusDeleting)
		}
		return nil, nil
	}

	// need to create some
	var clusterConfig cnpgapi.Cluster
	err = runtime.DefaultUnstructuredConverter.FromUnstructured(map[string]any{
		"metadata": config.Metadata,
		"spec":     config.Spec,
	}, &clusterConfig)
	if err != nil {
		return nil, fmt.Errorf("convert config to cnpgapi.Cluster: %w", err)
	}
	if clusterConfig.GetLabels() == nil {
		clusterConfig.SetLabels(map[string]string{})
	}

	labels := clusterConfig.GetLabels()
	labels[systems.LabelBenchmarkSystem] = config.Name
	labels[systems.LabelBenchmarkSystemCount] = fmt.Sprintf("%d", config.Count)
	clusterConfig.SetNamespace(config.Namespace)
	clusterConfig.SetGenerateName(config.Name + "-")

	inheritedMetadata := clusterConfig.Spec.InheritedMetadata
	if inheritedMetadata == nil {
		inheritedMetadata = &cnpgapi.EmbeddedObjectMetadata{}
	}
	if inheritedMetadata.Labels == nil {
		inheritedMetadata.Labels = map[string]string{}
	}
	maps.Copy(inheritedMetadata.Labels, labels)

	for i := len(instanceList); i < config.Count; i++ {
		labels[systems.LabelBenchmarkSystemIndex] = fmt.Sprintf("%d", i)

		log.Printf("Creating cluster %s", clusterConfig.GetName())

		{
			// Log cluster configuration as multiline json
			tmp, err := json.MarshalIndent(clusterConfig, "", "  ")
			if err != nil {
				log.Printf("ERROR marshal CNPG Cluster config: %v", err)
			} else {
				log.Printf("Cluster config:\n%s", tmp)
			}
		}

		created, err := c.Create(ctx, &clusterConfig, metav1.CreateOptions{})
		if err != nil {
			return nil, fmt.Errorf("create cluster: %w", err)
		}
		instanceList = append(instanceList, *created)
	}

	return instanceList, nil
}

func (c *SystemClient) List(ctx context.Context, name string) ([]cnpgapi.Cluster, int, error) {
	list, err := c.ListClusters(ctx, name)
	if err != nil {
		return nil, -1, err
	}
	if len(list) == 0 {
		return nil, -1, nil
	}

	wantCount := -1
	if labels := list[0].GetLabels(); labels != nil {
		countLabel := labels[systems.LabelBenchmarkSystemCount]
		i, err := strconv.Atoi(countLabel)
		if err == nil {
			wantCount = i
		}
	}

	return list, wantCount, nil
}

func (c *SystemClient) DeleteInstance(ctx context.Context, name string) error {
	return c.Delete(ctx, name, metav1.DeleteOptions{})
}

func (c *SystemClient) DeleteAll(ctx context.Context, name string) error {
	return c.DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{
		LabelSelector: metav1.FormatLabelSelector(&metav1.LabelSelector{
			MatchLabels: map[string]string{
				systems.LabelBenchmarkSystem: name,
			},
		}),
	})
}

func (c *SystemClient) ListClusters(ctx context.Context, name string) ([]cnpgapi.Cluster, error) {
	listOpts := metav1.ListOptions{
		LabelSelector: metav1.FormatLabelSelector(&metav1.LabelSelector{
			MatchLabels: map[string]string{
				systems.LabelBenchmarkSystem: name,
			},
		}),
	}

	return csutil.CollectList(ctx, c.ClustersInterface, listOpts, func(l *cnpgapi.ClusterList) []cnpgapi.Cluster { return l.Items })
}

func instanceInfoFromCNPGCluster(c *cnpgapi.Cluster) systems.SystemInstanceInfo {
	ready, _ := k8util.CheckCondTrue(c.Status.Conditions, "Ready")
	return systems.SystemInstanceInfo{
		Name:   c.Name,
		UID:    c.UID,
		Ready:  ready,
		Status: c.Status.Phase,
	}
}

func cnpgClustersForConfig(config *rest.Config, namespace string) (cnpgclient.ClustersInterface, error) {
	client, err := cnpgclient.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	return client.Clusters(namespace), nil
}
