package cnpgutil

import (
	"context"
	"errors"
	"fmt"
	v1 "pgdistbench/pkg/clientset/cnpg/v1"
	"pgdistbench/pkg/k8util"
	"time"

	cnpgapi "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/kubernetes"
)

func ClusterHasCNPG(ctx context.Context, k8client *kubernetes.Clientset) error {
	err := k8util.CheckCRDExists(ctx, k8client, "clusters.postgresql.cnpg.io")
	if err != nil {
		return fmt.Errorf("check CNPG available: %w", err)
	}
	return nil
}

func GuessClusterSecretName(inst *cnpgapi.Cluster) string {
	if inst.GetEnableSuperuserAccess() {
		return inst.Name + "-superuser"
	}

	if bootstrap := inst.Spec.Bootstrap; bootstrap != nil {
		if initDB := bootstrap.InitDB; initDB != nil {
			if secret := initDB.Secret; secret != nil {
				return secret.Name
			}

			if owner := initDB.Owner; owner != "" {
				return inst.Name + "-" + owner
			}
		}
	}

	return inst.Name + "-app"
}

func WaitClusterReady(ctx context.Context, client v1.ClustersInterface, name, namespace string, timeout time.Duration) error {
	opts := metav1.ListOptions{
		FieldSelector: fields.SelectorFromSet(map[string]string{
			"metadata.name":      name,
			"metadata.namespace": namespace,
		}).String(),
	}

	if timeout > 0 {
		timeoutSecs := int64(timeout.Seconds())
		opts.TimeoutSeconds = &timeoutSecs
	}

	watcher := k8util.NewWatcher[*cnpgapi.Cluster](ctx, client, opts)
	for eventType, doc := range watcher.Iter(ctx) {
		if eventType == "DELETED" {
			return fmt.Errorf("cluster %s deleted", doc.Name)
		}

		if ClusterReady(doc) {
			return nil
		}
	}

	err := watcher.Err()
	if err != nil {
		return fmt.Errorf("watch cluster: %w", err)
	}
	return errors.New("Cluster is not ready yet")
}

func ClusterReady(cluster *cnpgapi.Cluster) bool {
	ready, _ := k8util.CheckCondTrue(cluster.Status.Conditions, "Ready")
	return ready
}
