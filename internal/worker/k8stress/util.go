package k8stress

import (
	"context"
	"strconv"

	cnpgapi "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"pgdistbench/internal/worker"
	"pgdistbench/internal/worker/tpcc"
	"pgdistbench/pkg/client/systems/syscnpg"
	"pgdistbench/pkg/cnpgutil"
)

func tpccTaskFactoryFromEndpoint(ep syscnpg.EndpointInfo) *tpcc.Factory {
	instConfig := worker.Config{
		PGHost:     ep.Host,
		PGPort:     strconv.Itoa(ep.Port),
		PGUser:     ep.User,
		PGPass:     ep.Password,
		PGDatabase: "postgres",
		PGSSLMode:  "disable",
	}
	return tpcc.NewFactory(instConfig)
}

func fetchClusterEndpoint(ctx context.Context, k8client *kubernetes.Clientset, inst *cnpgapi.Cluster) (ep syscnpg.EndpointInfo, err error) {
	namespace := inst.GetNamespace()
	secretName := cnpgutil.GuessClusterSecretName(inst)

	secret, err := k8client.CoreV1().Secrets(namespace).Get(ctx, secretName, metav1.GetOptions{})
	if err != nil {
		return ep, err
	}
	return syscnpg.EndpointFromSecret(secret)
}

func chTrySend[T any](ctx context.Context, ch chan<- T, v T) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case ch <- v:
		return nil
	}
}
