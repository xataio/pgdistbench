package static

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"

	"pgdistbench/pkg/client/systems"
	"pgdistbench/pkg/k8util"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	corev1 "k8s.io/api/core/v1"
	k8errors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	corev1ifc "k8s.io/client-go/kubernetes/typed/core/v1"
)

// staticSystem stores a static configuration in a k8s secret.
type staticSystem struct {
	restConfig *rest.Config
	system     systems.SystemsConfig
}

type staticInstanceClient struct {
	corev1ifc.SecretInterface
}

const (
	staticSystemFieldHost    = "host"
	staticSystemFieldPort    = "port"
	staticSystemFieldUser    = "user"
	staticSystemFieldDB      = "database"
	staticSystemFieldPass    = "password"
	staticSystemFieldSSLMode = "sslmode"
)

var staticSystemFields = []string{
	staticSystemFieldHost,
	staticSystemFieldPort,
	staticSystemFieldUser,
	staticSystemFieldPass,
	staticSystemFieldDB,
	staticSystemFieldSSLMode,
}

func init() {
	systems.Register("static", newStaticSystem)
}

func newStaticSystem(restConfig *rest.Config, system systems.SystemsConfig) (systems.SystemKind, error) {
	return &staticSystem{
		restConfig: restConfig,
		system:     system,
	}, nil
}

// Apply creates a new system instance.
func (s *staticSystem) Apply(ctx context.Context) ([]systems.SystemInstance, error) {
	type systemConfig struct {
		Host              string `json:"host"`
		Port              int    `json:"port"`
		User              string `json:"user"`
		Password          string `json:"password"`
		SSLMode           string `json:"sslmode"`
		DB                string `json:"database"`
		UpdateMissingOnly bool   `json:"updateMissingOnly"`
	}
	var config systemConfig
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(s.system.Spec, &config)
	if err != nil {
		return nil, fmt.Errorf("convert config: %w", err)
	}

	fmt.Println("staticSystem config:", config)

	client, err := s.client(s.system.Namespace)
	if err != nil {
		return nil, err
	}

	fmt.Println("get secret", s.system.Name)
	secret, err := client.Get(ctx, s.system.Name, metav1.GetOptions{})
	if err != nil {
		if !k8errors.IsNotFound(err) {
			return nil, fmt.Errorf("lookup secret: %w", err)
		}
		secret, err = nil, nil
	}

	if secret == nil {
		doc := corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      s.system.Name,
				Namespace: s.system.Namespace,
			},
			StringData: map[string]string{
				staticSystemFieldHost:    config.Host,
				staticSystemFieldPort:    fmt.Sprintf("%d", config.Port),
				staticSystemFieldUser:    config.User,
				staticSystemFieldPass:    config.Password,
				staticSystemFieldDB:      config.DB,
				staticSystemFieldSSLMode: config.SSLMode,
			},
		}

		fmt.Println("create secret", s.system.Name)
		secret, err = client.Create(ctx, &doc, metav1.CreateOptions{})
		if err != nil {
			return nil, fmt.Errorf("create secret: %w", err)
		}
	}

	// unpack secret
	updates := make(map[string]string)
	collectFieldUpdate := func(field string, value string) {
		v, err := secretDecode(secret, field)
		if err != nil || v == "" || (!config.UpdateMissingOnly && v != value) {
			updates[field] = value
		}
	}
	collectFieldUpdate(staticSystemFieldHost, config.Host)
	collectFieldUpdate(staticSystemFieldPort, fmt.Sprintf("%d", config.Port))
	collectFieldUpdate(staticSystemFieldUser, config.User)
	collectFieldUpdate(staticSystemFieldPass, config.Password)
	collectFieldUpdate(staticSystemFieldDB, config.DB)
	collectFieldUpdate(staticSystemFieldSSLMode, config.SSLMode)
	if len(updates) > 0 {
		updateSecret := corev1.Secret{
			StringData: make(map[string]string, len(updates)),
		}
		for field, value := range updates {
			updateSecret.StringData[field] = value
		}

		payload, err := json.Marshal(updateSecret)
		if err != nil {
			return nil, fmt.Errorf("marshal patch: %w", err)
		}

		fmt.Println("patch secret", s.system.Name)
		secret, err = client.Patch(ctx, s.system.Name, types.StrategicMergePatchType, payload, metav1.PatchOptions{})
		if err != nil {
			return nil, fmt.Errorf("patch secret: %w", err)
		}
	}

	return []systems.SystemInstance{{
		Info:   staticInstanceInfoFromSecret(secret),
		Client: &staticInstanceClient{client},
	}}, nil
}

func secretDecode(secret *corev1.Secret, key string) (string, error) {
	v, ok := secret.Data[key]
	if !ok {
		return "", nil
	}

	tmp, err := base64.StdEncoding.DecodeString(string(v))
	return string(tmp), err
}

func (s *staticSystem) Delete(ctx context.Context) error {
	client, err := s.client(s.system.Namespace)
	if err != nil {
	}

	err = client.Delete(ctx, s.system.Name, metav1.DeleteOptions{})
	if err != nil && !k8errors.IsNotFound(err) {
		return fmt.Errorf("delete secret: %w", err)
	}
	return nil
}

func (s *staticSystem) Status(ctx context.Context) (any, error) {
	return nil, errors.New("not implemented")
}

func (s *staticSystem) List(ctx context.Context, namespace, name string) ([]systems.SystemInstance, int, error) {
	client, err := s.client(namespace)
	if err != nil {
		return nil, -1, err
	}

	secret, err := client.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if k8errors.IsNotFound(err) {
			return nil, 1, nil
		}
		return nil, -1, fmt.Errorf("get secret: %w", err)
	}

	return []systems.SystemInstance{{
		Info:   staticInstanceInfoFromSecret(secret),
		Client: &staticInstanceClient{client},
	}}, 1, nil
}

func (c *staticInstanceClient) GroupVersionKind() schema.GroupVersionKind {
	return corev1.SchemeGroupVersion.WithKind("Secret")
}

func (c *staticInstanceClient) EnvVars(info systems.SystemInstanceInfo) []corev1.EnvVar {
	secretKey := info.Name
	return []corev1.EnvVar{
		k8util.SecretKeyEnvVar(systems.EnvVarPGHost, secretKey, staticSystemFieldHost),
		k8util.SecretKeyEnvVar(systems.EnvVarPGPort, secretKey, staticSystemFieldPort),
		k8util.SecretKeyEnvVar(systems.EnvVarPGUser, secretKey, staticSystemFieldUser),
		k8util.SecretKeyEnvVar(systems.EnvVarPGPass, secretKey, staticSystemFieldPass),
		k8util.SecretKeyEnvVar(systems.EnvVarPGSSLMode, secretKey, staticSystemFieldSSLMode),
		k8util.SecretKeyEnvVar(systems.EnvVarPGDBName, secretKey, staticSystemFieldDB),
	}
}

func staticInstanceInfoFromSecret(secret *corev1.Secret) systems.SystemInstanceInfo {
	// check all fields present in secret
	status := systems.SystemStatusReady
	for _, name := range staticSystemFields {
		if _, ok := secret.Data[name]; !ok {
			status = systems.SystemStatusNotReady
		}
	}
	return systems.SystemInstanceInfo{
		Name:   secret.Name,
		Ready:  status == systems.SystemStatusNotReady,
		Status: string(status),
		UID:    secret.UID,
	}
}

func (s *staticSystem) client(namespace string) (corev1ifc.SecretInterface, error) {
	k8client, err := kubernetes.NewForConfig(s.restConfig)
	if err != nil {
		return nil, err
	}
	return k8client.CoreV1().Secrets(namespace), nil
}
