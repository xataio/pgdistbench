package systems

import (
	"context"
	"fmt"
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
)

// Systems access the System under Test manager.
//
// TODO: We might support multiple system types like RDS, Aurora, CNPG, custom
// container configuration in the future.
//
// The management, configuration depends on the system type. If an operator is
// involved we can't easily expect a deployment or Pod as a resource to manage.
type Systems struct {
	restConfig *rest.Config
}

type SystemInstance struct {
	Info   SystemInstanceInfo
	Client SystemKindClient
}

type SystemInstanceInfo struct {
	Name   string
	Ready  bool
	Status string
	UID    types.UID
}

type SystemInstanceStatus string

const (
	SystemStatusCreating SystemInstanceStatus = "creating"
	SystemStatusReady    SystemInstanceStatus = "ready"
	SystemStatusNotReady SystemInstanceStatus = "not-ready"
	SystemStatusDeleting SystemInstanceStatus = "deleting"
)

type SystemKind interface {
	Apply(ctx context.Context) ([]SystemInstance, error)
	Delete(ctx context.Context) error
	Status(ctx context.Context) (any, error)
	List(ctx context.Context, namespace, name string) ([]SystemInstance, int, error)
}

type SystemKindClient interface {
	GroupVersionKind() schema.GroupVersionKind
	EnvVars(info SystemInstanceInfo) []corev1.EnvVar
}

type SystemsConfig struct {
	Name      string         `yaml:"name" json:"name"`
	Namespace string         `yaml:"namespace" json:"namespace"`
	Kind      string         `yaml:"kind" json:"kind"`
	Count     int            `yaml:"count" json:"count"`
	Metadata  map[string]any `yaml:"metadata" json:"metadata"`
	Spec      map[string]any `yaml:"spec" json:"spec"`
}

const (
	LabelBenchmarkSystem      = "bench.maki.tech/system"
	LabelBenchmarkSystemIndex = "bench.maki.tech/system-index"
	LabelBenchmarkSystemCount = "bench.maki.tech/system-count"
)

const (
	EnvVarPGHost    = "PGHOST"
	EnvVarPGPort    = "PGPORT"
	EnvVarPGUser    = "PGUSER"
	EnvVarPGPass    = "PGPASS"
	EnvVarPGSSLMode = "PGSSLMODE"
	EnvVarPGDBName  = "PGDATABASE"
)

type SystemFactory func(*rest.Config, SystemsConfig) (SystemKind, error)

type systemRegistry struct {
	lock sync.RWMutex
	reg  map[string]SystemFactory
}

var registry = systemRegistry{
	reg: make(map[string]SystemFactory),
}

func (r *systemRegistry) Find(name string) SystemFactory {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.reg[name]
}

func (r *systemRegistry) Register(name string, factory SystemFactory) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.reg[name] = factory
}

func Register(name string, factory SystemFactory) {
	registry.Register(name, factory)
}

func New(restConfig *rest.Config) *Systems {
	return &Systems{
		restConfig: restConfig,
	}
}

func (s *Systems) initKind(cfg SystemsConfig) (SystemKind, error) {
	factory := registry.Find(strings.ToLower(cfg.Kind))
	if factory == nil {
		return nil, fmt.Errorf("unknown system kind: %s", cfg.Kind)
	}
	return factory(s.restConfig, cfg)
}

func (s *Systems) Apply(ctx context.Context, config SystemsConfig) ([]SystemInstance, error) {
	sk, err := s.initKind(config)
	if err != nil {
		return nil, err
	}
	return sk.Apply(ctx)
}

func (s *Systems) Delete(ctx context.Context, config SystemsConfig) error {
	sk, err := s.initKind(config)
	if err != nil {
		return err
	}
	return sk.Delete(ctx)
}

func (s *Systems) Status(ctx context.Context, config SystemsConfig) (any, error) {
	sk, err := s.initKind(config)
	if err != nil {
		return nil, err
	}
	return sk.Status(ctx)
}

func (s *Systems) List(ctx context.Context, config SystemsConfig) ([]SystemInstance, error) {
	sk, err := s.initKind(config)
	if err != nil {
		return nil, err
	}
	lst, _, err := sk.List(ctx, config.Namespace, config.Name)
	return lst, err
}

func (s *Systems) ListByName(ctx context.Context, namespace string, name string, kind string) (lst []SystemInstance, wantCount int, err error) {
	sk, err := s.initKind(SystemsConfig{
		Kind:      kind,
		Namespace: namespace,
		Name:      name,
	})
	if err != nil {
		return nil, 0, err
	}
	return sk.List(ctx, namespace, name)
}

func (s *SystemInstance) EnvVars() []corev1.EnvVar {
	return s.Client.EnvVars(s.Info)
}
