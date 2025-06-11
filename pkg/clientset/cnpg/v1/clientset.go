package v1

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/gentype"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"

	cnpgapi "github.com/cloudnative-pg/cloudnative-pg/api/v1"

	"pgdistbench/pkg/clientset/csutil"
)

type CNPGV1Interface interface {
	Clusters(namespace string) ClustersInterface
}

type CNPGV1Client struct {
	restClient rest.Interface
}

type ClustersInterface interface {
	csutil.ResourceInterfaceWithList[*cnpgapi.Cluster, *cnpgapi.ClusterList]
}

func init() {
	scheme.Scheme.AddKnownTypes(cnpgapi.GroupVersion,
		&cnpgapi.Cluster{},
		&cnpgapi.ClusterList{},
	)

	internalVersion := schema.GroupVersion{
		Group:   cnpgapi.GroupVersion.Group,
		Version: runtime.APIVersionInternal,
	}
	scheme.Scheme.AddKnownTypes(internalVersion,
		&cnpgapi.Cluster{},
		&cnpgapi.ClusterList{},
	)

	if err := cnpgapi.AddToScheme(scheme.Scheme); err != nil {
		panic(fmt.Sprintf("failed to add cnpg API to scheme: %v", err))
	}
}

func NewForConfig(config *rest.Config) (*CNPGV1Client, error) {
	rc, err := csutil.APIRestClientFor(config, &schema.GroupVersion{
		Group:   cnpgapi.GroupVersion.Group,
		Version: cnpgapi.GroupVersion.Version,
	})
	if err != nil {
		return nil, err
	}
	return &CNPGV1Client{restClient: rc}, nil
}

func (c *CNPGV1Client) Clusters(namespace string) ClustersInterface {
	return newClustersClient(c.restClient, namespace)
}

type clustersClient struct {
	*gentype.ClientWithList[*cnpgapi.Cluster, *cnpgapi.ClusterList]
}

func newClustersClient(restClient rest.Interface, ns string) *clustersClient {
	return &clustersClient{
		gentype.NewClientWithList(
			"clusters",
			restClient,
			scheme.ParameterCodec,
			ns,
			func() *cnpgapi.Cluster { return &cnpgapi.Cluster{} },
			func() *cnpgapi.ClusterList { return &cnpgapi.ClusterList{} },
			gentype.PrefersProtobuf[*cnpgapi.Cluster](),
		),
	}
}
