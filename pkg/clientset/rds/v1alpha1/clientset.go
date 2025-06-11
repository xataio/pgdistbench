package v1alpha1

import (
	"pgdistbench/pkg/clientset/csutil"

	rdsapi "github.com/aws-controllers-k8s/rds-controller/apis/v1alpha1"
	"k8s.io/client-go/gentype"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
)

type RDSV1Alpha1Interface interface {
	DBInstances(namespace string) DBInstancesInterface
	DBSubnetGroups(namespace string) DBSubnetGroupsInterface
}

type RDSV1Alpha1Client struct {
	restClient rest.Interface
}

type DBInstancesInterface interface {
	csutil.ResourceInterfaceWithList[*rdsapi.DBInstance, *rdsapi.DBInstanceList]
}

type DBSubnetGroupsInterface interface {
	csutil.ResourceInterfaceWithList[*rdsapi.DBSubnetGroup, *rdsapi.DBSubnetGroupList]
}

func NewForConfig(config *rest.Config) (RDSV1Alpha1Interface, error) {
	rc, err := csutil.APIRestClientFor(config, &rdsapi.GroupVersion)
	if err != nil {
		return nil, err
	}
	return &RDSV1Alpha1Client{restClient: rc}, nil
}

func (c *RDSV1Alpha1Client) DBInstances(namespace string) DBInstancesInterface {
	return newInstances(c.restClient, namespace)
}

func (c *RDSV1Alpha1Client) DBSubnetGroups(namespace string) DBSubnetGroupsInterface {
	return newSubnetGroups(c.restClient, namespace)
}

type instancesClient struct {
	*gentype.ClientWithList[*rdsapi.DBInstance, *rdsapi.DBInstanceList]
}

func newInstances(restClient rest.Interface, namespace string) *instancesClient {
	return &instancesClient{
		ClientWithList: gentype.NewClientWithList(
			"dbinstances",
			restClient,
			scheme.ParameterCodec,
			namespace,
			func() *rdsapi.DBInstance { return &rdsapi.DBInstance{} },
			func() *rdsapi.DBInstanceList { return &rdsapi.DBInstanceList{} },
			gentype.PrefersProtobuf[*rdsapi.DBInstance](),
		),
	}
}

type subnetGroupsClient struct {
	*gentype.ClientWithList[*rdsapi.DBSubnetGroup, *rdsapi.DBSubnetGroupList]
}

func newSubnetGroups(restClient rest.Interface, namespace string) *subnetGroupsClient {
	return &subnetGroupsClient{
		ClientWithList: gentype.NewClientWithList(
			"dbsubnetgroups",
			restClient,
			scheme.ParameterCodec,
			namespace,
			func() *rdsapi.DBSubnetGroup { return &rdsapi.DBSubnetGroup{} },
			func() *rdsapi.DBSubnetGroupList { return &rdsapi.DBSubnetGroupList{} },
			gentype.PrefersProtobuf[*rdsapi.DBSubnetGroup](),
		),
	}
}
