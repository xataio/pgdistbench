package csutil

import (
	"context"
	"fmt"
	"log"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
)

type ResourceInterfaceWithList[T any, List any] interface {
	ResourceInterface[T]
	ResourceList[List]
	ResourceWatch
}

type ResourceInterface[T any] interface {
	Create(ctx context.Context, item T, opts metav1.CreateOptions) (T, error)
	Update(ctx context.Context, item T, opts metav1.UpdateOptions) (T, error)
	UpdateStatus(ctx context.Context, item T, opts metav1.UpdateOptions) (T, error)
	Delete(ctx context.Context, name string, opts metav1.DeleteOptions) error
	DeleteCollection(ctx context.Context, opts metav1.DeleteOptions, listOpts metav1.ListOptions) error
	Get(ctx context.Context, name string, opts metav1.GetOptions) (T, error)
	Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts metav1.PatchOptions, subresources ...string) (result T, err error)
}

type ResourceList[List any] interface {
	List(ctx context.Context, opts metav1.ListOptions) (List, error)
}

type ResourceWatch interface {
	Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error)
}

func APIRestClientFor(config *rest.Config, groupVersion *schema.GroupVersion) (*rest.RESTClient, error) {
	apiConfig := *config
	apiConfig.GroupVersion = groupVersion
	apiConfig.ContentType = "application/json"
	apiConfig.APIPath = "/apis"
	apiConfig.NegotiatedSerializer = serializer.NewCodecFactory(scheme.Scheme)
	apiConfig.UserAgent = rest.DefaultKubernetesUserAgent()
	return rest.RESTClientFor(&apiConfig)
}

type ListInterface[T any] interface {
	GetRemainingItemCount() *int64
	GetContinue() string
}

func CollectList[T any, List ListInterface[T]](
	ctx context.Context,
	client ResourceList[List],
	opts metav1.ListOptions,
	getItems func(List) []T,
) ([]T, error) {
	list, err := client.List(ctx, opts)
	if err != nil {
		if de, ok := err.(interface{ DebugError() (string, any) }); ok {
			log.Printf(de.DebugError())
		}

		log.Printf("List error (%T): %v", err, err)
		return nil, fmt.Errorf("list first page: %w", err)
	}

	items := getItems(list)
	for rc := list.GetRemainingItemCount(); rc != nil && *rc > 0; rc = list.GetRemainingItemCount() {
		opts.Continue = list.GetContinue()
		list, err = client.List(ctx, opts)
		if err != nil {
			return nil, fmt.Errorf("list next page: %w", err)
		}

		items = append(items, getItems(list)...)
	}

	return items, nil
}
