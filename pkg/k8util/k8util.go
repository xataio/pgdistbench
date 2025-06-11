package k8util

import (
	"context"
	"fmt"
	"iter"
	"pgdistbench/pkg/clientset/csutil"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
)

// checkCRDExists checks if a CRD with the given name exists in the cluster.
func CheckCRDExists(ctx context.Context, clientset *kubernetes.Clientset, crdName string) error {
	// Access the APIExtensions client to list CRDs
	_, err := clientset.RESTClient().Get().
		AbsPath("/apis/apiextensions.k8s.io/v1/customresourcedefinitions").
		DoRaw(ctx)
	if err != nil {
		return fmt.Errorf("failed to list CRDs: %w", err)
	}
	return nil
}

func SecretKeyEnvVar(env, secret, name string) corev1.EnvVar {
	return corev1.EnvVar{
		Name: env,
		ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{
				Key: name,
				LocalObjectReference: corev1.LocalObjectReference{
					Name: secret,
				},
			},
		},
	}
}

func FindCondition(conds []metav1.Condition, name string) (*metav1.Condition, bool) {
	for i := range conds {
		if conds[i].Type == name {
			return &conds[i], true
		}
	}
	return nil, false
}

func CheckCondStatus(conds []metav1.Condition, name string, status metav1.ConditionStatus) (match, exists bool) {
	cond, exists := FindCondition(conds, name)
	if !exists {
		return false, false
	}
	return cond.Status == status, true
}

func CheckCondTrue(conds []metav1.Condition, name string) (bool, bool) {
	cond, exists := FindCondition(conds, name)
	if !exists {
		return false, false
	}
	return cond.Status == metav1.ConditionTrue, true
}

func CheckCondFalse(conds []metav1.Condition, name string) (bool, bool) {
	cond, exists := FindCondition(conds, name)
	if !exists {
		return false, false
	}
	return cond.Status == metav1.ConditionFalse, true
}

type Watcher[T any] struct {
	err error
	ifc watch.Interface
}

func NewWatcher[T any](ctx context.Context, client csutil.ResourceWatch, opts metav1.ListOptions) Watcher[T] {
	ifc, err := client.Watch(ctx, opts)
	if err != nil {
		return Watcher[T]{err: err}
	}
	return Watcher[T]{ifc: ifc}
}

func NewWatcherFromInterface[T any](ctx context.Context, ifc watch.Interface) Watcher[T] {
	return Watcher[T]{ifc: ifc}
}

func (w *Watcher[T]) Next(ctx context.Context) (watch.EventType, T, bool) {
	var zero T
	if w.err != nil {
		return "", zero, false
	}

	if err := ctx.Err(); err != nil {
		w.err = err
		return "", zero, false
	}

	select {
	case <-ctx.Done():
		w.err = ctx.Err()
		return "", zero, false
	case event, ok := <-w.ifc.ResultChan():
		if !ok {
			return "", zero, false
		}

		if event.Type == "ERROR" {
			if err, ok := event.Object.(error); ok && err != nil {
				w.err = fmt.Errorf("watcher error: %w", err)
			} else {
				w.err = fmt.Errorf("watcher error: %v", event.Object)
			}
			return "", zero, false
		}

		doc, ok := event.Object.(T)
		if !ok {
			w.err = fmt.Errorf("unexpected type: %T", event.Object)
			return "", zero, false
		}

		return event.Type, doc, true
	}
}

func (w *Watcher[T]) Err() error {
	return w.err
}

func (w *Watcher[T]) Iter(ctx context.Context) iter.Seq2[watch.EventType, T] {
	return func(yield func(watch.EventType, T) bool) {
		for {
			event, doc, ok := w.Next(ctx)
			if !ok || !yield(event, doc) {
				return
			}
		}
	}
}
