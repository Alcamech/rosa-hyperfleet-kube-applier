package listers

import (
	"fmt"

	"k8s.io/client-go/tools/cache"

	"github.com/openshift-online/rosa-hyperfleet-kube-applier/internal/database"
)

func listAll[T any](store cache.Store) ([]*T, error) {
	items := store.List()
	result := make([]*T, 0, len(items))
	for _, item := range items {
		typed, ok := item.(*T)
		if !ok {
			return nil, fmt.Errorf("expected *%T, got %T", *new(T), item)
		}
		result = append(result, typed)
	}
	return result, nil
}

func getByKey[T any](indexer cache.Indexer, key string) (*T, error) {
	item, exists, err := indexer.GetByKey(key)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, database.NewNotFoundError()
	}
	typed, ok := item.(*T)
	if !ok {
		return nil, fmt.Errorf("expected *%T, got %T", *new(T), item)
	}
	return typed, nil
}
