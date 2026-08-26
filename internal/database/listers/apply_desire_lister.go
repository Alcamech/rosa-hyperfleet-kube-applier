package listers

import (
	"k8s.io/client-go/tools/cache"

	kubeapplier "github.com/openshift-online/rosa-hyperfleet-kube-applier/api/kubeapplier"
)

type ApplyDesireLister interface {
	List() ([]*kubeapplier.ApplyDesire, error)
	Get(documentID string) (*kubeapplier.ApplyDesire, error)
}

type applyDesireLister struct {
	indexer cache.Indexer
}

func NewApplyDesireLister(indexer cache.Indexer) ApplyDesireLister {
	return &applyDesireLister{indexer: indexer}
}

func (l *applyDesireLister) List() ([]*kubeapplier.ApplyDesire, error) {
	return listAll[kubeapplier.ApplyDesire](l.indexer)
}

func (l *applyDesireLister) Get(documentID string) (*kubeapplier.ApplyDesire, error) {
	return getByKey[kubeapplier.ApplyDesire](l.indexer, documentID)
}
