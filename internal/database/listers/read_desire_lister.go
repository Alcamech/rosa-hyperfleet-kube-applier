package listers

import (
	"k8s.io/client-go/tools/cache"

	kubeapplier "github.com/openshift-online/rosa-hyperfleet-kube-applier/api/kubeapplier"
)

type ReadDesireLister interface {
	List() ([]*kubeapplier.ReadDesire, error)
	Get(documentID string) (*kubeapplier.ReadDesire, error)
}

type readDesireLister struct {
	indexer cache.Indexer
}

func NewReadDesireLister(indexer cache.Indexer) ReadDesireLister {
	return &readDesireLister{indexer: indexer}
}

func (l *readDesireLister) List() ([]*kubeapplier.ReadDesire, error) {
	return listAll[kubeapplier.ReadDesire](l.indexer)
}

func (l *readDesireLister) Get(documentID string) (*kubeapplier.ReadDesire, error) {
	return getByKey[kubeapplier.ReadDesire](l.indexer, documentID)
}
