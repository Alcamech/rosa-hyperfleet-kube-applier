package informers

import (
	"context"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"

	kubeapplier "github.com/rrp-bot/rosa-hyperfleet-kube-applier/api/kubeapplier"
	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/internal/database"
	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/internal/database/listers"
	hd "github.com/rrp-bot/rosa-hyperfleet-kube-applier/hyperfleet-dynamo/dynamodb"
)

const DefaultResyncPeriod = 30 * time.Second

type KubeApplierInformers interface {
	ApplyDesires() (cache.SharedIndexInformer, listers.ApplyDesireLister)
	ReadDesires() (cache.SharedIndexInformer, listers.ReadDesireLister)
	RunWithContext(ctx context.Context)
}

type kubeApplierInformers struct {
	applyDesireInformer cache.SharedIndexInformer
	applyDesireLister   listers.ApplyDesireLister
	readDesireInformer  cache.SharedIndexInformer
	readDesireLister    listers.ReadDesireLister
}

// NewKubeApplierInformers creates informers that watch the specs DynamoDB
// tables for desire document changes via GSI-based polling (two-speed engine).
// specsClient is the DynamoDB client for the specs tables.
// specsPrefix is the table name prefix (full table names are
// prefix+"-applydesires" / prefix+"-readdesires").
func NewKubeApplierInformers(
	specsClient *dynamodb.Client,
	specsPrefix string,
) KubeApplierInformers {
	return NewKubeApplierInformersWithResyncPeriod(specsClient, specsPrefix, DefaultResyncPeriod)
}

func NewKubeApplierInformersWithResyncPeriod(
	specsClient *dynamodb.Client,
	specsPrefix string,
	resyncPeriod time.Duration,
) KubeApplierInformers {
	return NewKubeApplierInformersWithOptions(specsClient, specsPrefix, resyncPeriod, hd.Options{})
}

// NewKubeApplierInformersWithOptions is like NewKubeApplierInformersWithResyncPeriod
// but also accepts explicit watcher Options (poll/relist intervals). Primarily
// useful for integration tests that need tighter polling intervals.
func NewKubeApplierInformersWithOptions(
	specsClient *dynamodb.Client,
	specsPrefix string,
	resyncPeriod time.Duration,
	watcherOpts hd.Options,
) KubeApplierInformers {
	applyTable := specsPrefix + database.TableSuffixApplyDesires
	readTable := specsPrefix + database.TableSuffixReadDesires

	applyInf := newDesireInformer(
		specsClient,
		applyTable,
		&kubeapplier.ApplyDesire{},
		func(ctx context.Context) (runtime.Object, error) {
			specReader := database.NewDynamoDBKubeApplierDBClient(specsClient, specsClient, specsPrefix, specsPrefix).ApplyDesireSpecs()
			items, err := specReader.List(ctx)
			if err != nil {
				return nil, err
			}
			list := &kubeapplier.ApplyDesireList{}
			list.ResourceVersion = "0"
			for _, d := range items {
				list.Items = append(list.Items, *d)
			}
			return list, nil
		},
		func(item hd.Item) (runtime.Object, error) {
			return database.ItemToApplyDesire(item)
		},
		resyncPeriod,
		watcherOpts,
	)

	readInf := newDesireInformer(
		specsClient,
		readTable,
		&kubeapplier.ReadDesire{},
		func(ctx context.Context) (runtime.Object, error) {
			specReader := database.NewDynamoDBKubeApplierDBClient(specsClient, specsClient, specsPrefix, specsPrefix).ReadDesireSpecs()
			items, err := specReader.List(ctx)
			if err != nil {
				return nil, err
			}
			list := &kubeapplier.ReadDesireList{}
			list.ResourceVersion = "0"
			for _, d := range items {
				list.Items = append(list.Items, *d)
			}
			return list, nil
		},
		func(item hd.Item) (runtime.Object, error) {
			return database.ItemToReadDesire(item)
		},
		resyncPeriod,
		watcherOpts,
	)

	return &kubeApplierInformers{
		applyDesireInformer: applyInf,
		applyDesireLister:   listers.NewApplyDesireLister(applyInf.GetIndexer()),
		readDesireInformer:  readInf,
		readDesireLister:    listers.NewReadDesireLister(readInf.GetIndexer()),
	}
}

func newDesireInformer(
	dbClient *dynamodb.Client,
	tableName string,
	exampleObj runtime.Object,
	listFn func(context.Context) (runtime.Object, error),
	decodeFn func(hd.Item) (runtime.Object, error),
	resyncPeriod time.Duration,
	watcherOpts hd.Options,
) cache.SharedIndexInformer {
	lw := &cache.ListWatch{
		ListWithContextFunc: func(ctx context.Context, _ metav1.ListOptions) (runtime.Object, error) {
			return listFn(ctx)
		},
		WatchFuncWithContext: func(ctx context.Context, _ metav1.ListOptions) (watch.Interface, error) {
			return hd.NewWatchAdapter(ctx, dbClient, tableName, watcherOpts, decodeFn), nil
		},
	}
	return cache.NewSharedIndexInformerWithOptions(
		hd.ListWatchWithoutWatchListSemantics{ListWatch: lw},
		exampleObj,
		cache.SharedIndexInformerOptions{
			ResyncPeriod: resyncPeriod,
		},
	)
}

func (k *kubeApplierInformers) ApplyDesires() (cache.SharedIndexInformer, listers.ApplyDesireLister) {
	return k.applyDesireInformer, k.applyDesireLister
}

func (k *kubeApplierInformers) ReadDesires() (cache.SharedIndexInformer, listers.ReadDesireLister) {
	return k.readDesireInformer, k.readDesireLister
}

func (k *kubeApplierInformers) RunWithContext(ctx context.Context) {
	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		defer wg.Done()
		k.applyDesireInformer.RunWithContext(ctx)
	}()
	go func() {
		defer wg.Done()
		k.readDesireInformer.RunWithContext(ctx)
	}()

	<-ctx.Done()
	wg.Wait()
}
