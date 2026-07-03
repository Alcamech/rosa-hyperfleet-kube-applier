// Package delete_desire implements the DeleteDesireController.
//
// On every sync the controller resolves spec.targetItem to a GVR, gets the
// object, and either:
//   - reports Successful=True if the object is gone,
//   - reports Successful=False (WaitingForDeletion) if it's there and has a
//     deletionTimestamp (or after issuing a delete that succeeded but the
//     object hasn't fully gone away yet because of finalizers), or
//   - issues a delete and re-checks the same way.
package delete_desire

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	utilsclock "k8s.io/utils/clock"

	"github.com/rrp-bot/kube-applier-aws/internal/api/kubeapplier"
	controllerutil "github.com/rrp-bot/kube-applier-aws/internal/controllerutils"
	"github.com/rrp-bot/kube-applier-aws/internal/database"
	"github.com/rrp-bot/kube-applier-aws/pkg/controllers/conditions"
	"github.com/rrp-bot/kube-applier-aws/pkg/controllers/desirestatuswriter"
	"github.com/rrp-bot/kube-applier-aws/pkg/controllers/keys"
)

// DefaultCooldownPeriod is the minimum interval between two reconciles of a
// DeleteDesire whose UpdateTime has not changed. 1 minute is shorter than
// apply_desire's 10-minute default because a DeleteDesire in the
// WaitingForDeletion state needs to keep checking whether finalizers have
// completed.
const DefaultCooldownPeriod = 1 * time.Minute

type Config struct {
	CooldownPeriod time.Duration
	Clock          utilsclock.PassiveClock
}

func (c Config) withDefaults() Config {
	if c.CooldownPeriod == 0 {
		c.CooldownPeriod = DefaultCooldownPeriod
	}
	if c.Clock == nil {
		c.Clock = utilsclock.RealClock{}
	}
	return c
}

// completedCacheKey identifies a specific version of a DeleteDesire that has
// reached Successful=True. Including the UpdateTime means a re-created desire
// with the same documentID but a newer UpdateTime will never match the stale
// entry, so the cache is self-correcting without relying on DeleteFunc events.
type completedCacheKey struct {
	documentID string
	updateTime time.Time
}

// DeleteDesireController reconciles DeleteDesires by deleting their target items
// and reporting WaitingForDeletion until the items actually disappear.
type DeleteDesireController struct {
	name                 string
	deleteDesireInformer cache.SharedIndexInformer
	specFetcher          *deleteDesireSpecFetcher
	dyn                  dynamic.Interface
	writer               desirestatuswriter.StatusWriter[kubeapplier.DeleteDesire, keys.DeleteDesireKey]
	statusCRUD           database.ResourceCRUD[kubeapplier.DeleteDesire]
	queue                workqueue.TypedRateLimitingInterface[keys.DeleteDesireKey]

	cfg      Config
	cooldown controllerutil.CooldownChecker

	// completedCache tracks (documentID, updateTime) pairs for DeleteDesires
	// that have reached Successful=True. SyncOnce short-circuits immediately
	// for any key found here, avoiding redundant kube-apiserver GETs for
	// already-completed deletes. Keying by updateTime means a re-created desire
	// with the same documentID but a new updateTime will never hit a stale
	// entry — no reliance on DeleteFunc eviction for correctness.
	// Populated at startup by PreloadCompletedCache and updated incrementally.
	mu             sync.RWMutex
	completedCache map[completedCacheKey]bool
}

// NewDeleteDesireController wires up the informer event handler and returns a
// ready-to-Run controller.
func NewDeleteDesireController(
	deleteDesireInformer cache.SharedIndexInformer,
	dyn dynamic.Interface,
	specReader database.SpecReader[kubeapplier.DeleteDesire],
	statusCRUD database.ResourceCRUD[kubeapplier.DeleteDesire],
	cfg Config,
) (*DeleteDesireController, error) {
	cfg = cfg.withDefaults()
	specFetcher := &deleteDesireSpecFetcher{reader: specReader}
	statusFetcher := &deleteDesireStatusFetcher{crud: statusCRUD}
	cooldownChecker := controllerutil.NewTimeBasedCooldownChecker(cfg.CooldownPeriod)
	cooldownChecker.SetClock(cfg.Clock)
	c := &DeleteDesireController{
		name:                 "DeleteDesireController",
		deleteDesireInformer: deleteDesireInformer,
		specFetcher:          specFetcher,
		dyn:                  dyn,
		writer: desirestatuswriter.New[kubeapplier.DeleteDesire, keys.DeleteDesireKey, *kubeapplier.DeleteDesire](
			statusFetcher,
			&deleteDesireReplacer{crud: statusCRUD},
			&deleteDesireCreator{crud: statusCRUD},
		),
		statusCRUD: statusCRUD,
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[keys.DeleteDesireKey](),
			workqueue.TypedRateLimitingQueueConfig[keys.DeleteDesireKey]{Name: "DeleteDesireController"},
		),
		cfg:            cfg,
		cooldown:       cooldownChecker,
		completedCache: make(map[completedCacheKey]bool),
	}

	if _, err := deleteDesireInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { c.handleAdd(obj) },
		UpdateFunc: func(oldObj, newObj any) { c.handleUpdate(oldObj, newObj) },
		DeleteFunc: func(obj any) { c.handleDelete(obj) },
	}); err != nil {
		return nil, fmt.Errorf("register informer handler: %w", err)
	}
	return c, nil
}

func (c *DeleteDesireController) Run(ctx context.Context, threadiness int) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	logger := klog.FromContext(ctx).WithName(c.name)
	ctx = klog.NewContext(ctx, logger)
	logger.Info("starting controller")
	defer logger.Info("stopped controller")

	for i := 0; i < threadiness; i++ {
		go wait.UntilWithContext(ctx, c.runWorker, time.Second)
	}
	<-ctx.Done()
}

func (c *DeleteDesireController) handleAdd(obj any) {
	d, ok := obj.(*kubeapplier.DeleteDesire)
	if !ok {
		return
	}
	c.enqueue(d)
}

func (c *DeleteDesireController) handleDelete(obj any) {
	// Support tombstone objects delivered by the informer on cache resync.
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tombstone.Obj
	}
	d, ok := obj.(*kubeapplier.DeleteDesire)
	if !ok {
		return
	}
	// Best-effort eviction from the completed cache to reclaim memory.
	// Correctness does not depend on this: if the event is missed, the next
	// SyncOnce for a re-created desire will use a different updateTime and
	// therefore never match the stale entry.
	c.mu.Lock()
	delete(c.completedCache, completedCacheKey{documentID: d.GetDocumentID(), updateTime: d.GetUpdateTime()})
	c.mu.Unlock()

	// Best-effort: delete the corresponding status record so orphaned status
	// entries don't accumulate in the status table indefinitely.
	if err := c.statusCRUD.Delete(context.Background(), d.GetDocumentID()); err != nil && !database.IsNotFoundError(err) {
		klog.ErrorS(err, "failed to delete status record for deleted DeleteDesire", "documentID", d.GetDocumentID())
	}
}

func (c *DeleteDesireController) handleUpdate(oldObj, newObj any) {
	oldD, oldOK := oldObj.(*kubeapplier.DeleteDesire)
	newD, newOK := newObj.(*kubeapplier.DeleteDesire)
	if !oldOK || !newOK {
		return
	}
	if !oldD.UpdateTime.Equal(newD.UpdateTime) {
		c.enqueue(newD)
		return
	}
	key, err := keys.DeleteDesireKeyFromDesire(newD)
	if err != nil {
		utilruntime.HandleError(err)
		return
	}
	if !c.cooldown.CanSync(context.TODO(), key) {
		return
	}
	c.queue.Add(key)
}

func (c *DeleteDesireController) enqueue(d *kubeapplier.DeleteDesire) {
	key, err := keys.DeleteDesireKeyFromDesire(d)
	if err != nil {
		utilruntime.HandleError(err)
		return
	}
	c.queue.Add(key)
}

func (c *DeleteDesireController) runWorker(ctx context.Context) {
	for c.processNext(ctx) {
	}
}

func (c *DeleteDesireController) processNext(ctx context.Context) bool {
	key, shutdown := c.queue.Get()
	if shutdown {
		return false
	}
	defer c.queue.Done(key)

	if err := c.SyncOnce(ctx, key); err != nil {
		utilruntime.HandleErrorWithContext(ctx, err, "sync error; requeuing", "key", key)
		c.queue.AddRateLimited(key)
		return true
	}
	c.queue.Forget(key)
	return true
}

// PreloadCompletedCache performs a one-time scan of the status table on
// startup and marks any DeleteDesire that already has Successful=True as
// complete in the in-memory cache. This prevents redundant kube-apiserver
// GETs for deletes that finished before (or during a previous run of) this
// process. Returns a fatal error if the status table cannot be listed.
func (c *DeleteDesireController) PreloadCompletedCache(ctx context.Context, statusReader database.ResourceCRUD[kubeapplier.DeleteDesire]) error {
	logger := klog.FromContext(ctx).WithName(c.name)
	items, err := statusReader.List(ctx)
	if err != nil {
		return fmt.Errorf("listing delete desire statuses for completed cache preload: %w", err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, item := range items {
		cond := meta.FindStatusCondition(item.Status.Conditions, kubeapplier.ConditionTypeSuccessful)
		if cond != nil && cond.Status == metav1.ConditionTrue {
			c.completedCache[completedCacheKey{documentID: item.GetDocumentID(), updateTime: item.Status.ObservedDesireUpdateTime}] = true
		}
	}
	logger.Info("preloaded completed cache", "total", len(items), "completed", len(c.completedCache))
	return nil
}

// SyncOnce performs a single reconcile pass for the named DeleteDesire.
func (c *DeleteDesireController) SyncOnce(ctx context.Context, key keys.DeleteDesireKey) error {
	desire, err := c.specFetcher.Fetch(ctx, key)
	if database.IsNotFoundError(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if desire == nil {
		return nil
	}

	// Short-circuit: if this exact (documentID, updateTime) pair is already
	// known to be Successful=True, skip all kube-apiserver calls. Using
	// updateTime as part of the key means a re-created desire with the same
	// documentID but a newer updateTime will never match a stale entry — no
	// reliance on DeleteFunc eviction for correctness.
	ck := completedCacheKey{documentID: desire.GetDocumentID(), updateTime: desire.GetUpdateTime()}
	c.mu.RLock()
	done := c.completedCache[ck]
	c.mu.RUnlock()
	if done {
		return nil
	}

	mutate, _ := c.evaluate(ctx, desire)
	if err := c.writer.UpdateStatus(ctx, key, func(d *kubeapplier.DeleteDesire) {
		d.SetDocumentID(desire.GetDocumentID())
		d.Status.ObservedDesireUpdateTime = desire.GetUpdateTime()
		mutate(d)
	}); err != nil {
		return err
	}

	// Check whether this reconcile resulted in Successful=True by applying
	// the mutate to a throwaway value. If so, mark it in the completed cache
	// so future resyncs skip it without touching the kube-apiserver.
	probe := &kubeapplier.DeleteDesire{}
	mutate(probe)
	cond := meta.FindStatusCondition(probe.Status.Conditions, kubeapplier.ConditionTypeSuccessful)
	if cond != nil && cond.Status == metav1.ConditionTrue {
		c.mu.Lock()
		c.completedCache[ck] = true
		c.mu.Unlock()
	}

	return nil
}

// evaluate runs the state machine for one DeleteDesire.
func (c *DeleteDesireController) evaluate(ctx context.Context, d *kubeapplier.DeleteDesire) (desirestatuswriter.MutateFunc[kubeapplier.DeleteDesire], error) {
	target := d.Spec.TargetItem
	if len(target.Resource) == 0 || len(target.Version) == 0 || len(target.Name) == 0 {
		err := conditions.NewPreCheckError(errors.New("spec.targetItem requires version, resource, and name"))
		return func(d *kubeapplier.DeleteDesire) {
			conditions.SetSuccessful(&d.Status.Conditions, err)
			conditions.SetDegraded(&d.Status.Conditions, classifyAsDegraded(err))
		}, err
	}

	gvr := schema.GroupVersionResource{Group: target.Group, Version: target.Version, Resource: target.Resource}
	resource := c.dyn.Resource(gvr)
	var kubeResourceAccessor dynamic.ResourceInterface = resource
	if len(target.Namespace) > 0 {
		kubeResourceAccessor = resource.Namespace(target.Namespace)
	}

	got, getErr := kubeResourceAccessor.Get(ctx, target.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(getErr) {
		return func(d *kubeapplier.DeleteDesire) {
			conditions.SetSuccessful(&d.Status.Conditions, nil)
			conditions.SetDegraded(&d.Status.Conditions, nil)
		}, nil
	}
	if getErr != nil {
		err := fmt.Errorf("get target: %w", getErr)
		return func(d *kubeapplier.DeleteDesire) {
			conditions.SetSuccessful(&d.Status.Conditions, err)
			conditions.SetDegraded(&d.Status.Conditions, classifyAsDegraded(err))
		}, err
	}

	if dt := got.GetDeletionTimestamp(); dt != nil {
		uid := got.GetUID()
		return func(d *kubeapplier.DeleteDesire) {
			conditions.SetSuccessfulWaitingForDeletion(&d.Status.Conditions, *dt, uid)
			conditions.SetDegraded(&d.Status.Conditions, nil)
		}, nil
	}

	if delErr := kubeResourceAccessor.Delete(ctx, target.Name, metav1.DeleteOptions{}); delErr != nil {
		if apierrors.IsNotFound(delErr) {
			return func(d *kubeapplier.DeleteDesire) {
				conditions.SetSuccessful(&d.Status.Conditions, nil)
				conditions.SetDegraded(&d.Status.Conditions, nil)
			}, nil
		}
		err := fmt.Errorf("delete target: %w", delErr)
		return func(d *kubeapplier.DeleteDesire) {
			conditions.SetSuccessful(&d.Status.Conditions, err)
			conditions.SetDegraded(&d.Status.Conditions, classifyAsDegraded(err))
		}, err
	}

	post, postErr := kubeResourceAccessor.Get(ctx, target.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(postErr) {
		return func(d *kubeapplier.DeleteDesire) {
			conditions.SetSuccessful(&d.Status.Conditions, nil)
			conditions.SetDegraded(&d.Status.Conditions, nil)
		}, nil
	}
	if postErr != nil {
		err := fmt.Errorf("post-delete get: %w", postErr)
		return func(d *kubeapplier.DeleteDesire) {
			conditions.SetSuccessful(&d.Status.Conditions, err)
			conditions.SetDegraded(&d.Status.Conditions, classifyAsDegraded(err))
		}, err
	}
	dt := post.GetDeletionTimestamp()
	uid := post.GetUID()
	if dt == nil {
		now := metav1.NewTime(time.Now())
		dt = &now
	}
	return func(d *kubeapplier.DeleteDesire) {
		conditions.SetSuccessfulWaitingForDeletion(&d.Status.Conditions, *dt, uid)
		conditions.SetDegraded(&d.Status.Conditions, nil)
	}, nil
}

func classifyAsDegraded(err error) error {
	if err == nil {
		return nil
	}
	var preCheck *conditions.PreCheckError
	if errors.As(err, &preCheck) {
		return nil
	}
	var statusErr *apierrors.StatusError
	if errors.As(err, &statusErr) {
		c := statusErr.ErrStatus.Code
		if c >= 400 && c < 500 {
			return nil
		}
	}
	return err
}

type deleteDesireSpecFetcher struct {
	reader database.SpecReader[kubeapplier.DeleteDesire]
}

var _ desirestatuswriter.Fetcher[kubeapplier.DeleteDesire, keys.DeleteDesireKey] = &deleteDesireSpecFetcher{}

func (f *deleteDesireSpecFetcher) Fetch(ctx context.Context, key keys.DeleteDesireKey) (*kubeapplier.DeleteDesire, error) {
	return f.reader.Get(ctx, key.Name)
}

type deleteDesireStatusFetcher struct {
	crud database.ResourceCRUD[kubeapplier.DeleteDesire]
}

var _ desirestatuswriter.Fetcher[kubeapplier.DeleteDesire, keys.DeleteDesireKey] = &deleteDesireStatusFetcher{}

func (f *deleteDesireStatusFetcher) Fetch(ctx context.Context, key keys.DeleteDesireKey) (*kubeapplier.DeleteDesire, error) {
	return f.crud.Get(ctx, key.Name)
}

type deleteDesireReplacer struct {
	crud database.ResourceCRUD[kubeapplier.DeleteDesire]
}

var _ desirestatuswriter.Replacer[kubeapplier.DeleteDesire] = &deleteDesireReplacer{}

func (r *deleteDesireReplacer) Replace(ctx context.Context, desired *kubeapplier.DeleteDesire) error {
	_, err := r.crud.Replace(ctx, desired)
	return err
}

type deleteDesireCreator struct {
	crud database.ResourceCRUD[kubeapplier.DeleteDesire]
}

var _ desirestatuswriter.Creator[kubeapplier.DeleteDesire] = &deleteDesireCreator{}

func (c *deleteDesireCreator) Create(ctx context.Context, obj *kubeapplier.DeleteDesire) error {
	_, err := c.crud.Create(ctx, obj)
	return err
}
