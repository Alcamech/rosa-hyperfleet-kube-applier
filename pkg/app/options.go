// Package app wires the kube-applier binary together. It is invoked from
// cmd after flags have been parsed and external dependencies (kubeconfig,
// leader-election lock, DynamoDB clients) have been constructed.
package app

import (
	"github.com/prometheus/client_golang/prometheus"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	"github.com/openshift-online/rosa-hyperfleet-kube-applier/internal/database"
	"github.com/openshift-online/rosa-hyperfleet-kube-applier/internal/database/informers"
)

const AppShortDescriptionName = "AWS HCP kube-applier"

const (
	threadsApply       = 4
	threadsReadManager = 1
)

// Options is the wired bundle of dependencies the kube-applier needs to run.
type Options struct {
	ManagementCluster string

	LeaderElectionLock  resourcelock.Interface
	KubeApplierDBClient database.KubeApplierDBClient
	Informers           informers.KubeApplierInformers
	DynamicClient       dynamic.Interface

	MetricsServerListenAddress string
	HealthzServerListenAddress string

	MetricsRegisterer prometheus.Registerer
	MetricsGatherer   prometheus.Gatherer

	ExitOnPanic bool
}
