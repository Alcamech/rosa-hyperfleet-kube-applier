package app

import (
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// NewKubeconfig loads a *rest.Config from kubeconfigPath if non-empty, falling
// back to client-go's default loading rules (KUBECONFIG env, $HOME/.kube/config,
// in-cluster).
func NewKubeconfig(kubeconfigPath string) (*rest.Config, error) {
	loader := clientcmd.NewDefaultClientConfigLoadingRules()
	if kubeconfigPath != "" {
		loader.ExplicitPath = kubeconfigPath
	}
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loader, nil).ClientConfig()
}

// NewDynamicClient returns a dynamic.Interface backed by cfg. Every controller
// shares this single client; per-instance reflectors scope themselves to a
// single GVR + name via the ListWatch. qps and burst control the client-side
// rate limiter for all kube-apiserver requests made by the controllers.
func NewDynamicClient(cfg *rest.Config, qps float32, burst int) (dynamic.Interface, error) {
	dynCfg := rest.CopyConfig(cfg)
	dynCfg.QPS = qps
	dynCfg.Burst = burst
	return dynamic.NewForConfig(dynCfg)
}
