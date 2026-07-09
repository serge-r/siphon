package collector

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

type Enricher interface {
	Name() string
	Fields() []SchemaField
	Ready() error
	Enrich(ctx context.Context, flow *EnrichedFlow) error
}

func BuildEnrichers(cfg Config, logger *slog.Logger) ([]Enricher, error) {
	var enrichers []Enricher

	if len(cfg.Enrichment.Static) > 0 {
		staticEnricher, err := NewStaticEnricher(cfg.Enrichment.Static)
		if err != nil {
			return nil, err
		}
		enrichers = append(enrichers, staticEnricher)
	}

	if cfg.Enrichment.Inventory.Enabled {
		enrichers = append(enrichers, NewInventoryEnricher(cfg.Enrichment.Inventory, logger))
	}

	if cfg.Enrichment.Kubernetes.Enabled {
		k8sEnricher, err := NewKubernetesEnricher(cfg.Enrichment.Kubernetes, logger)
		if err != nil {
			return nil, err
		}
		enrichers = append(enrichers, k8sEnricher)
	}

	if cfg.Enrichment.AWSIPRanges.Enabled {
		enrichers = append(enrichers, NewAWSIPRangesEnricher(cfg.Enrichment.AWSIPRanges, logger))
	}

	if cfg.Enrichment.DNS.Enabled {
		enrichers = append(enrichers, NewDNSEnricher(cfg.Enrichment.DNS))
	}

	if cfg.Enrichment.GeoIP.Enabled {
		geoIPEnricher, err := NewGeoIPEnricher(cfg.Enrichment.GeoIP, logger)
		if err != nil {
			return nil, err
		}
		enrichers = append(enrichers, geoIPEnricher)
	}

	return enrichers, nil
}

type StaticEnricher struct {
	exact    map[netip.Addr]string
	prefixes []staticPrefix
}

type staticPrefix struct {
	prefix netip.Prefix
	name   string
}

func NewStaticEnricher(values map[string]string) (*StaticEnricher, error) {
	enricher := &StaticEnricher{
		exact: map[netip.Addr]string{},
	}

	for raw, name := range values {
		if strings.Contains(raw, "/") {
			prefix, err := netip.ParsePrefix(raw)
			if err != nil {
				return nil, fmt.Errorf("parse static enrichment prefix %q: %w", raw, err)
			}
			enricher.prefixes = append(enricher.prefixes, staticPrefix{prefix: prefix.Masked(), name: name})
			continue
		}
		addr, err := netip.ParseAddr(raw)
		if err != nil {
			return nil, fmt.Errorf("parse static enrichment IP %q: %w", raw, err)
		}
		enricher.exact[addr] = name
	}

	return enricher, nil
}

func (e *StaticEnricher) Name() string { return "static" }

func (e *StaticEnricher) Ready() error { return nil }

func (e *StaticEnricher) Fields() []SchemaField {
	return []SchemaField{
		{Name: "source_static_name", Type: "string"},
		{Name: "destination_static_name", Type: "string"},
	}
}

func (e *StaticEnricher) Enrich(_ context.Context, flow *EnrichedFlow) error {
	if name := e.lookup(flow.SourceIP); name != "" {
		flow.SourceStaticName = name
		setSourceName(flow, name)
	}
	if name := e.lookup(flow.DestinationIP); name != "" {
		flow.DestinationStaticName = name
		setDestinationName(flow, name)
	}
	return nil
}

func (e *StaticEnricher) lookup(rawIP string) string {
	addr, err := netip.ParseAddr(rawIP)
	if err != nil {
		return ""
	}
	if name := e.exact[addr]; name != "" {
		return name
	}

	var bestName string
	bestBits := -1
	for _, candidate := range e.prefixes {
		if candidate.prefix.Contains(addr) && candidate.prefix.Bits() > bestBits {
			bestName = candidate.name
			bestBits = candidate.prefix.Bits()
		}
	}
	return bestName
}

type DNSEnricher struct {
	resolver   *net.Resolver
	timeout    time.Duration
	cacheTTL   time.Duration
	maxEntries int
	mu         sync.Mutex
	cache      map[string]dnsCacheEntry
}

type dnsCacheEntry struct {
	name      string
	expiresAt time.Time
}

func NewDNSEnricher(cfg DNSEnrichmentConfig) *DNSEnricher {
	return &DNSEnricher{
		resolver:   net.DefaultResolver,
		timeout:    cfg.Timeout,
		cacheTTL:   cfg.CacheTTL,
		maxEntries: cfg.MaxCacheEntries,
		cache:      map[string]dnsCacheEntry{},
	}
}

func (e *DNSEnricher) Name() string { return "dns" }

func (e *DNSEnricher) Ready() error { return nil }

func (e *DNSEnricher) Fields() []SchemaField {
	return []SchemaField{
		{Name: "source_dns_name", Type: "string"},
		{Name: "destination_dns_name", Type: "string"},
	}
}

func (e *DNSEnricher) Enrich(ctx context.Context, flow *EnrichedFlow) error {
	if name := e.lookup(ctx, flow.SourceIP); name != "" {
		flow.SourceDNSName = name
		setSourceName(flow, name)
	}
	if name := e.lookup(ctx, flow.DestinationIP); name != "" {
		flow.DestinationDNSName = name
		setDestinationName(flow, name)
	}
	return nil
}

func (e *DNSEnricher) lookup(ctx context.Context, ip string) string {
	now := time.Now()
	e.mu.Lock()
	if cached, ok := e.cache[ip]; ok && cached.expiresAt.After(now) {
		e.mu.Unlock()
		return cached.name
	}
	e.mu.Unlock()

	lookupCtx, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()

	names, err := e.resolver.LookupAddr(lookupCtx, ip)
	name := ""
	if err == nil && len(names) > 0 {
		name = strings.TrimSuffix(names[0], ".")
	}

	e.mu.Lock()
	if _, exists := e.cache[ip]; !exists && e.maxEntries > 0 && len(e.cache) >= e.maxEntries {
		e.evictLocked(now)
	}
	e.cache[ip] = dnsCacheEntry{name: name, expiresAt: now.Add(e.cacheTTL)}
	e.mu.Unlock()
	return name
}

// evictLocked bounds the cache size. It first drops expired entries; if the
// cache is still at capacity it removes arbitrary entries until it is back
// under the limit. Callers must hold e.mu.
func (e *DNSEnricher) evictLocked(now time.Time) {
	for ip, entry := range e.cache {
		if !entry.expiresAt.After(now) {
			delete(e.cache, ip)
		}
	}
	for ip := range e.cache {
		if len(e.cache) < e.maxEntries {
			break
		}
		delete(e.cache, ip)
	}
}

type KubernetesEnricher struct {
	internalNetworks []netip.Prefix
	mu               sync.RWMutex
	podsByIP         map[string]podRef
	synced           atomic.Bool
	syncErr          atomic.Value
	stopCh           chan struct{}
	stopOnce         sync.Once
}

type podRef struct {
	namespace string
	name      string
}

func NewKubernetesEnricher(cfg KubernetesEnrichmentConf, logger *slog.Logger) (*KubernetesEnricher, error) {
	enricher := &KubernetesEnricher{
		podsByIP: map[string]podRef{},
		stopCh:   make(chan struct{}),
	}
	for _, raw := range cfg.InternalNetworks {
		prefix, err := netip.ParsePrefix(raw)
		if err != nil {
			return nil, fmt.Errorf("parse kubernetes internal network %q: %w", raw, err)
		}
		enricher.internalNetworks = append(enricher.internalNetworks, prefix.Masked())
	}

	restConfig, err := kubernetesRestConfig(cfg.Kubeconfig)
	if err != nil {
		return nil, err
	}
	client, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("create kubernetes client: %w", err)
	}

	factory := informers.NewSharedInformerFactoryWithOptions(
		client,
		10*time.Minute,
		informers.WithTweakListOptions(func(options *metav1.ListOptions) {
			options.FieldSelector = fields.Everything().String()
		}),
	)
	informer := factory.Core().V1().Pods().Informer()
	_, err = informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			if pod, ok := obj.(*corev1.Pod); ok {
				enricher.upsertPod(pod)
			}
		},
		UpdateFunc: func(_, newObj any) {
			if pod, ok := newObj.(*corev1.Pod); ok {
				enricher.upsertPod(pod)
			}
		},
		DeleteFunc: func(obj any) {
			switch pod := obj.(type) {
			case *corev1.Pod:
				enricher.deletePod(pod)
			case cache.DeletedFinalStateUnknown:
				if p, ok := pod.Obj.(*corev1.Pod); ok {
					enricher.deletePod(p)
				}
			}
		},
	})
	if err != nil {
		return nil, fmt.Errorf("add pod informer handler: %w", err)
	}

	factory.Start(enricher.stopCh)
	go func() {
		if !cache.WaitForCacheSync(enricher.stopCh, informer.HasSynced) {
			err := fmt.Errorf("kubernetes pod cache did not sync")
			enricher.syncErr.Store(err)
			logger.Warn("kubernetes pod cache did not sync")
			return
		}
		enricher.synced.Store(true)
		logger.Info("kubernetes pod cache synced")
	}()

	return enricher, nil
}

func (e *KubernetesEnricher) Name() string { return "kubernetes" }

func (e *KubernetesEnricher) Close() {
	e.stopOnce.Do(func() {
		close(e.stopCh)
	})
}

func (e *KubernetesEnricher) Ready() error {
	if e.synced.Load() {
		return nil
	}
	if err, ok := e.syncErr.Load().(error); ok && err != nil {
		return err
	}
	return fmt.Errorf("kubernetes pod cache is not synced")
}

func (e *KubernetesEnricher) Fields() []SchemaField {
	return []SchemaField{
		{Name: "source_namespace", Type: "string"},
		{Name: "source_pod_name", Type: "string"},
		{Name: "destination_namespace", Type: "string"},
		{Name: "destination_pod_name", Type: "string"},
	}
}

func (e *KubernetesEnricher) Enrich(_ context.Context, flow *EnrichedFlow) error {
	if pod, ok := e.lookup(flow.SourceIP); ok {
		flow.SourceNamespace = pod.namespace
		flow.SourcePodName = pod.name
		setSourceName(flow, pod.namespace+"/"+pod.name)
	}
	if pod, ok := e.lookup(flow.DestinationIP); ok {
		flow.DestinationNamespace = pod.namespace
		flow.DestinationPodName = pod.name
		setDestinationName(flow, pod.namespace+"/"+pod.name)
	}
	return nil
}

func (e *KubernetesEnricher) lookup(ip string) (podRef, bool) {
	addr, err := netip.ParseAddr(ip)
	if err != nil {
		return podRef{}, false
	}
	if len(e.internalNetworks) > 0 {
		matched := false
		for _, network := range e.internalNetworks {
			if network.Contains(addr) {
				matched = true
				break
			}
		}
		if !matched {
			return podRef{}, false
		}
	}

	e.mu.RLock()
	defer e.mu.RUnlock()
	pod, ok := e.podsByIP[ip]
	return pod, ok
}

func (e *KubernetesEnricher) upsertPod(pod *corev1.Pod) {
	ref := podRef{namespace: pod.Namespace, name: pod.Name}

	e.mu.Lock()
	defer e.mu.Unlock()
	for _, podIP := range pod.Status.PodIPs {
		if podIP.IP != "" {
			e.podsByIP[podIP.IP] = ref
		}
	}
	if pod.Status.PodIP != "" {
		e.podsByIP[pod.Status.PodIP] = ref
	}
}

func (e *KubernetesEnricher) deletePod(pod *corev1.Pod) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, podIP := range pod.Status.PodIPs {
		delete(e.podsByIP, podIP.IP)
	}
	if pod.Status.PodIP != "" {
		delete(e.podsByIP, pod.Status.PodIP)
	}
}

func kubernetesRestConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		path, err := filepath.Abs(kubeconfig)
		if err != nil {
			return nil, err
		}
		cfg, err := clientcmd.BuildConfigFromFlags("", path)
		if err != nil {
			return nil, fmt.Errorf("load kubeconfig %q: %w", kubeconfig, err)
		}
		return cfg, nil
	}

	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("load in-cluster kubernetes config: %w", err)
	}
	return cfg, nil
}

func setSourceName(flow *EnrichedFlow, name string) {
	if flow.SourceName == "" {
		flow.SourceName = name
	}
}

func setDestinationName(flow *EnrichedFlow, name string) {
	if flow.DestinationName == "" {
		flow.DestinationName = name
	}
}
