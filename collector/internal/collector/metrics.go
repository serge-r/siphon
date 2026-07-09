package collector

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// maxHostSeries bounds the number of distinct `host` label values tracked by
// the per-host counter. Host names are derived from enrichment (including
// reverse DNS of endpoints seen on the wire), so leaving this unbounded is a
// cardinality-based memory DoS vector.
const maxHostSeries = 10000

type Metrics struct {
	PacketsReceived   prometheus.Counter
	PacketDrops       *prometheus.CounterVec
	FlowsDecoded      prometheus.Counter
	FlowsProcessed    prometheus.Counter
	BatchesSent       prometheus.Counter
	SinkErrors        *prometheus.CounterVec
	HostProcessed     *prometheus.CounterVec
	HostSeriesDropped prometheus.Counter
	QueueCapacity     *prometheus.GaugeVec

	hostMu   sync.Mutex
	hostSeen map[string]struct{}
}

func NewMetrics() *Metrics {
	return &Metrics{
		PacketsReceived: promauto.NewCounter(prometheus.CounterOpts{
			Name: "siphon_packets_received_total",
			Help: "Total number of received IPFIX packets.",
		}),
		PacketDrops: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "siphon_packets_dropped_total",
			Help: "Total number of packets or flow records dropped by saturated pipeline stages.",
		}, []string{"stage"}),
		FlowsDecoded: promauto.NewCounter(prometheus.CounterOpts{
			Name: "siphon_flows_decoded_total",
			Help: "Total number of decoded flow records.",
		}),
		FlowsProcessed: promauto.NewCounter(prometheus.CounterOpts{
			Name: "siphon_flows_processed_total",
			Help: "Total number of enriched flow records.",
		}),
		BatchesSent: promauto.NewCounter(prometheus.CounterOpts{
			Name: "siphon_batches_sent_total",
			Help: "Total number of successfully written batches.",
		}),
		SinkErrors: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "siphon_sink_errors_total",
			Help: "Total number of sink write errors.",
		}, []string{"sink"}),
		HostProcessed: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "siphon_host_flows_processed_total",
			Help: "Total number of processed flow endpoint observations per enriched host.",
		}, []string{"host"}),
		HostSeriesDropped: promauto.NewCounter(prometheus.CounterOpts{
			Name: "siphon_host_series_dropped_total",
			Help: "Total number of per-host counter increments dropped after reaching the host cardinality limit.",
		}),
		QueueCapacity: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "siphon_channel_capacity",
			Help: "Configured capacity of a collector pipeline queue.",
		}, []string{"stage"}),
		hostSeen: map[string]struct{}{},
	}
}

func (m *Metrics) ObserveQueue(stage string, ch any) {
	switch typed := ch.(type) {
	case chan Packet:
		m.QueueCapacity.WithLabelValues(stage).Set(float64(cap(typed)))
		prometheus.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name:        "siphon_channel_queue_size",
			Help:        "Current number of items in a collector pipeline queue.",
			ConstLabels: prometheus.Labels{"stage": stage},
		}, func() float64 { return float64(len(typed)) }))
	case chan Flow:
		m.QueueCapacity.WithLabelValues(stage).Set(float64(cap(typed)))
		prometheus.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name:        "siphon_channel_queue_size",
			Help:        "Current number of items in a collector pipeline queue.",
			ConstLabels: prometheus.Labels{"stage": stage},
		}, func() float64 { return float64(len(typed)) }))
	case chan EnrichedFlow:
		m.QueueCapacity.WithLabelValues(stage).Set(float64(cap(typed)))
		prometheus.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name:        "siphon_channel_queue_size",
			Help:        "Current number of items in a collector pipeline queue.",
			ConstLabels: prometheus.Labels{"stage": stage},
		}, func() float64 { return float64(len(typed)) }))
	case chan []EnrichedFlow:
		m.QueueCapacity.WithLabelValues(stage).Set(float64(cap(typed)))
		prometheus.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name:        "siphon_channel_queue_size",
			Help:        "Current number of items in a collector pipeline queue.",
			ConstLabels: prometheus.Labels{"stage": stage},
		}, func() float64 { return float64(len(typed)) }))
	}
}

func (m *Metrics) CountHost(host string) {
	if host == "" {
		return
	}

	m.hostMu.Lock()
	if _, seen := m.hostSeen[host]; !seen {
		if len(m.hostSeen) >= maxHostSeries {
			m.hostMu.Unlock()
			m.HostSeriesDropped.Inc()
			return
		}
		m.hostSeen[host] = struct{}{}
	}
	m.hostMu.Unlock()

	m.HostProcessed.WithLabelValues(host).Inc()
}
