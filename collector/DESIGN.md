# siphon-collector

`siphon-collector` receives IPFIX packets from `siphon-probe`, enriches flow
records with local and external metadata, emits flat JSON events, batches the
events, and writes them to Kafka or stdout.

## Scope

The first implementation intentionally supports only the IPFIX format produced
by the local `siphon-probe` project:

- IPFIX version: `10`
- transport: UDP
- template ID: `256`
- observation domain: accepted as-is
- record fields:
  - `sourceIPv4Address`
  - `destinationIPv4Address`
  - `sourceTransportPort`
  - `destinationTransportPort`
  - `protocolIdentifier`
  - `packetDeltaCount`
  - `octetDeltaCount`

TCP listener support and generic IPFIX template handling are out of scope for
the initial version.

## Runtime Pipeline

The service is built as a bounded pipeline:

```text
UDP listener -> IPFIX decoder -> enrichment workers -> batcher -> sink
```

Pipeline queues are bounded and configured in YAML. If a downstream stage cannot
keep up, the collector drops work at the saturated boundary and increments
Prometheus counters. This is preferred over unbounded memory growth.

Required backpressure metrics:

- `siphon_packets_received_total`
- `siphon_packets_dropped_total{stage=...}`
- `siphon_flows_decoded_total`
- `siphon_flows_processed_total`
- `siphon_channel_queue_size{stage=...}`
- `siphon_channel_capacity{stage=...}`
- `siphon_batches_sent_total`
- `siphon_sink_errors_total{sink=...}`

The collector also exposes per-host processing counters, where host means the
best available enriched name for a flow endpoint.

## Enrichment

Enrichment is implemented as an ordered chain of enrichers. Each enricher can add
its own flat fields to the output record, for example `source_pod_name`,
`destination_namespace`, or `source_dns_name`.
GeoIP contributes `source_geoip_isp`, `source_geoip_asn`,
`source_geoip_country`, `source_geoip_city`, and the matching `destination_*`
fields.

Initial enrichers:

- static map: exact IP and CIDR mapping from config, for example
  `10.0.0.0/8: internal`
- inventory: periodically fetch a server inventory from an HTTP JSON endpoint
  and build an `ip -> server name` map from each entry's `server_ip`;
  container/additional-network data is ignored. The endpoint is expected to
  return `{"data": [{"name": ..., "server_ip": [{"ip": ...}], ...}]}`; only
  `name` and `server_ip[].ip` are used, every other field is optional and
  ignored (see `internal/collector/inventory.go` for the full accepted shape).
  Contributes `source_inventory_name` and `destination_inventory_name`
- Kubernetes: list/watch pods and keep a local `podIP -> namespace/name` cache;
  lookup is used only for IPs inside configured internal networks
- AWS IP ranges: download `https://ip-ranges.amazonaws.com/ip-ranges.json`,
  cache it on disk, refresh by TTL, and enrich public IPv4 endpoints with AWS
  service and region when they match an AWS prefix
- DNS: reverse DNS lookup with timeout and TTL cache
- GeoIP: download `GeoIP2-City.tar.gz`, `GeoIP2-Country.tar.gz`, and
  `GeoIP2-ISP.tar.gz` from `enrichment.geoip.database_url`, extract the MMDB
  files into `enrichment.geoip.database_save_path`, and refresh them every
  `enrichment.geoip.cache_ttl`; enrich only external IPs with ISP, ASN, country,
  and city fields

The current enrichment order is static, inventory, Kubernetes, AWS IP ranges,
DNS, GeoIP.

Kubernetes authentication supports both in-cluster config and an explicit
`kubeconfig` path.

Future enrichers should be added without changing the pipeline shape.

## Output Schema

The base flow schema is fixed by the `siphon-probe` IPFIX record. The output
event is intentionally flat: endpoint fields use names such as `source_ip`,
`source_pod_name`, `destination_ip`, and `destination_dns_name`. Enrichers may
extend the output with additional flat fields. The collector should be able to
expose or generate a JSON Schema that reflects:

- base flow fields
- enabled enrichers
- fields contributed by each enabled enricher

The schema is intended for downstream validation pipelines and for deriving SQL
table definitions. The first implementation may generate a static schema from
the configured enricher set; schema registry integration is out of scope.

## Batching and Sinks

The batcher flushes when either condition is met:

- `pipeline.batch.max_records`
- `pipeline.batch.flush_interval`

Supported sinks:

- Kafka, using `franz-go`
- stdout JSON for debugging and local validation

Kafka must support SASL/PLAIN authentication. SASL/PLAIN can run over TLS when
`outputs[].config.ssl.enabled` is set; the CA can be provided either as
`ssl.ca_file` or inline `ssl.ca_pem`. Kafka outputs also support producer tuning
in `outputs[].config`, including producer linger, max record batch bytes, max
buffered records/bytes, produce request timeout, and ping timeout.
Kafka batch compression can be configured through
`outputs[].config.compression`; supported values are `none`, `snappy`, `lz4`,
`zstd`, and `gzip`. If unset, the franz-go default is used.

## Configuration

Configuration is loaded from a YAML file. The current example is
`config.example.yaml` and includes:

- global listener settings
- metrics address
- JSON logging settings
- pipeline queue sizes and worker count
- batch settings
- output definitions
- Kafka brokers/topic/SASL and producer tuning settings for Kafka outputs
- static, inventory, Kubernetes, AWS IP ranges, DNS, and GeoIP enrichment settings

## Build

The service is built with the existing Dockerfile.

## Local Run

Use Docker Compose to start a local Kafka-compatible broker and the collector:

```sh
docker compose up --build
```

The compose file exposes:

- UDP IPFIX listener: `localhost:4739`
- Prometheus metrics: `http://localhost:9090/metrics`
- readiness probe: `http://localhost:9090/readyz`
- generated JSON Schema: `http://localhost:9090/schema`
- Kafka broker for local tools: `localhost:19092`
- Kafka UI: `http://localhost:8080`
