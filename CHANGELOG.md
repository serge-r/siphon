# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed
- **collector:** a single failed enrichment refresh no longer fails `/readyz`. A
  transient error from the inventory endpoint (or the AWS IP ranges / GeoIP
  sources) left the cache it had already built fully intact, so enrichment kept
  working, but flipped the pod to NotReady until the next refresh — up to a whole
  `cache_ttl`. Behind a `LoadBalancer` Service the pod was then removed from the
  EndpointSlice and IPFIX packets stopped being delivered, turning degraded
  enrichment into a total loss of ingestion. An enricher serving previously
  loaded data is now ready; only one that never loaded any is not.
- **collector:** a failed enrichment refresh is retried with jittered
  exponential backoff (from 5s, capped at `cache_ttl`) instead of waiting for the
  next `cache_ttl` tick, so a transient upstream error costs seconds.
- **collector:** the Kafka readiness check no longer pings the brokers from
  inside the probe request. `KafkaSink.Ready` derived its ping timeout from the
  HTTP request context, so with `readinessProbe.timeoutSeconds: 1` kubelet
  closing the connection failed `/readyz` with `context canceled` against a
  healthy Kafka. A background loop now pings every 10s and `Ready` reads the
  cached verdict, which also drops broker load from one ping per probe.
- **collector:** enrichers and the Kafka sink stored bare `error` values in an
  `atomic.Value`, which panics as soon as two different error implementations
  reach the same field — reachable on the very path above, where an unexpected
  HTTP status and a network failure produce different types.

## [0.1.1] - 2026-07-10

### Fixed
- **probe:** `flow_map` was created as `LRU_HASH` instead of
  `LRU_PERCPU_HASH` because of a wrong map-type constant in the self-contained
  BPF header (`9` instead of `10`). This broke flow polling with
  `unmarshaling *[]main.flowStats doesn't consume all data` and caused cross-CPU
  counter races. Flow counters are now correctly per-CPU.

### Added
- **probe:** `-debug` flag for verbose logging — eBPF map metadata at startup,
  per-interval drained/exported flow counts, and each flow's 5-tuple. Under
  systemd it can be toggled via `SIPHON_PROBE_EXTRA_ARGS=-debug` in
  `/etc/default/siphon-probe`.

## [0.1.0] - 2026-07-09

### Added
- Initial release: eBPF TC probe (IPFIX exporter) and IPFIX collector with an
  enrichment pipeline (static, inventory, Kubernetes, AWS IP ranges, DNS,
  GeoIP), Kafka/stdout sinks, multi-arch Docker images for both components, and
  deb/rpm packages for the probe.

[Unreleased]: https://github.com/serge-r/siphon/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/serge-r/siphon/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/serge-r/siphon/releases/tag/v0.1.0
