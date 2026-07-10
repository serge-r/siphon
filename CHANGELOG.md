# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

[0.1.1]: https://github.com/serge-r/siphon/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/serge-r/siphon/releases/tag/v0.1.0
