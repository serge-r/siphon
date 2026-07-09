# Project Siphon

Siphon is a toolchain for capturing, enriching, and shipping network flow data
from a Linux node. It has two independent components:

- **probe** — a small Linux daemon that attaches an eBPF TC classifier to a
  network interface, aggregates IPv4 TCP/UDP traffic by 5-tuple, and exports the
  counters as IPFIX over UDP.
- **collector** — a service that receives IPFIX packets, enriches each flow with
  local and external metadata (static maps, server inventory, Kubernetes, AWS IP
  ranges, reverse DNS, GeoIP), and writes flat JSON events to Kafka or stdout.

```text
             IPFIX / UDP                 enrich                 JSON
  ┌───────┐  ───────────►  ┌───────────┐  ────►  ┌──────────────────┐
  │ probe │                │ collector │         │ Kafka  /  stdout │
  └───────┘                └───────────┘         └──────────────────┘
   eBPF TC                  UDP listener →
   classifier               decoder → enrichers →
                            batcher → sink
```

The two components are separate Go modules and can be built, deployed, and run
independently: a probe on each monitored node, and one or more central
collectors.

## Repository layout

```text
probe/        eBPF traffic classifier and IPFIX exporter (Linux only)
collector/    IPFIX collector, enrichment pipeline, and sinks
```

Each component has its own `DESIGN.md` with the detailed design and full flag /
configuration reference.

## probe

Linux-only. Requires `CAP_NET_ADMIN` / `CAP_BPF` at runtime and clang + llvm to
build the embedded eBPF object (no libbpf or kernel headers needed).

Build:

```sh
cd probe
make generate   # compile bpf/probe.c and embed it (needs clang + llvm)
make build      # build the single self-contained binary
```

Run:

```sh
sudo ./siphon-probe \
  -iface eth0 \
  -direction ingress \
  -interval 10s \
  -max-flows 10000 \
  -collector 127.0.0.1:4739
```

The eBPF object is embedded into the binary, so a single file runs across kernel
versions (it reads only packet headers, so it needs no CO-RE/BTF).

For host installation, each release ships `deb` and `rpm` packages that place
the binary at `/opt/siphon-probe/siphon-probe` and install the systemd unit
`siphon-probe.service`:

```sh
sudo dpkg -i siphon-probe_0.1.0_amd64.deb        # Debian/Ubuntu
sudo rpm -i siphon-probe-0.1.0.x86_64.rpm        # RHEL/Fedora
sudo systemctl enable --now siphon-probe
```

A container image is also published (`serger89/siphon-probe`); it needs host
networking and NET_ADMIN/BPF capabilities:

```sh
docker run --network host --cap-add NET_ADMIN --cap-add BPF \
  serger89/siphon-probe -iface eth0 -collector 10.0.0.1:4739
```

See [`probe/DESIGN.md`](probe/DESIGN.md) for build-from-source details and the
exported IPFIX template.

## collector

Build and test:

```sh
cd collector
go build ./...
go test ./...
```

Run against a config file:

```sh
./siphon-collector -config config.example.yaml
```

Print the generated JSON Schema and exit:

```sh
./siphon-collector -config config.example.yaml -print-schema
```

Print the version and exit:

```sh
./siphon-collector -version
```

By default the collector listens for IPFIX on UDP `:4739` and serves on
`:9090`:

- `GET /metrics` — Prometheus metrics
- `GET /readyz` — readiness probe
- `GET /schema` — generated JSON Schema for the current enricher set

Configuration is documented in [`config.example.yaml`](collector/config.example.yaml)
and [`collector/DESIGN.md`](collector/DESIGN.md). A ClickHouse schema for the
emitted events is provided in [`collector/scheme.sql`](collector/scheme.sql).

### Local stack

A Docker Compose stack brings up a Redpanda (Kafka-compatible) broker, a Kafka
UI, and the collector:

```sh
cd collector
docker compose up --build
```

It exposes the UDP IPFIX listener on `localhost:4739`, metrics on
`localhost:9090`, the Kafka broker on `localhost:19092`, and the Kafka UI on
`localhost:8080`.

## Releases

CI (`.github/workflows/ci.yml`) builds, vets, and tests both components and
builds both container images on every push and pull request.

Releases are cut by pushing a semver tag:

```sh
git tag v0.1.0
git push origin v0.1.0
```

The release workflow (`.github/workflows/release.yml`) then, for the tag,
produces:

- multi-arch (`linux/amd64` + `linux/arm64`) Docker images
  `serger89/siphon-collector` and `serger89/siphon-probe` (tagged with the git
  tag verbatim, e.g. `v0.1.0`, plus `latest`), pushed to Docker Hub;
- `linux` `amd64`/`arm64` binaries for both components, with the version embedded
  (`-version`);
- `deb` and `rpm` packages for the probe (numeric version, e.g. `0.1.0`);

and attaches the binaries and packages to a GitHub release.

Pushing images requires two repository secrets: `DOCKERHUB_USERNAME` and
`DOCKERHUB_TOKEN` (a Docker Hub access token with push scope).

## License

Siphon is released under the [MIT License](LICENSE). It is provided "as is",
without warranty of any kind; use it at your own risk.
