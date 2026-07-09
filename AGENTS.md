# Agent & contributor guide

Siphon has two independent Go modules. Work in the one you are changing.

## Layout

- `probe/` — eBPF TC classifier + IPFIX exporter. Linux-only runtime. Module
  `github.com/serge-r/siphon/probe`.
- `collector/` — IPFIX collector, enrichment pipeline, sinks. Module
  `github.com/serge-r/siphon/collector`.

## Build & test

collector:

```sh
cd collector
go vet ./...
go build ./...
go test ./...
```

probe (needs clang + llvm to compile/embed the eBPF object; no libbpf):

```sh
cd probe
make generate   # compile bpf/probe.c and embed it via bpf2go
make build      # build the Go loader
go vet ./...
```

`bpf/probe.c` is self-contained (`bpf/bpf_probe.h`) and must stay free of libbpf
and distro/kernel headers. Regenerate with `make generate` after changing it;
the generated `probe_bpf*.go`/`.o` files are build artifacts (git-ignored).

CI runs the same steps for both modules (`.github/workflows/ci.yml`); keep it
green.

## Conventions

- Format with `gofmt` and keep `go vet` clean before committing.
- The probe is Linux-only: platform code lives in `main_linux.go`
  (`//go:build linux`) with a stub in `main_unsupported.go`. Do not add code
  that breaks `go build` on non-Linux hosts.
- Collector enrichers implement the `Enricher` interface and, if they own
  background goroutines, a `Close()` that stops them. Enrichers contribute flat
  `source_*` / `destination_*` fields; register new fields in `Fields()` and in
  `schema.go` so the generated JSON Schema stays accurate.
- Prometheus metric names are prefixed `siphon_`.
- The project name is **siphon** (`siphon-probe`, `siphon-collector`). The old
  working name `trafic-capture` / `trafic-collector` must not reappear.

## Do not leak

This is a public repository.

- No company-internal hostnames, registries, AWS account IDs, or credentials.
- Example configs and tests use placeholders and documentation-range addresses
  (RFC 5737: `192.0.2.0/24`, `198.51.100.0/24`, `203.0.113.0/24`) and
  `example.com` hostnames. Keep it that way.
