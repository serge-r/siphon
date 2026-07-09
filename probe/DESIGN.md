# siphon-probe

Small Linux daemon that attaches an eBPF TC classifier to an interface,
aggregates IPv4 TCP/UDP traffic by 5-tuple, and exports counters to an IPFIX
collector over UDP.

The flow table is `BPF_MAP_TYPE_LRU_PERCPU_HASH`. When it reaches `-max-flows`,
the kernel can evict older entries to make room for new flows.

The kernel does not expose an exact eviction counter for LRU hash maps to the
program. The daemon reports capacity pressure when one polling interval drains
at least `-max-flows` flow entries, and also logs BPF-side counters:

- `new_flow_insert_attempts_delta`: new flow keys seen during the interval.
- `flow_insert_failures_delta`: failed map insertions.
- `flow_map_capacity_pressure=1`: the map reached the configured flow limit
  during the interval, so LRU evictions may have happened.

## Build

```sh
make generate   # compile bpf/probe.c and embed it into the Go bindings
make build      # build the daemon (single self-contained binary)
```

Build both Linux architectures:

```sh
make generate
make build-all
```

This produces:

- `siphon-probe-linux-amd64`
- `siphon-probe-linux-arm64`

The eBPF object is compiled from `bpf/probe.c` by
[`bpf2go`](https://pkg.go.dev/github.com/cilium/ebpf/cmd/bpf2go) and embedded
into the binary, so there is no separate `.o` file to ship. `bpf/probe.c` is
self-contained (see `bpf/bpf_probe.h`): it reads only packet wire headers and
needs **no libbpf and no kernel headers**. `make generate` only requires clang
with a `bpf` target and `llvm-strip` (from the `llvm` package):

```sh
sudo apt-get install clang llvm
```

Because the program touches no internal kernel structures, it needs no CO-RE
relocations and the same binary runs across kernel versions, including kernels
built without BTF. amd64 and arm64 are both little-endian, so a single
little-endian object serves both (bpf2go still emits a big-endian variant for
completeness).

## Run

```sh
sudo ./siphon-probe \
  -iface eth0 \
  -direction ingress \
  -interval 10s \
  -max-flows 10000 \
  -collector 127.0.0.1:4739
```

Flags:

- `-iface`: interface used for TC attachment.
- `-direction`: `ingress` or `egress`.
- `-interval`: eBPF map polling interval.
- `-max-flows`: maximum number of flow entries in the LRU eBPF map.
- `-collector`: UDP address of the IPFIX collector.

## Exported IPFIX Template

Template ID `256` contains:

- `sourceIPv4Address`
- `destinationIPv4Address`
- `sourceTransportPort`
- `destinationTransportPort`
- `protocolIdentifier`
- `packetDeltaCount`
- `octetDeltaCount`
