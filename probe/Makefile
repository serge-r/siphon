APP ?= siphon-probe
GOOS ?= linux
GOARCH ?= amd64
VERSION ?= dev

# Pin bpf2go to the same version as the cilium/ebpf runtime library.
EBPF_VERSION := $(shell go list -m -f '{{.Version}}' github.com/cilium/ebpf)
GOPATH_BIN := $(shell go env GOPATH)/bin

.PHONY: all generate build build-amd64 build-arm64 build-all clean

all: generate build

# Compile bpf/probe.c and (re)generate the embedded Go bindings and objects.
# Requires clang with a bpf target and llvm-strip on PATH. No libbpf or kernel
# headers are needed: bpf/probe.c is self-contained (see bpf/bpf_probe.h).
generate:
	go install github.com/cilium/ebpf/cmd/bpf2go@$(EBPF_VERSION)
	PATH="$(GOPATH_BIN):$$PATH" go generate ./...

build:
	GOOS=$(GOOS) GOARCH=$(GOARCH) go build -trimpath \
	  -ldflags "-s -w -X main.version=$(VERSION)" -o $(APP) .

build-amd64:
	$(MAKE) build GOARCH=amd64 APP=siphon-probe-linux-amd64 VERSION=$(VERSION)

build-arm64:
	$(MAKE) build GOARCH=arm64 APP=siphon-probe-linux-arm64 VERSION=$(VERSION)

build-all: build-amd64 build-arm64

clean:
	rm -f $(APP) siphon-probe-linux-amd64 siphon-probe-linux-arm64 \
	  probe_bpfel.o probe_bpfeb.o probe_bpfel.go probe_bpfeb.go
