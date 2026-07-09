package main

// The eBPF object is compiled from bpf/probe.c and embedded into the binary by
// bpf2go, which generates probe_bpfel.go / probe_bpfeb.go and the matching .o
// files (loadProbe lives there). Regenerate after changing the C with:
//
//	make generate   # needs clang with a bpf target and llvm-strip on PATH
//
//go:generate bpf2go -target bpfel,bpfeb probe ./bpf/probe.c
