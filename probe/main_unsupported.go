//go:build !linux

package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "siphon-probe requires Linux: TC/eBPF netlink APIs are not available on this OS")
	os.Exit(1)
}
