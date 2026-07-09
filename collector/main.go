package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/serge-r/siphon/collector/internal/collector"
)

// version is set at build time via -ldflags "-X main.version=...".
var version = "dev"

func main() {
	var configPath string
	var printSchema bool
	var showVersion bool
	flag.StringVar(&configPath, "config", "config.example.yaml", "path to YAML config")
	flag.BoolVar(&printSchema, "print-schema", false, "print generated JSON Schema and exit")
	flag.BoolVar(&showVersion, "version", false, "print version and exit")
	flag.Parse()

	if showVersion {
		fmt.Println(version)
		return
	}

	cfg, err := collector.LoadConfig(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config: %v\n", err)
		os.Exit(1)
	}

	logger := collector.NewLogger(cfg.Global.Logging)
	if printSchema {
		schema := collector.BuildJSONSchemaForConfig(cfg)
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(schema); err != nil {
			logger.Error("print schema", "error", err)
			os.Exit(1)
		}
		return
	}

	enrichers, err := collector.BuildEnrichers(cfg, logger)
	if err != nil {
		logger.Error("build enrichers", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := collector.Run(ctx, cfg, enrichers, logger); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("collector stopped", "error", err)
		os.Exit(1)
	}
}
