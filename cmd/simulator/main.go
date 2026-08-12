package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"lawson.com/gpu-crd/simulator"
)

func main() {
	namespace := flag.String("namespace", "default", "namespace to scan for GPUJobs")
	out := flag.String("out", "simulation.json", "output JSON file path")
	flag.Parse()

	report, err := simulator.Run(*namespace)
	if err != nil {
		fmt.Fprintf(os.Stderr, "simulation failed: %v\n", err)
		os.Exit(1)
	}

	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "json marshal failed: %v\n", err)
		os.Exit(1)
	}

	if err := os.WriteFile(*out, data, 0644); err != nil {
		fmt.Fprintf(os.Stderr, "write file failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("simulation complete, results written to %s\n", *out)
}
