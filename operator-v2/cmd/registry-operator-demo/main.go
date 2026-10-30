// registry-operator-demo is an explicitly local simulation, never a worker.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/simulation"
)

func main() {
	scenario := flag.String("scenario", "readiness-failure", "Local scenario: success, readiness-failure, smoke-failure, evidence-failure, cleanup-failure")
	asJSON := flag.Bool("json", false, "Print a safe local simulation result as JSON")
	flag.Parse()
	if flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "This command accepts only local scenario flags.")
		os.Exit(2)
	}
	output, err := simulation.Run(context.Background(), simulation.Scenario(*scenario))
	if *asJSON {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		if encodeErr := encoder.Encode(output); encodeErr != nil {
			fmt.Fprintln(os.Stderr, "Could not encode safe simulation result.")
			os.Exit(1)
		}
	} else {
		fmt.Println("LOCAL SIMULATION: fake providers and an in-memory journal; no cloud, Kubernetes or Temporal connection.")
		fmt.Printf("Scenario: %s; observed steps: %d.\n", output.Scenario, output.Steps)
		if output.Diagnostics.Identity.OperationID != "" {
			fmt.Println(diagnostics.Explain(output.Diagnostics))
		}
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "Local simulation did not complete; inspect its safe diagnostic result.")
		os.Exit(1)
	}
}
