package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/testinfra/fixtureplan"
)

func main() {
	out := flag.String("out", "", "Exact isolated fixture output directory (required)")
	flag.Parse()
	if *out == "" {
		fmt.Fprintln(os.Stderr, "-out required")
		os.Exit(2)
	}
	plan, err := fixtureplan.Render(nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err = os.MkdirAll(*out, 0755); err != nil {
		fmt.Fprintln(os.Stderr, "cannot create review directory")
		os.Exit(1)
	}
	for name, value := range map[string]any{"main.tf.json": plan.Terraform, "review.json": plan.Review, "temporal.review.json": plan.Temporal} {
		raw, err := fixtureplan.WriteJSON(value)
		if err != nil {
			fmt.Fprintln(os.Stderr, "cannot encode plan")
			os.Exit(1)
		}
		if err = os.WriteFile(filepath.Join(*out, name), append(raw, '\n'), 0644); err != nil {
			fmt.Fprintln(os.Stderr, "cannot write plan")
			os.Exit(1)
		}
	}
	fmt.Println(plan.String())
}
