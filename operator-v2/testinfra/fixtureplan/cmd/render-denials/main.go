package main

import (
	"flag"
	"fmt"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/testinfra/fixtureplan"
	"os"
	"path/filepath"
)

func main() {
	out := flag.String("out", "", "Separate deny-sentinel review directory (required)")
	flag.Parse()
	if *out == "" || filepath.Base(filepath.Clean(*out)) != fixtureplan.RunID+"-deny" {
		fmt.Fprintln(os.Stderr, "-out must identify the separate fixture run deny directory")
		os.Exit(2)
	}
	r, err := fixtureplan.RenderDenySentinels()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err = os.MkdirAll(*out, 0755); err != nil {
		fmt.Fprintln(os.Stderr, "cannot create separate review directory")
		os.Exit(1)
	}
	for name, value := range map[string]any{"main.tf.json": r.Terraform, "review.json": r.Review, "temporal.review.json": r.Temporal} {
		raw, err := fixtureplan.WriteJSON(value)
		if err != nil {
			fmt.Fprintln(os.Stderr, "cannot encode review")
			os.Exit(1)
		}
		if err = os.WriteFile(filepath.Join(*out, name), append(raw, '\n'), 0644); err != nil {
			fmt.Fprintln(os.Stderr, "cannot write review")
			os.Exit(1)
		}
	}
	fmt.Println("Owned deny-sentinel metadata plan rendered; no apply, seeding or probes authorized.")
}
