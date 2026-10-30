package main

import (
	"flag"
	"fmt"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/testinfra/fixtureplan"
	"os"
	"path/filepath"
)

func main() {
	out := flag.String("out", "", "Project-only review directory (required)")
	flag.Parse()
	if *out == "" {
		fmt.Fprintln(os.Stderr, "-out required")
		os.Exit(2)
	}
	payload, review, err := fixtureplan.ProjectCreateReview()
	if err != nil {
		fmt.Fprintln(os.Stderr, "cannot render project review")
		os.Exit(1)
	}
	raw, err := fixtureplan.WriteJSON(review)
	if err != nil {
		fmt.Fprintln(os.Stderr, "cannot encode project review")
		os.Exit(1)
	}
	if err = os.MkdirAll(*out, 0755); err != nil {
		fmt.Fprintln(os.Stderr, "cannot create review directory")
		os.Exit(1)
	}
	for name, data := range map[string][]byte{"temporal-project-create.request.json": payload, "temporal-project-create.review.json": append(raw, '\n')} {
		if err = os.WriteFile(filepath.Join(*out, name), data, 0644); err != nil {
			fmt.Fprintln(os.Stderr, "cannot write project review")
			os.Exit(1)
		}
	}
	fmt.Println("Exact Temporal Project create request rendered for review; no cloud call or authorization.")
}
