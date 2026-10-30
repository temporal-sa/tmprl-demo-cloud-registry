package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/testinfra/fixtureplan"
)

func main() {
	out := flag.String("out", "", "New separate synthetic read-proof review directory (required)")
	flag.Parse()
	if *out == "" || filepath.Base(filepath.Clean(*out)) != fixtureplan.ReadProofID {
		fmt.Fprintln(os.Stderr, "-out must identify the exact separate read-proof review directory")
		os.Exit(2)
	}
	r, e := fixtureplan.RenderReadProofInitial()
	if e != nil {
		fmt.Fprintln(os.Stderr, "cannot render read-proof review")
		os.Exit(1)
	}
	// Do not overwrite an existing module, local state or earlier approval.
	if e = os.Mkdir(*out, 0755); e != nil {
		fmt.Fprintln(os.Stderr, "review directory must be new with an existing parent")
		os.Exit(1)
	}
	for name, v := range map[string]any{"main.tf.json": r.Initial, "review.json": r.Review, "seeds.review.json": r.Seeds, "enable.template.json": r.EnableTemplate} {
		raw, e := fixtureplan.WriteJSON(v)
		if e != nil {
			fmt.Fprintln(os.Stderr, "cannot encode read-proof review")
			os.Exit(1)
		}
		f, e := os.OpenFile(filepath.Join(*out, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
		if e != nil {
			fmt.Fprintln(os.Stderr, "cannot exclusively create review artifact")
			os.Exit(1)
		}
		_, we := f.Write(append(raw, '\n'))
		se := f.Sync()
		ce := f.Close()
		if we != nil || se != nil || ce != nil {
			fmt.Fprintln(os.Stderr, "review write uncertain; retain exact partial artifacts")
			os.Exit(1)
		}
	}
	fmt.Println("Synthetic reader initial deny-all review rendered. No plan/apply, credentials, seeds, assumptions or probes authorized.")
}
