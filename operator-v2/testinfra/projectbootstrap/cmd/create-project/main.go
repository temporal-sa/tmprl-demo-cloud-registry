// create-project is a fixture-specific, separately approved human bootstrap.
// Merely building it has no effects. Never invoke it with live OAuth before the
// coordinator has reviewed its exact request, private approval and fault tests.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"io"
	"os"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/testinfra/projectbootstrap"
)

func main() {
	f := flag.NewFlagSet("create-project", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	o := projectbootstrap.Options{}
	f.StringVar(&o.OAuthConfigPath, "oauth-config", "", "Explicit private OAuth config path")
	f.StringVar(&o.Profile, "profile", "", "Exact OAuth profile")
	f.StringVar(&o.ApprovalPath, "approval", "", "Reviewed private approval JSON")
	f.StringVar(&o.RequestPath, "request", "", "Exact public reviewed request bytes")
	f.StringVar(&o.ReceiptPath, "receipt", "", "Absent private durable dispatch receipt path")
	if f.Parse(os.Args[1:]) != nil || f.NArg() != 0 {
		_, _ = io.WriteString(os.Stderr, "BootstrapArgumentsInvalid\n")
		os.Exit(2)
	}
	r, err := projectbootstrap.Execute(context.Background(), o)
	// Both values have fixed/validated safe schemas. There is no raw provider,
	// filesystem/parser error or full SDK response formatting on this path.
	result := struct {
		Receipt    projectbootstrap.Receipt `json:"receipt"`
		Diagnostic string                   `json:"diagnostic,omitempty"`
	}{Receipt: r}
	if err != nil {
		result.Diagnostic = err.Error()
	}
	if json.NewEncoder(os.Stdout).Encode(result) != nil || err != nil {
		os.Exit(1)
	}
}
