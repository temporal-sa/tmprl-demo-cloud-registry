// create-namespace is a separately reviewed human fixture bootstrap. Building
// it has no effects. Live execution requires explicit coordinator approval.
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
	f := flag.NewFlagSet("create-namespace", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	o := projectbootstrap.NamespaceOptions{}
	f.StringVar(&o.OAuthConfigPath, "oauth-config", "", "Explicit private OAuth config path")
	f.StringVar(&o.Profile, "profile", "", "Exact OAuth profile")
	f.StringVar(&o.ApprovalPath, "approval", "", "Reviewed private namespace approval JSON")
	f.StringVar(&o.RequestPath, "request", "", "Exact public reviewed namespace request bytes")
	f.StringVar(&o.ReceiptPath, "receipt", "", "Approval-bound absent private dispatch receipt path")
	f.StringVar(&o.ParentBindingPath, "parent-binding", "", "Strict private immutable Project binding JSON")
	if f.Parse(os.Args[1:]) != nil || f.NArg() != 0 {
		_, _ = io.WriteString(os.Stderr, "NamespaceBootstrapArgumentsInvalid\n")
		os.Exit(2)
	}
	r, err := projectbootstrap.ExecuteNamespace(context.Background(), o)
	result := struct {
		Receipt    projectbootstrap.NamespaceReceipt `json:"receipt"`
		Diagnostic string                            `json:"diagnostic,omitempty"`
	}{Receipt: r}
	if err != nil {
		result.Diagnostic = err.Error()
	}
	if json.NewEncoder(os.Stdout).Encode(result) != nil || err != nil {
		os.Exit(1)
	}
}
