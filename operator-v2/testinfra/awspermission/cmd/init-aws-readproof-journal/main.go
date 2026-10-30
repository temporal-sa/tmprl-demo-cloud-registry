// init-aws-readproof-journal only initializes an explicitly supplied new empty
// local root and an absent outside pin. It never reads FD3, credentials or AWS.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"io"
	"os"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/testinfra/awspermission"
)

func main() {
	flags := flag.NewFlagSet("init-aws-readproof-journal", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var o awspermission.InitOptions
	flags.StringVar(&o.JournalDirectory, "journal-root", "", "Existing NEW empty owned0700 root")
	flags.StringVar(&o.PinPath, "pin", "", "Absent outside owned0600 pin in owned0700 parent")
	flags.StringVar(&o.BootstrapUID, "bootstrap-uid", "", "Independently retained bootstrap UID64hex")
	flags.StringVar(&o.ConfigurationSHA256, "configuration-sha256", "", "Exact reviewed configuration digest64hex")
	if flags.Parse(os.Args[1:]) != nil || flags.NArg() != 0 {
		io.WriteString(os.Stderr, "AWSReadProofArgumentsInvalid\n")
		os.Exit(2)
	}
	receipt, err := awspermission.InitializeInvocationJournal(context.Background(), o)
	diagnostic := ""
	if err != nil {
		diagnostic = string(awspermission.Unproven)
		switch err {
		case awspermission.Invalid, awspermission.Denied, awspermission.Unproven, awspermission.AlreadyAttempted:
			diagnostic = string(err.(awspermission.Error))
		}
	}
	report := struct {
		Receipt    awspermission.InitReceipt `json:"receipt"`
		Diagnostic string                    `json:"diagnostic,omitempty"`
	}{receipt, diagnostic}
	if json.NewEncoder(os.Stdout).Encode(report) != nil || err != nil {
		os.Exit(1)
	}
}
