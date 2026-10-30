// run-aws-readproof consumes only a separately reviewed explicit inherited FD3
// anonymous pipe. Never invoke with live credentials without exact coordinator
// approval of binary/source/config/pin/account/policy and the parent handoff.
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
	flags := flag.NewFlagSet("run-aws-readproof", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var o awspermission.InvocationOptions
	flags.StringVar(&o.InvocationPath, "invocation", "", "Explicit private canonical invocation")
	flags.StringVar(&o.ExpectedInvocationSHA256, "invocation-sha256", "", "Independently reviewed exact invocation bytes")
	flags.StringVar(&o.JournalDirectory, "journal-root", "", "Explicit existing pinned private journal root")
	flags.StringVar(&o.JournalPinPath, "journal-pin", "", "Independent outside private pin file")
	flags.StringVar(&o.ExpectedJournalPinSHA256, "journal-pin-sha256", "", "Independently reviewed exact pin bytes")
	if flags.Parse(os.Args[1:]) != nil || flags.NArg() != 0 {
		io.WriteString(os.Stderr, "AWSReadProofArgumentsInvalid\n")
		os.Exit(2)
	}
	// No path, stdin, env, CLI or credential-loader fallback exists.
	pipe := os.NewFile(3, "aws-readproof-explicit-credential-pipe")
	result, err := awspermission.ExecuteInvocation(context.Background(), o, pipe)
	diagnostic := ""
	if err != nil {
		diagnostic = string(awspermission.Unproven)
		switch err {
		case awspermission.Invalid, awspermission.Denied, awspermission.Unproven, awspermission.UnexpectedAllowed, awspermission.WrongIdentity, awspermission.MetadataChanged, awspermission.ProviderFailure, awspermission.PrivacyDenied, awspermission.AlreadyAttempted:
			diagnostic = string(err.(awspermission.Error))
		}
	}
	report := struct {
		Result     awspermission.Result `json:"result"`
		Diagnostic string               `json:"diagnostic,omitempty"`
	}{result, diagnostic}
	if json.NewEncoder(os.Stdout).Encode(report) != nil || err != nil {
		os.Exit(1)
	}
}
