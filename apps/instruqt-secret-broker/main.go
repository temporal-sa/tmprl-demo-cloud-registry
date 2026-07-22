package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	litellm "github.com/temporal-sa/tmprl-demo-cloud-registry/apps/instruqt-secret-broker/internal/providers"
)

var (
	version   = "dev"
	commit    = "unknown"
	buildDate = "unknown"
)

func main() {
	var duration string
	var budget float64
	var models stringListFlag
	var defaultModel string
	var budgetDuration string
	var rpmLimit int
	var tpmLimit int
	var maxParallelRequests int
	var showVersion bool

	flags := flag.NewFlagSet("secret-broker", flag.ExitOnError)
	flags.StringVar(&duration, "duration", "", "required LiteLLM key duration, for example 1d")
	flags.Float64Var(&budget, "budget", 0, "required LiteLLM max budget")
	flags.Var(&models, "model", "allowed LiteLLM model or pattern; repeat for multiple models")
	flags.StringVar(&defaultModel, "default-model", "", "default model exposed to workshop clients")
	flags.StringVar(&budgetDuration, "budget-duration", "", "optional LiteLLM budget reset duration")
	flags.IntVar(&rpmLimit, "rpm-limit", 0, "optional requests-per-minute limit")
	flags.IntVar(&tpmLimit, "tpm-limit", 0, "optional tokens-per-minute limit")
	flags.IntVar(&maxParallelRequests, "max-parallel-requests", 0, "optional concurrent request limit")
	flags.BoolVar(&showVersion, "version", false, "print version information")

	args := os.Args[1:]
	if len(args) > 0 && args[0] == "litellm" {
		args = args[1:]
	}

	if err := flags.Parse(args); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		os.Exit(2)
	}
	if flags.NArg() > 0 {
		fmt.Fprintf(os.Stderr, "ERROR: unexpected argument: %s\n", flags.Arg(0))
		os.Exit(2)
	}
	if showVersion {
		fmt.Printf("secret-broker %s (%s, %s)\n", version, commit, buildDate)
		return
	}

	opts := litellm.Options{
		Duration:       duration,
		Budget:         budget,
		Models:         []string(models),
		DefaultModel:   defaultModel,
		BudgetDuration: budgetDuration,
	}
	if flagWasSet(flags, "rpm-limit") {
		opts.RPMLimit = &rpmLimit
	}
	if flagWasSet(flags, "tpm-limit") {
		opts.TPMLimit = &tpmLimit
	}
	if flagWasSet(flags, "max-parallel-requests") {
		opts.MaxParallelRequests = &maxParallelRequests
	}

	if err := litellm.MintAndPersist(opts); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		os.Exit(1)
	}
}

type stringListFlag []string

func (values *stringListFlag) String() string {
	return strings.Join(*values, ",")
}

func (values *stringListFlag) Set(value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return fmt.Errorf("model must not be empty")
	}
	*values = append(*values, value)
	return nil
}

func flagWasSet(flags *flag.FlagSet, name string) bool {
	found := false
	flags.Visit(func(current *flag.Flag) {
		if current.Name == name {
			found = true
		}
	})
	return found
}
