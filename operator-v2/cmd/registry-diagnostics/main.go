package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnosticcli"
)

func main() {
	fmt.Fprintln(os.Stderr, diagnosticcli.PrivilegeNotice)
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func arguments(args []string) (bool, error) {
	if len(args) == 1 && args[0] == "read-local" {
		return false, nil
	}
	if len(args) == 2 && args[0] == "read-local" && args[1] == "--json" {
		return true, nil
	}
	return false, &diagnosticcli.Error{Code: "UseReadLocalWithOptionalJSON"}
}
func run(args []string) error {
	asJSON, err := arguments(args)
	if err != nil {
		return err
	}
	// Prevent process settings which can enable plaintext HTTP/SDK tracing.
	for _, entry := range os.Environ() {
		key, value, _ := strings.Cut(entry, "=")
		if key == "GODEBUG" && value != "" || key == "GOTRACEBACK" && value != "" && value != "none" || strings.HasPrefix(key, "OTEL_") || key == "SSLKEYLOGFILE" || key == "AWS_SDK_GO_LOG_LEVEL" {
			return &diagnosticcli.Error{Code: "LocalUnsafeProcessSettings"}
		}
	}
	in, err := bootstrapPipe(3, time.Now().Add(5*time.Second))
	if err != nil {
		return err
	}
	b, err := diagnosticcli.DecodeBootstrap(in)
	closeErr := in.Close()
	if err != nil {
		return err
	}
	defer b.Close()
	if closeErr != nil {
		return &diagnosticcli.Error{Code: "LocalBootstrapCloseFailed"}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	view, err := diagnosticcli.ReadLocal(ctx, b)
	if err != nil {
		return err
	}
	return diagnosticcli.WriteView(os.Stdout, view, asJSON)
}

func bootstrapPipe(fd int, deadline time.Time) (*os.File, error) {
	var stat syscall.Stat_t
	if syscall.Fstat(fd, &stat) != nil {
		return nil, &diagnosticcli.Error{Code: "LocalBootstrapUnavailable"}
	}
	if stat.Mode&syscall.S_IFMT != syscall.S_IFIFO || syscall.SetNonblock(fd, true) != nil {
		_ = syscall.Close(fd)
		return nil, &diagnosticcli.Error{Code: "LocalBootstrapNotPollablePipe"}
	}
	f := os.NewFile(uintptr(fd), "owned-local-diagnostic-bootstrap")
	if f == nil {
		_ = syscall.Close(fd)
		return nil, &diagnosticcli.Error{Code: "LocalBootstrapUnavailable"}
	}
	if f.SetReadDeadline(deadline) != nil {
		_ = f.Close()
		return nil, &diagnosticcli.Error{Code: "LocalBootstrapDeadlineUnavailable"}
	}
	return f, nil
}
