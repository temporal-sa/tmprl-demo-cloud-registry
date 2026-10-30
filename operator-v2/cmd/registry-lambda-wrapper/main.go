package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/lambdawrapper"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "registry Lambda wrapper:", err)
		os.Exit(1)
	}
}
func run() error {
	if err := lambdawrapper.CheckProcessSafety(); err != nil {
		return err
	}
	in, err := lambdawrapper.NewInput(os.Environ(), os.Args[1:])
	if err != nil {
		return err
	}
	var policy lambdawrapper.Policy
	var source lambdawrapper.Source
	mode := os.Getenv(lambdawrapper.BootstrapMode)
	if mode != "" {
		if mode != lambdawrapper.BootstrapFDMode {
			return &lambdawrapper.Error{Code: "WrapperInvalidBootstrapMode"}
		}
		f, openErr := bootstrapPipe(3, time.Now().Add(5*time.Second))
		if openErr != nil {
			return openErr
		}
		policy, source, err = lambdawrapper.DecodeBootstrap(f)
		closeErr := f.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return &lambdawrapper.Error{Code: "WrapperBootstrapCloseFailed"}
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	prepared, err := lambdawrapper.Prepare(ctx, in, policy, source, "/tmp")
	if err != nil {
		return err
	}
	return prepared.Execute(func(name string, args, env []string) error {
		name, err := executable(name, env)
		if err != nil {
			return err
		}
		return syscall.Exec(name, args, env)
	})
}

// The trusted producer must pass its owned pipe read end. Inherited blocking
// descriptors are not automatically pollable via os.NewFile. Set nonblocking
// BEFORE wrapping and require runtime deadline registration; regular files,
// sockets and unsupported descriptor kinds are rejected, never read blindly.
func bootstrapPipe(fd int, deadline time.Time) (*os.File, error) {
	var stat syscall.Stat_t
	if syscall.Fstat(fd, &stat) != nil {
		return nil, &lambdawrapper.Error{Code: "WrapperBootstrapUnavailable"}
	}
	if stat.Mode&syscall.S_IFMT != syscall.S_IFIFO || syscall.SetNonblock(fd, true) != nil {
		_ = syscall.Close(fd)
		return nil, &lambdawrapper.Error{Code: "WrapperBootstrapNotPollablePipe"}
	}
	f := os.NewFile(uintptr(fd), "trusted-bootstrap-pipe")
	if f == nil {
		_ = syscall.Close(fd)
		return nil, &lambdawrapper.Error{Code: "WrapperBootstrapUnavailable"}
	}
	if f.SetReadDeadline(deadline) != nil {
		_ = f.Close()
		return nil, &lambdawrapper.Error{Code: "WrapperBootstrapDeadlineUnavailable"}
	}
	return f, nil
}

// Resolve PATH from the explicit final child environment, never exec.LookPath's
// ambient process environment. There is no shell evaluation or ENOEXEC fallback.
func executable(name string, env []string) (string, error) {
	if strings.ContainsRune(name, '/') {
		return name, nil
	}
	path := ""
	for _, s := range env {
		if strings.HasPrefix(s, "PATH=") {
			path = strings.TrimPrefix(s, "PATH=")
			break
		}
	}
	for _, dir := range filepath.SplitList(path) {
		if dir == "" {
			dir = "."
		}
		candidate := filepath.Join(dir, name)
		info, err := os.Stat(candidate)
		if err == nil && info.Mode().IsRegular() && info.Mode().Perm()&0111 != 0 {
			return candidate, nil
		}
	}
	return "", &lambdawrapper.Error{Code: "WrapperExecutableUnavailable"}
}
