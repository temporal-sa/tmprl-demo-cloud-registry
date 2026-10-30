package main

import (
	"errors"
	"io"
	"os"
	"syscall"
	"testing"
	"time"
)

func TestOnlyExplicitPrivilegedLocalReadArgumentsAreAccepted(t *testing.T) {
	for _, args := range [][]string{{"read-local"}, {"read-local", "--json"}} {
		if _, err := arguments(args); err != nil {
			t.Fatal(err)
		}
	}
	for _, args := range [][]string{nil, {"read"}, {"read-local", "--target=foreign"}, {"read-local", "--json", "extra"}, {"--json", "read-local"}} {
		if _, err := arguments(args); err == nil {
			t.Fatal("implicit mode or override accepted")
		}
	}
}
func TestBootstrapRequiresPollablePipeAndCannotWaitForeverForEOF(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "not-a-pipe")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = bootstrapPipe(int(file.Fd()), time.Now().Add(time.Second)); err == nil {
		t.Fatal("regular file accepted")
	}
	_ = file.Close()
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer write.Close()
	fd, err := syscall.Dup(int(read.Fd()))
	if err != nil {
		t.Fatal(err)
	}
	read.Close()
	in, err := bootstrapPipe(fd, time.Now().Add(30*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	started := time.Now()
	_, err = io.ReadAll(in)
	if !errors.Is(err, os.ErrDeadlineExceeded) || time.Since(started) > time.Second {
		t.Fatal("stalled writer did not have a bounded read")
	}
}
