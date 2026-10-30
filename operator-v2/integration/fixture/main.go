// A static local-only workload used by the disposable Kubernetes test tier.
package main

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

func main() {
	revision := flag.String("revision", "active", "Response body")
	ready := flag.Bool("ready", true, "Readiness result")
	probe := flag.String("probe", "", "Probe one in-cluster Service and exit")
	expected := flag.String("expect", "", "Expected probe body after controller convergence")
	flag.Parse()
	if *probe != "" {
		client := &http.Client{Timeout: time.Second}
		deadline := time.Now().Add(12 * time.Second)
		var last string
		for time.Now().Before(deadline) {
			response, err := client.Get(*probe)
			if err != nil {
				last = err.Error()
			} else {
				data, readErr := io.ReadAll(io.LimitReader(response.Body, 1024))
				_ = response.Body.Close()
				if readErr == nil && response.StatusCode == http.StatusOK && string(data) == *expected {
					_, _ = os.Stdout.Write(data)
					return
				}
				last = fmt.Sprintf("HTTP %d body %q", response.StatusCode, data)
			}
			time.Sleep(200 * time.Millisecond)
		}
		fmt.Fprintln(os.Stderr, "Service did not converge to expected revision:", last)
		os.Exit(1)
	}
	http.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		if !*ready {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_, _ = io.WriteString(w, *revision)
	})
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, *revision) })
	server := &http.Server{Addr: ":8080", ReadHeaderTimeout: 5 * time.Second}
	if err := server.ListenAndServe(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
