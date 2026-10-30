// render-namespaces reads a safe Project attestation and emits review artifacts.
// It never reads credentials, constructs clients or sends provider requests.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/testinfra/fixtureplan"
)

func main() {
	bindingPath := flag.String("binding", "", "Safe independently reviewed Project attestation JSON")
	out := flag.String("out", "", "Review output directory")
	privateBinding := flag.String("private-binding-out", "", "Optional absent private reduced Project binding JSON path")
	flag.Parse()
	if *bindingPath == "" || *out == "" {
		fail()
	}
	f, err := os.Open(*bindingPath)
	if err != nil {
		fail()
	}
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > 1<<20 {
		fail()
	}
	raw, err := io.ReadAll(io.LimitReader(f, (1<<20)+1))
	_ = f.Close()
	if err != nil || len(raw) > 1<<20 {
		fail()
	}
	var binding fixtureplan.NamespaceProjectBinding
	if json.Unmarshal(raw, &binding) != nil {
		fail()
	}
	drafts, review, err := fixtureplan.NamespaceCreateReview(binding)
	if err != nil {
		fail()
	}
	if os.MkdirAll(*out, 0755) != nil {
		fail()
	}
	for _, draft := range drafts {
		if os.WriteFile(filepath.Join(*out, "temporal-namespace-"+draft.Label+"-create.request.json"), draft.RequestJSON, 0644) != nil {
			fail()
		}
	}
	reviewJSON, err := fixtureplan.WriteJSON(review)
	if err != nil || os.WriteFile(filepath.Join(*out, "temporal-namespaces-create.review.json"), append(reviewJSON, '\n'), 0644) != nil {
		fail()
	}
	if *privateBinding != "" {
		canonical, err := json.MarshalIndent(binding, "", "  ")
		if err != nil || !filepath.IsAbs(*privateBinding) {
			fail()
		}
		info, err := os.Stat(filepath.Dir(*privateBinding))
		if err != nil || !info.IsDir() || info.Mode().Perm()&0077 != 0 {
			fail()
		}
		f, err := os.OpenFile(*privateBinding, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			fail()
		}
		_, err = f.Write(append(canonical, '\n'))
		if err != nil || f.Sync() != nil || f.Close() != nil {
			fail()
		}
	}
	fmt.Println("Three bound namespace review requests rendered; no credentials or provider calls.")
}

func fail() {
	fmt.Fprintln(os.Stderr, "Cannot render namespace review from safe Project binding")
	os.Exit(1)
}
