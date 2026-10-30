package servicejournal

import (
	"context"
	"errors"
	kube "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	"testing"
)

func TestUnconstructedServiceAuthorityFailsClosed(t *testing.T) {
	if _, e := New(context.Background(), Config{}); !errors.Is(e, ErrUnverified) {
		t.Fatal("unconstructed admission produced a Service guard", e)
	}
	for _, g := range []*Guard{nil, {}} {
		if e := g.VerifyServiceIntent(context.Background(), kube.ServiceWriteIntent{}); !errors.Is(e, ErrUnverified) {
			t.Fatal("unconstructed Service authority accepted", e)
		}
	}
}
