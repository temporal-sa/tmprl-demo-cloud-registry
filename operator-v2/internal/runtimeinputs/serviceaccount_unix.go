//go:build darwin || linux

package runtimeinputs

import (
	"bytes"
	"context"
	"encoding/json"
	"io"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	corev1 "k8s.io/api/core/v1"
)

func (s *Store) supports(f family) bool {
	switch f {
	case deploymentFamily:
		return true
	case serviceFamily:
		return s.schema == "private-render-inputs-v2" || s.schema == "private-render-inputs-v3"
	case serviceAccountFamily:
		return s.config.ServiceAccounts && s.schema == "private-render-inputs-v3"
	default:
		return false
	}
}

// RetainServiceAccount requires explicit fresh-store opt-in. Old manifests
// cannot gain this capability, even when supplied an otherwise valid body.
func (s *Store) RetainServiceAccount(ctx context.Context, c ServiceAccountCapture) (journal.InputReference, error) {
	if s == nil || !s.config.ServiceAccounts || !s.valid(c.Target) || c.Desired == nil {
		return journal.InputReference{}, ErrInvalid
	}
	o := c.Desired.DeepCopy()
	body := encode(o)
	if len(body) > 512*1024 || hash(body) != c.Target.Fingerprint || o.Name != c.Target.Name || o.Namespace != c.Target.Parent.Name || o.APIVersion != "v1" || o.Kind != "ServiceAccount" {
		return journal.InputReference{}, ErrInvalid
	}
	return s.retain(ctx, c.Target, body, serviceAccountFamily)
}

func (s *Store) ResolveServiceAccount(ctx context.Context, ref journal.InputReference, t Target) (ServiceAccountPayload, error) {
	var result ServiceAccountPayload
	body, err := s.resolve(ctx, ref, t, serviceAccountFamily)
	if err != nil {
		return result, err
	}
	var object corev1.ServiceAccount
	d := json.NewDecoder(bytes.NewReader(body))
	d.DisallowUnknownFields()
	if d.Decode(&object) != nil || d.Decode(new(any)) != io.EOF || !bytes.Equal(encode(&object), body) || hash(body) != t.Fingerprint || object.Name != t.Name || object.Namespace != t.Parent.Name || object.APIVersion != "v1" || object.Kind != "ServiceAccount" {
		return result, ErrUnproven
	}
	return ServiceAccountPayload{&serviceAccountPayload{object.DeepCopy()}}, nil
}
