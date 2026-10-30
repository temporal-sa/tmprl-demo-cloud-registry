package credentialobservation

import (
	"testing"

	tc "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/temporalcloud"
)

func TestPublicEndpointBindingPreservesExactV1WireHash(t *testing.T) {
	f := setup(t)
	c := EndpointBindingInput{Cloud: f.config.Cloud, Data: f.config.Data, TrustBindingID: f.config.TrustBindingID, ProjectID: f.config.ProjectID}
	binding, e := ExpectedEndpointBinding(c)
	if e != nil || binding.Schema != 1 || binding.Contract != EndpointBindingContract || f.records.reads != 0 || f.secrets.reads != 0 || f.probes.identityCalls != 0 {
		t.Fatal("public endpoint helper performed observation or rejected trusted public config")
	}
	// Frozen original wire layout, before public helper extraction. Adding a
	// cryptographic prefix/field/normalization here would break the v1 contract.
	legacy := hash(struct {
		Cloud, Data         Endpoint
		Trust, Project, API string
	}{c.Cloud, c.Data, c.TrustBindingID, c.ProjectID, tc.SupportedAPIVersion})
	if binding.Digest != legacy {
		t.Fatal("v1 endpoint wire hash changed")
	}
	proof, e := f.observer.Observe(bg, f.ref)
	if e != nil || proof.EndpointBindingDigest != binding.Digest {
		t.Fatal("actual observation diverged from precomputed public binding")
	}
}

func TestPublicEndpointBindingValidatesAndBindsEveryPublicInput(t *testing.T) {
	f := setup(t)
	original := EndpointBindingInput{Cloud: f.config.Cloud, Data: f.config.Data, TrustBindingID: f.config.TrustBindingID, ProjectID: f.config.ProjectID}
	baseline := must(ExpectedEndpointBinding(original))
	for name, change := range map[string]func(*EndpointBindingInput){
		"project": func(c *EndpointBindingInput) { c.ProjectID = "other-project" },
		"trust":   func(c *EndpointBindingInput) { c.TrustBindingID = "other-trust" },
		"cloud": func(c *EndpointBindingInput) {
			c.Cloud.Address = "other.example.test:443"
			c.Cloud.ServerName = "other.example.test"
		},
		"data": func(c *EndpointBindingInput) {
			c.Data.Address = "other.example.test:443"
			c.Data.ServerName = "other.example.test"
		},
		"ca-bytes": func(c *EndpointBindingInput) {
			c.Cloud.RootCAPEM = append(append([]byte(nil), c.Cloud.RootCAPEM...), '\n')
		},
	} {
		t.Run(name, func(t *testing.T) {
			c := original
			change(&c)
			binding, e := ExpectedEndpointBinding(c)
			if e != nil || binding.Digest == baseline.Digest {
				t.Fatal("public endpoint binding ignored a trusted input")
			}
		})
	}
	for _, change := range []func(*EndpointBindingInput){
		func(c *EndpointBindingInput) { c.ProjectID = "" },
		func(c *EndpointBindingInput) { c.TrustBindingID = "" },
		func(c *EndpointBindingInput) { c.Cloud.RootCAPEM = nil },
		func(c *EndpointBindingInput) { c.Data.ServerName = "foreign.example.test" },
	} {
		c := original
		change(&c)
		if _, e := ExpectedEndpointBinding(c); e == nil {
			t.Fatal("invalid public trust binding admitted")
		}
	}
	if f.records.reads != 0 || f.secrets.reads != 0 || f.probes.identityCalls != 0 {
		t.Fatal("public helper accessed dependency/credentials")
	}
}
