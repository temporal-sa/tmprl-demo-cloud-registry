package credentialobservation

import tc "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/temporalcloud"

const EndpointBindingContract = "temporal-candidate-namespace-auth-endpoints-v1"

type EndpointBindingInput struct {
	Cloud, Data               Endpoint
	TrustBindingID, ProjectID string
}

// EndpointBinding names the existing v1 canonical hash domain. Digest retains
// the EXACT v1 wire hash: no new cryptographic prefix or normalization is added.
// A future changed hash domain must use a separately reviewed contract/version.
type EndpointBinding struct {
	Schema   int
	Contract string
	Digest   string
}

// ExpectedEndpointBinding derives public bootstrap evidence without clients,
// credentials, ambient configuration or network calls. It validates the same
// explicit CA/SNI/endpoints as New. It grants no authority and proves no actual
// endpoint/principal permissions. The actor must pin this BEFORE observation,
// never learn an expected digest from the first network response.
func ExpectedEndpointBinding(c EndpointBindingInput) (EndpointBinding, error) {
	if !identifier.MatchString(c.ProjectID) || !identifier.MatchString(c.TrustBindingID) {
		return EndpointBinding{}, failure(Invalid, Metadata)
	}
	if _, e := endpointTLS(c.Cloud); e != nil {
		return EndpointBinding{}, e
	}
	if _, e := endpointTLS(c.Data); e != nil {
		return EndpointBinding{}, e
	}
	return EndpointBinding{Schema: 1, Contract: EndpointBindingContract, Digest: endpointBindingDigest(c)}, nil
}

// This is the single v1 hash implementation shared by bootstrap and actual
// output. Field names/order, CA bytes and fixed API version are compatibility
// data; do not silently change or duplicate it in another package.
func endpointBindingDigest(c EndpointBindingInput) string {
	return hash(struct {
		Cloud, Data         Endpoint
		Trust, Project, API string
	}{c.Cloud, c.Data, c.TrustBindingID, c.ProjectID, tc.SupportedAPIVersion})
}
