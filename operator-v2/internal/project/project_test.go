package project

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const minimal = `apiVersion: registry.tmprl-demo.cloud/v1alpha1
kind: DemoProject
metadata:
  name: example
  namespace: authoring-only-extension
spec:
  source:
    repo: https://github.com/temporal-sa/example
  builds:
    web:
      context: .
      dockerfile: Dockerfile
  components:
    web:
      build: web
      ports:
        - containerPort: 8080
          servicePort: 80
  ingress:
    subdomain: example
`

func TestCurrentProjectFixturesDecodeAndNormalize(t *testing.T) {
	var files []string
	for _, pattern := range []string{"../../../projects/demo/*.yaml", "../../../projects/operator/*.yaml"} {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, matches...)
	}
	if len(files) < 20 {
		t.Fatal("current authoring fixture inventory unexpectedly missing")
	}
	for _, path := range files {
		t.Run(filepath.Base(path), func(t *testing.T) {
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			document, err := Decode(data)
			if err != nil {
				t.Fatal(err)
			}
			effective, err := Normalize(document)
			if err != nil {
				t.Fatal(err)
			}
			if effective.Metadata.Name != document.Metadata.Name || effective.Spec.Source.Branch == nil {
				t.Fatal("normalization lost project identity/defaults")
			}
		})
	}
}

func TestDefaultingPreservesOmissionAndExplicitValues(t *testing.T) {
	document, err := Decode([]byte(minimal))
	if err != nil {
		t.Fatal(err)
	}
	component := document.Spec.Components["web"]
	zero := 0
	component.Replicas = &zero
	component.Worker = &Worker{Enabled: false}
	document.Spec.Components["web"] = component
	effective, err := Normalize(document)
	if err != nil {
		t.Fatal(err)
	}
	if document.Spec.Source.Branch != nil || document.Spec.Components["web"].Ports[0].Name != nil {
		t.Fatal("defaulting mutated authoring state")
	}
	if *effective.Spec.Source.Branch != "main" || *effective.Spec.Builds["web"].Dockerfile != "Dockerfile" {
		t.Fatal("source/build defaults missing")
	}
	if *effective.Spec.Components["web"].Replicas != 0 || effective.Spec.Components["web"].Worker.Enabled {
		t.Fatal("explicit zero/false lost")
	}
	if *effective.Spec.Components["web"].Ports[0].Name != "http" || effective.Spec.Ingress.Routes[0].Component != "web" {
		t.Fatal("port/default-route derivation changed")
	}
	*effective.Spec.Components["web"].Ports[0].Name = "changed"
	if document.Spec.Components["web"].Ports[0].Name != nil {
		t.Fatal("normalized component aliases source")
	}
	encoded, _ := json.Marshal(effective)
	if !bytes.Contains(encoded, []byte(`"worker":false`)) || !bytes.Contains(encoded, []byte(`"namespace":"authoring-only-extension"`)) {
		t.Fatal("union/extension metadata lost during normalization")
	}
}

func TestInvalidDocumentsHaveSafePreciseErrors(t *testing.T) {
	for _, scenario := range []struct{ name, data, code, pointer string }{
		{"duplicate YAML", strings.Replace(minimal, "name: example", "name: example\n  name: secret-sentinel", 1), "DuplicateKey", "/metadata/name"},
		{"duplicate JSON", `{"apiVersion":"one","apiVersion":"secret-sentinel"}`, "DuplicateKey", "/apiVersion"},
		{"multiple", minimal + "\n---\nsecret-sentinel", "MultipleDocuments", ""},
		{"unknown field", strings.Replace(minimal, "  ingress:", "  secretField: secret-sentinel\n  ingress:", 1), "SchemaViolation", "/spec"},
		{"unknown build", strings.Replace(minimal, "build: web", "build: missing", 1), "UnknownBuild", "/spec/components/web/build"},
		{"oversized", strings.Repeat(" ", MaxDocumentBytes+1), "DocumentTooLarge", ""},
		{"array root", "[secret-sentinel]", "InvalidDocument", ""},
		{"unsafe tag", strings.Replace(minimal, "context: .", "context: !!binary c2VjcmV0LXNlbnRpbmVs", 1), "UnsupportedYAMLTag", "/spec/builds/web/context"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			_, err := Decode([]byte(scenario.data))
			var invalid *ValidationError
			if !errors.As(err, &invalid) || len(invalid.Issues) == 0 {
				t.Fatalf("expected safe validation issue, got %v", err)
			}
			if invalid.Issues[0].Code != scenario.code || invalid.Issues[0].Pointer != scenario.pointer {
				t.Fatalf("got %+v", invalid.Issues)
			}
			encoded, _ := json.Marshal(invalid)
			if bytes.Contains(encoded, []byte("secret-sentinel")) || strings.Contains(err.Error(), "secret-sentinel") {
				t.Fatalf("rejected scalar leaked: %s", encoded)
			}
		})
	}
}

func TestCanonicalSchemaHasNotDrifted(t *testing.T) {
	canonical, err := os.ReadFile("../../../schemas/project.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(canonical, schemaBytes) {
		t.Fatal("embedded schema differs: run python3 tools/generate_project_types.py")
	}
	if _, err := (noRemoteSchemas{}).Load("https://example.invalid/schema"); err == nil {
		t.Fatal("remote schema loading unexpectedly enabled")
	}
}

func TestRecursiveAliasFailsBeforeExpansion(t *testing.T) {
	data := strings.Replace(minimal, "namespace: authoring-only-extension", "namespace: &cycle [*cycle]", 1)
	_, err := Decode([]byte(data))
	var invalid *ValidationError
	if !errors.As(err, &invalid) || invalid.Issues[0].Code != "DocumentComplexity" {
		t.Fatalf("unbounded YAML alias was not rejected: %v", err)
	}
}

func TestYAMLMergeOverrideMatchesExistingAuthoringLoader(t *testing.T) {
	data := strings.Replace(minimal, "  namespace: authoring-only-extension", "  base: &base {generation: 1}\n  <<: *base\n  generation: 2", 1)
	document, err := Decode([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	if document.Metadata.Generation != 2 {
		t.Fatal("explicit metadata did not override merged value")
	}
}

func TestAmbiguousIngressAndCleanedRootCertificatePathsAreRejected(t *testing.T) {
	document := authored(t)
	document.Spec.Components["second"] = document.Spec.Components["web"]
	if _, err := Normalize(document); err == nil {
		t.Fatal("ambiguous default ingress targets should require explicit routes")
	}
	document = authored(t)
	document.Spec.Temporal = &Temporal{Auth: &TemporalAuth{Mode: pointer("mtls")}}
	changeComponent(&document, func(c *Component) {
		c.TemporalMtls = &TemporalMtls{CertFiles: &TemporalMtlsCertFiles{Paths: &TemporalMtlsCertFilePaths{CACert: pointer("/shared/../ca.crt"), ClientCert: pointer("/shared/../tls.crt"), ClientKey: pointer("/shared/../tls.key")}}}
	})
	if !containsCode(ValidateSemantics(document), "UnsafeCertificateMount") {
		t.Fatal("cleaned certificate paths cannot resolve to a root mount")
	}
}

func TestExpandedScalarBytesAreBoundedBeforeDecode(t *testing.T) {
	// This compact input expands only hundreds of nodes, but many megabytes
	// of scalar data. Node-count and input-size guards alone are insufficient.
	data := strings.Replace(minimal, "  namespace: authoring-only-extension", "  blob: &blob "+strings.Repeat("x", 32768)+"\n  copies: ["+strings.Repeat("*blob,", 200)+"*blob]", 1)
	if len(data) > MaxDocumentBytes {
		t.Fatal("regression fixture must remain compact")
	}
	_, err := Decode([]byte(data))
	var invalid *ValidationError
	if !errors.As(err, &invalid) || invalid.Issues[0].Code != "DocumentComplexity" {
		t.Fatalf("expanded scalar budget was not enforced: %v", err)
	}
}

func TestJSONEscapingCannotExceedEncodedDocumentBudget(t *testing.T) {
	data := strings.Replace(minimal, "  namespace: authoring-only-extension", "  escaped: "+strings.Repeat("<", MaxDocumentBytes/5), 1)
	_, err := Decode([]byte(data))
	var invalid *ValidationError
	if !errors.As(err, &invalid) || invalid.Issues[0].Code != "DocumentTooLarge" {
		t.Fatalf("encoded representation budget was not enforced: %v", err)
	}
}

func TestRequiredFalseAndVersionedWorkerRoundTrip(t *testing.T) {
	data := strings.Replace(minimal, "      ports:", "      temporalProxy:\n        enabled: false\n      worker:\n        versioning:\n          enabled: false\n      ports:", 1)
	document, err := Decode([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(encoded, []byte(`"temporalProxy":{"enabled":false}`)) {
		t.Fatalf("required false omitted: %s", encoded)
	}
	roundtrip, err := Decode(encoded)
	if err != nil {
		t.Fatal(err)
	}
	worker := roundtrip.Spec.Components["web"].Worker
	if worker == nil || !worker.Enabled || worker.Versioning == nil {
		t.Fatal("object-form worker contract lost")
	}
}

func TestEmptyGateInputRemainsDistinctFromOmission(t *testing.T) {
	data := strings.Replace(minimal, "      ports:", "      worker:\n        versioning:\n          enabled: true\n          rollout:\n            gate:\n              workflowType: ExampleGate\n              input: {}\n      ports:", 1)
	document, err := Decode([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	effective, err := Normalize(document)
	if err != nil {
		t.Fatal(err)
	}
	gate := effective.Spec.Components["web"].Worker.Versioning.Rollout.Gate
	if gate.Input == nil || len(*gate.Input) != 0 {
		t.Fatal("explicit empty gate input was omitted")
	}
	encoded, err := json.Marshal(effective)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(encoded, []byte(`"input":{}`)) {
		t.Fatal("empty workflow argument changed during serialization")
	}
	absent := strings.Replace(data, "              input: {}\n", "", 1)
	document, err = Decode([]byte(absent))
	if err != nil {
		t.Fatal(err)
	}
	effective, err = Normalize(document)
	if err != nil {
		t.Fatal(err)
	}
	if effective.Spec.Components["web"].Worker.Versioning.Rollout.Gate.Input != nil {
		t.Fatal("omitted gate input became an argument")
	}
}

func TestSemanticReferencesAndProgrammaticDefaultingFailSafely(t *testing.T) {
	document, err := Decode([]byte(minimal))
	if err != nil {
		t.Fatal(err)
	}
	component := document.Spec.Components["web"]
	component.Ports = append(component.Ports, ComponentPort{ContainerPort: 9090})
	document.Spec.Components["web"] = component
	if _, err = Normalize(document); err == nil {
		t.Fatal("multiple unnamed ports should fail before route dereference")
	}
	document, err = Decode([]byte(minimal))
	if err != nil {
		t.Fatal(err)
	}
	document.Spec.Ingress.Routes = []IngressRoute{{Component: "missing", Port: "http"}}
	if issues := ValidateReferences(document); len(issues) != 1 || issues[0].Code != "UnknownComponent" {
		t.Fatalf("missing route component: %+v", issues)
	}
}

func TestMetadataExtensionsCannotReintroduceKnownIdentityFields(t *testing.T) {
	metadata := Metadata{Name: "example", Extra: map[string]json.RawMessage{"uid": json.RawMessage(`"foreign-id"`), "GENERATION": json.RawMessage(`99`), "labels": json.RawMessage(`{"forged":"true"}`), "namespace": json.RawMessage(`"preserved"`)}}
	encoded, err := json.Marshal(metadata)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(encoded, []byte("foreign-id")) || bytes.Contains(encoded, []byte("GENERATION")) || bytes.Contains(encoded, []byte("forged")) {
		t.Fatalf("extension overwrote omitted known metadata: %s", encoded)
	}
	var decoded Metadata
	if err = json.Unmarshal([]byte(`{"name":"example","UID":"foreign-id","generation":2}`), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.UID != "" || decoded.Generation != 2 {
		t.Fatal("case-insensitive extension contaminated known fields")
	}
}

func TestNormalizeValidatesForbiddenPresenceBeforeOmittingFields(t *testing.T) {
	document := authored(t)
	changeComponent(&document, func(c *Component) {
		c.Serverless = &Serverless{Provider: "aws-lambda"}
		c.Ports = nil
		c.Secrets = []DNSLabel{}
	})
	_, err := Normalize(document)
	var invalid *ValidationError
	if !errors.As(err, &invalid) || !containsCode(invalid.Issues, "UnsupportedLambdaField") {
		t.Fatalf("forbidden empty field vanished during normalization: %v", err)
	}
	_, err = Prepare(document)
	if !errors.As(err, &invalid) || !containsCode(invalid.Issues, "UnsupportedLambdaField") {
		t.Fatalf("forbidden empty field vanished at public preparation boundary: %v", err)
	}
}

func TestEffectiveIngressExpansionPreservesRawAuthoringBoundary(t *testing.T) {
	for _, ingress := range []Ingress{
		{Subdomain: "example", Component: pointer(DNSLabel("web")), Port: pointer(DNSLabel("http"))},
		{Subdomain: "example", Routes: []IngressRoute{{Component: "web", Port: "http"}}},
		{Subdomain: "example"},
	} {
		d := authored(t)
		d.Spec.Ingress = &ingress
		before, _ := json.Marshal(d)
		effective, err := Prepare(d)
		if err != nil {
			t.Fatal(err)
		}
		if len(effective.Spec.Ingress.Routes) != 1 {
			t.Fatal("effective route expansion missing")
		}
		if _, raw := any(effective).(Document); raw {
			t.Fatal("effective and authoring types are interchangeable")
		}
		after, _ := json.Marshal(d)
		if !bytes.Equal(before, after) {
			t.Fatal("preparation modified authoring")
		}
		// Subsequent capability preparation starts from the retained raw input.
		if _, err = Prepare(d); err != nil {
			t.Fatal(err)
		}
	}
}
