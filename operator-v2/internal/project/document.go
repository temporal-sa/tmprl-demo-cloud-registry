// Package project decodes authoring contracts without provider or ambient state.
// Structural types are generated from the canonical repository schema; semantic
// validation and defaulting are intentionally separate, explicit operations.
package project

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

type Document struct {
	APIVersion string   `json:"apiVersion"`
	Kind       string   `json:"kind"`
	Metadata   Metadata `json:"metadata"`
	Spec       Spec     `json:"spec"`
}

// Effective is the independently defaulted runtime view. It is deliberately a
// distinct type: normalization can expand authoring shorthand into structures
// that are not valid raw authoring (for example ingress component plus routes).
// Keep Document for schema validation and capability entry points; pass Effective
// only within their runtime planning implementation.
type Effective Document

// Prepare is the public raw-authoring to effective-runtime boundary. Semantic
// checks run before encoding so omitempty cannot erase prohibited present-empty
// programmatic fields. Structural schema validation still runs before defaults.
func Prepare(authored Document) (Effective, error) {
	if issues := ValidateSemantics(authored); len(issues) > 0 {
		return Effective{}, &ValidationError{Issues: issues}
	}
	encoded, err := json.Marshal(authored)
	if err != nil {
		return Effective{}, err
	}
	validated, err := Decode(encoded)
	if err != nil {
		return Effective{}, err
	}
	return Normalize(validated)
}

type Metadata struct {
	Name        string            `json:"name"`
	UID         string            `json:"uid,omitempty"`
	Generation  int64             `json:"generation,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
	// Preserve API-owned/extension metadata without treating it as operator
	// authority. Renderers must patch only fields they own.
	Extra map[string]json.RawMessage `json:"-"`
}

func (m *Metadata) UnmarshalJSON(data []byte) error {
	type plain Metadata
	var parsed plain
	var extra map[string]json.RawMessage
	if err := json.Unmarshal(data, &extra); err != nil {
		return err
	}
	declared := map[string]json.RawMessage{}
	for _, key := range []string{"name", "uid", "generation", "labels", "annotations"} {
		if value, found := extra[key]; found {
			declared[key] = value
		}
		delete(extra, key)
	}
	known, err := json.Marshal(declared)
	if err != nil {
		return err
	}
	if err = json.Unmarshal(known, &parsed); err != nil {
		return err
	}
	*m = Metadata(parsed)
	m.Extra = extra
	return nil
}

func (m Metadata) MarshalJSON() ([]byte, error) {
	type plain Metadata
	data, err := json.Marshal(plain(m))
	if err != nil {
		return nil, err
	}
	var combined map[string]json.RawMessage
	if err = json.Unmarshal(data, &combined); err != nil {
		return nil, err
	}
	for key, value := range m.Extra {
		if !reservedMetadataField(key) {
			combined[key] = value
		}
	}
	return json.Marshal(combined)
}

func reservedMetadataField(key string) bool {
	for _, reserved := range []string{"name", "uid", "generation", "labels", "annotations"} {
		if strings.EqualFold(key, reserved) {
			return true
		}
	}
	return false
}

// Worker preserves both supported authoring shapes: a boolean or a versioned
// worker object. Object omission is distinct from explicit worker:false.
type Worker struct {
	Enabled       bool
	Versioning    *WorkerVersioning
	LivenessProbe bool
	object        bool
}

func (w *Worker) UnmarshalJSON(data []byte) error {
	if bytes.Equal(data, []byte("true")) || bytes.Equal(data, []byte("false")) {
		*w = Worker{Enabled: bytes.Equal(data, []byte("true"))}
		return nil
	}
	var object struct {
		Versioning    *WorkerVersioning `json:"versioning"`
		LivenessProbe bool              `json:"livenessProbe"`
	}
	if err := json.Unmarshal(data, &object); err != nil {
		return fmt.Errorf("invalid worker object")
	}
	if object.Versioning == nil {
		return fmt.Errorf("worker object requires versioning")
	}
	*w = Worker{Enabled: true, Versioning: object.Versioning, LivenessProbe: object.LivenessProbe, object: true}
	return nil
}

func (w Worker) MarshalJSON() ([]byte, error) {
	if !w.object && w.Versioning == nil {
		return json.Marshal(w.Enabled)
	}
	return json.Marshal(struct {
		Versioning    *WorkerVersioning `json:"versioning"`
		LivenessProbe bool              `json:"livenessProbe,omitempty"`
	}{w.Versioning, w.LivenessProbe})
}
