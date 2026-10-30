package runtimeinputs

import (
	"encoding/json"
	"fmt"
	"io"

	corev1 "k8s.io/api/core/v1"
)

// ServiceAccountCapture is a PRIVATE original execution body. Storage proves
// retention, not that the body or any observed ServiceAccount is authorized.
type ServiceAccountCapture struct {
	Target  Target
	Desired *corev1.ServiceAccount
}

func (ServiceAccountCapture) String() string               { return "ServiceAccountInputCapture{execution-only}" }
func (c ServiceAccountCapture) Format(s fmt.State, _ rune) { _, _ = io.WriteString(s, c.String()) }
func (ServiceAccountCapture) MarshalJSON() ([]byte, error) { return json.Marshal("execution-only") }

type ServiceAccountPayload struct{ private *serviceAccountPayload }
type serviceAccountPayload struct{ object *corev1.ServiceAccount }

func (ServiceAccountPayload) String() string               { return "ServiceAccountInput{private}" }
func (p ServiceAccountPayload) Format(s fmt.State, _ rune) { _, _ = io.WriteString(s, p.String()) }
func (ServiceAccountPayload) MarshalJSON() ([]byte, error) { return json.Marshal("private") }

// ObjectForProvider returns a private independent copy, never a durable DTO.
func (p ServiceAccountPayload) ObjectForProvider() *corev1.ServiceAccount {
	if p.private == nil {
		return nil
	}
	return p.private.object.DeepCopy()
}
