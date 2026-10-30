package kubernetes

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestPrivateCredentialAndCauseFormatting(t *testing.T) {
	credential := Credentials{private: &credentialValues{[]byte("PRIVATE-CA"), []byte("PRIVATE-CERT"), []byte("PRIVATE-KEY")}}
	providerError := failure(ParentUnavailable, errors.New("PRIVATE-ERROR-SENTINEL")).(*Error)
	for _, value := range []any{credential, &credential, Connection{Credentials: credential}, providerError, *providerError} {
		for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%X", "%d", "%f", "%p", "%#p", "%t", "%z"} {
			formatted := fmt.Sprintf(verb, value)
			if strings.Contains(formatted, "PRIVATE") || strings.Contains(formatted, "50524956415445") || strings.Contains(formatted, "[80 82 73 86 65 84 69") {
				t.Fatalf("private data exposed through %s", verb)
			}
		}
	}
	if !strings.Contains(providerError.Unwrap().Error(), "PRIVATE-ERROR") {
		t.Fatal("explicit cause inspection no longer works")
	}
}
