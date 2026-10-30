package temporalcloud

import (
	"fmt"
	"strings"
	"testing"
)

func TestCredentialAndMaterialFormattingNeverReflectsSecrets(t *testing.T) {
	credential, err := NewCredentials("PRIVATE-CLOUD-KEY")
	if err != nil {
		t.Fatal(err)
	}
	material := newMaterial("PRIVATE-MATERIAL")
	defer material.close()
	for _, value := range []any{credential, &credential, ConnectionOptions{Credentials: credential}, material, &material} {
		for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%X", "%d", "%f", "%p", "%#p", "%t", "%z"} {
			formatted := fmt.Sprintf(verb, value)
			if strings.Contains(formatted, "PRIVATE") || strings.Contains(formatted, "50524956415445") || strings.Contains(formatted, "[80 82 73 86 65 84 69") {
				t.Fatalf("private data exposed through %s", verb)
			}
		}
	}
}
