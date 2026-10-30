package awssecrets

import (
	"fmt"
	"strings"
	"testing"
)

func TestCredentialFormattingNeverReflectsSecrets(t *testing.T) {
	credential, err := NewCredentials("PRIVATE-ACCESS", "PRIVATE-SECRET", "PRIVATE-SESSION")
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []any{credential, &credential, ConnectionOptions{Credentials: credential}} {
		for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%X", "%d", "%f", "%p", "%#p", "%t", "%z"} {
			formatted := fmt.Sprintf(verb, value)
			if strings.Contains(formatted, "PRIVATE") {
				t.Fatalf("credential exposed through %s", verb)
			}
		}
	}
}
