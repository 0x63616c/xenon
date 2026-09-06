package identity

import (
	"regexp"
	"strings"
	"testing"
)

func TestOperationReferencePreservesExactLegacyAlphabet(t *testing.T) {
	legacy := regexp.MustCompile(`^[a-zA-Z0-9-]{1,128}$`)
	values := []string{"", strings.Repeat("a", 128), strings.Repeat("a", 129), "723ef4ba-ea7b-4c25-8b7a-bf19074691f4", "legacy-replay", "user_name", "../path", "a\nb", "op_0000000000000000000001", "op_7n42DGM5Tflk9n8mt7Fhc7", "op_7n42DGM5Tflk9n8mt7Fhc8", "inc_0000000000000000000001"}
	for ch := 0; ch < 256; ch++ {
		values = append(values, string([]byte{byte(ch)}))
	}
	for _, value := range values {
		want := legacy.MatchString(value) || OperationID(value).Validate() == nil
		if got := ValidateOperationReference(value) == nil; got != want {
			t.Errorf("%q: got %v want %v", value, got, want)
		}
	}
}
