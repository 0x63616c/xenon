package identity

// ValidateOperationReference accepts new canonical op_ IDs and EXACTLY the
// legacy ingress alphabet [a-zA-Z0-9-]{1,128}. Compatibility is read/ingress only:
// new callers generate NewOperationID, and old journal keys are never rewritten.
func ValidateOperationReference(value string) error {
	if OperationID(value).Validate() == nil {
		return nil
	}
	if len(value) < 1 || len(value) > 128 {
		return ErrInvalid
	}
	for i := range len(value) {
		c := value[i]
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-') {
			return ErrInvalid
		}
	}
	return nil
}
