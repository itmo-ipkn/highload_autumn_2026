package monitoring

import (
	"fmt"
	"regexp"
)

// identityPattern keeps pod and datacenter labels bounded: one token, not a
// request id, URL, or free-form error string.
var identityPattern = regexp.MustCompile(`^[A-Za-z0-9](?:[A-Za-z0-9_.-]{0,61}[A-Za-z0-9])?$`)

func ValidateIdentity(podID, datacenter string) error {
	if err := validateIdentity("POD_ID", podID); err != nil {
		return err
	}
	return validateIdentity("DATACENTER", datacenter)
}

func validateIdentity(name, value string) error {
	if value == "" {
		return fmt.Errorf("%s is required", name)
	}
	if !identityPattern.MatchString(value) {
		return fmt.Errorf("%s must be 1-63 characters and use letters, digits, '.', '_' or '-'", name)
	}
	return nil
}
