package oauth

import (
	"fmt"
)

// TokenResponseError is the error returned from 4xx responses.
type TokenResponseError struct {
	Code             int    `json:"-"`
	Err              string `json:"error"`
	ErrorDescription string `json:"error_description"`
}

// Error implements the error interface.
func (t *TokenResponseError) Error() string {
	if t.ErrorDescription != "" {
		return t.Err + ": " + t.ErrorDescription
	}
	return t.Err
}

// BadClientTokenError is returned when the client ID is invalid or some other
// error occurs. This can be used as a hint that the client ID needs to be
// cleared as well.
type BadClientTokenError struct {
	Err error
}

func (e *BadClientTokenError) Error() string {
	return fmt.Sprintf("invalid client token: %v", e.Err)
}
