// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package shadow

import (
	"errors"
	"fmt"
	"strings"

	"connectrpc.com/connect"
)

// handleConnectError transforms Connect API errors into user-friendly error
// messages. It returns the original error if no transformation is needed, or a
// new error with a more helpful message for known error codes.
//
// The function handles:
//   - NotFound (404): Suggests running 'rpk shadow list' to see available links.
//   - PermissionDenied: Provides guidance about permissions, preserving role requirements.
//
// Parameters:
//   - err: The Connect error to handle.
//   - operation: The operation being performed (e.g., "delete", "create").
//   - linkName: The name of the shadow link (optional).
func handleConnectError(err error, operation, linkName string) error {
	if err == nil {
		return nil
	}
	var ce *connect.Error
	if errors.As(err, &ce) {
		switch ce.Code() {
		case connect.CodeNotFound:
			if linkName != "" {
				return fmt.Errorf("shadow link %q not found; to get all active shadow links run 'rpk shadow list'", linkName)
			}
			return fmt.Errorf("shadow link not found; to get all active shadow links run 'rpk shadow list'")

		case connect.CodePermissionDenied:
			msg := fmt.Sprintf("permission denied: you don't have access to %s shadow links", operation)
			// Preserve role requirement information from the original error
			if strings.Contains(ce.Message(), "superuser role required") {
				msg += " (superuser role required). Use a superuser account by updating your rpk profile (see 'rpk profile set --help')"
			} else {
				msg += "; check your roles and permissions using 'rpk security' commands"
			}
			return fmt.Errorf("%s", msg)
		}
	}
	return err
}
