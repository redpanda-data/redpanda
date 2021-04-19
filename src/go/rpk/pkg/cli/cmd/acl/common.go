// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package acl

import (
	"fmt"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/hashicorp/go-multierror"
)

const (
	resourceFlag       = "resource"
	resourceNameFlag   = "resource-name"
	namePatternFlag    = "name-pattern"
	allowPrincipalFlag = "allow-principal"
	allowHostFlag      = "allow-host"
	denyPrincipalFlag  = "deny-principal"
	denyHostFlag       = "deny-host"
	operationFlag      = "operation"

	defaultClusterResourceName = "kafka-cluster"
)

// These need to match the ones in Sarama:
// https://github.com/Shopify/sarama/blob/41df78df10a9ef3c807cbe1f2814001e330fbdf1/acl_types.go#L172-L176
func supportedResources() []string {
	return []string{"any", "cluster", "group", "topic", "transactionalid"}
}

// These need to match the ones in Sarama:
// https://github.com/Shopify/sarama/blob/41df78df10a9ef3c807cbe1f2814001e330fbdf1/acl_types.go#L200-L203
func supportedPatterns() []string {
	return []string{"any", "match", "literal", "prefixed"}
}

// These need to match the ones in Sarama:
// https://github.com/Shopify/sarama/blob/41df78df10a9ef3c807cbe1f2814001e330fbdf1/acl_types.go#L68-L79
func supportedOperations() []string {
	return []string{
		"any",
		"all",
		"read",
		"write",
		"create",
		"delete",
		"alter",
		"describe",
		"clusteraction",
		"describeconfigs",
		"alterconfigs",
		"idempotentwrite",
	}
}

// These need to match the ones in Sarama:
// https://github.com/Shopify/sarama/blob/41df78df10a9ef3c807cbe1f2814001e330fbdf1/acl_types.go#L68-L79
func supportedPermissions() []string {
	return []string{"any", "deny", "allow"}
}

func parseOperations(operations []string) ([]sarama.AclOperation, error) {
	var errs error
	var ops []sarama.AclOperation
	for _, operation := range operations {
		var op sarama.AclOperation
		err := op.UnmarshalText([]byte(operation))
		if err != nil {
			errs = multierror.Append(errs, err)
			continue
		}
		ops = append(ops, op)
	}
	if errs != nil {
		return nil, errs
	}
	return ops, nil
}

func parsePermissions(
	permissions []string,
) ([]sarama.AclPermissionType, error) {
	var errs error
	var perms []sarama.AclPermissionType
	for _, p := range permissions {
		var perm sarama.AclPermissionType
		err := perm.UnmarshalText([]byte(p))
		if err != nil {
			errs = multierror.Append(errs, err)
			continue
		}
		perms = append(perms, perm)
	}
	if errs != nil {
		return nil, errs
	}
	return perms, nil
}

func checkSupported(supported []string, val string, flag string) error {
	if val == "" {
		return fmt.Errorf(
			"--%s can't be empty. Supported values: %s.",
			flag,
			strings.Join(supported, ", "),
		)
	}
	val = strings.ToLower(val)
	for _, s := range supported {
		if val == s {
			return nil
		}
	}
	return fmt.Errorf(
		"invalid %s '%s'. Supported values: %s.",
		flag,
		val,
		strings.Join(supported, ", "),
	)
}
