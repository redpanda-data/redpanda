// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package acl

import (
	"errors"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kmsg"
)

const (
	resourceFlag     = "resource"      // deprecated
	resourceNameFlag = "resource-name" // deprecated
	namePatternFlag  = "name-pattern"  // deprecated

	topicFlag          = "topic"
	groupFlag          = "group"
	clusterFlag        = "cluster"
	txnIDFlag          = "transactional-id"
	patternFlag        = "resource-pattern-type"
	allowPrincipalFlag = "allow-principal"
	allowRoleFlag      = "allow-role"
	allowHostFlag      = "allow-host"
	denyPrincipalFlag  = "deny-principal"
	denyRoleFlag       = "deny-role"
	denyHostFlag       = "deny-host"
	operationFlag      = "operation"

	kafkaCluster = "kafka-cluster"

	rolePrefix = "RedpandaRole:"
)

var (
	// For all outputs, we either have the following headers, or the
	// following headers and an additional "Error" message column.
	headers = []string{
		"Principal",
		"Host",
		"Resource-Type",
		"Resource-Name",
		"Resource-Pattern-Type",
		"Operation",
		"Permission",
	}
	headersWithError = append(headers, "Error")
)

type (
	// Corresponding to the above, acl and aclWithMessage are the rows
	// for PrintStructFields.
	acl struct {
		Principal           string                      `json:"principal"`
		Host                string                      `json:"host"`
		ResourceType        kmsg.ACLResourceType        `json:"resource_type"`
		ResourceName        string                      `json:"resource_name"`
		ResourcePatternType kmsg.ACLResourcePatternType `json:"resource_pattern_type"`
		Operation           kmsg.ACLOperation           `json:"operation"`
		Permission          kmsg.ACLPermissionType      `json:"permission"`
	}
	aclWithMessage struct {
		Principal           string                      `json:"principal"`
		Host                string                      `json:"host"`
		ResourceType        kmsg.ACLResourceType        `json:"resource_type"`
		ResourceName        string                      `json:"resource_name"`
		ResourcePatternType kmsg.ACLResourcePatternType `json:"resource_pattern_type"`
		Operation           kmsg.ACLOperation           `json:"operation"`
		Permission          kmsg.ACLPermissionType      `json:"permission"`
		Message             string                      `json:"message"`
	}
)

// A helper function to ensure we print an empty string.
func unptr(str *string) string {
	if str == nil {
		return ""
	}
	return *str
}

func PrefixRole(roles []string) {
	for i, u := range roles {
		if !strings.HasPrefix(u, rolePrefix) {
			roles[i] = rolePrefix + u
		}
	}
}

// The acls struct contains everything we receive from flags, and one field
// that stores anything from those flags that needs parsing.
type acls struct {
	// deprecated create & delete & list flags
	resourceType           string
	resourceName           string
	oldResourcePatternType string

	// deprecated list flags
	listPermissions []string
	listPrincipals  []string
	listHosts       []string

	// create & delete & list flags
	topics          []string
	groups          []string
	cluster         bool
	txnIDs          []string
	allowPrincipals []string
	allowRoles      []string
	allowHosts      []string
	denyPrincipals  []string
	denyRoles       []string
	denyHosts       []string

	// create & delete & list flags, to be parsed
	resourcePatternType string
	operations          []string

	parsed parsed
}

// parsed contains the results of flags that need parsing.
type parsed struct {
	operations []kmsg.ACLOperation
	pattern    kmsg.ACLResourcePatternType
}

func (a *acls) addDeprecatedFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&a.resourceType, resourceFlag, "", "")
	cmd.Flags().StringVar(&a.resourceName, resourceNameFlag, "", "")
	cmd.Flags().StringVar(&a.oldResourcePatternType, namePatternFlag, "", "")
	cmd.Flags().MarkDeprecated(resourceFlag, "use --topic, --group, --transactional-id, or --cluster")
	cmd.Flags().MarkDeprecated(resourceNameFlag, "use --topic, --group, --transactional-id, or --cluster")
	cmd.Flags().MarkDeprecated(namePatternFlag, "use --resource-pattern-type")
}

func (a *acls) backcompatList() error {
	// We reject using the new flags with their replacements.
	if len(a.allowPrincipals) > 0 ||
		len(a.allowRoles) > 0 ||
		len(a.allowHosts) > 0 ||
		len(a.denyPrincipals) > 0 ||
		len(a.denyRoles) > 0 ||
		len(a.denyHosts) > 0 {
		if len(a.listPermissions) > 0 ||
			len(a.listPrincipals) > 0 ||
			len(a.listHosts) > 0 {
			return errors.New("invalid mix of new list flags and old list flags")
		}

		// Only new flags specified, no old: nothing to backcompat.
		return nil
	}

	// At this point, either everything is empty, or only old flags are
	// specified. Emptiness of old flags is significant (it implies "any").
	var permAny, permAllow, permDeny bool
	for _, perm := range a.listPermissions {
		switch strings.ToLower(perm) {
		case "any":
			permAny = true
		case "allow":
			permAllow = true
		case "deny":
			permDeny = true
		default:
			return fmt.Errorf("unrecognized permission %q", perm)
		}
	}

	// If no permissions were specified, or "any" was specified, or both
	// allow and deny were specified, then the user is asking for any
	// permission.
	permAny = permAny || len(a.listPermissions) == 0
	permAny = permAny || (permDeny && permAllow)
	// If they are asking for any permission, then they are asking for deny
	// or allow. We use these two below.
	permAllow = permAllow || permAny
	permDeny = permDeny || permAny

	// We now migrate the old flags to the new: principals/hosts get added
	// to {allow,deny}{principals,hosts} based on whether we allow or deny.
	// The builder harmonizes the rest below (only allow vs. only deny vs.
	// any).
	for _, migrate := range []struct {
		perm   bool
		source []string
		dest   *[]string
	}{
		{permAllow, a.listPrincipals, &a.allowPrincipals},
		{permAllow, a.listHosts, &a.allowHosts},
		{permDeny, a.listPrincipals, &a.denyPrincipals},
		{permDeny, a.listHosts, &a.denyHosts},
	} {
		if migrate.perm {
			*migrate.dest = append(*migrate.dest, migrate.source...)
		}
	}

	a.listPermissions = nil
	a.listPrincipals = nil
	a.listHosts = nil

	return nil
}

func (a *acls) backcompat(list bool) error {
	if list {
		if err := a.backcompatList(); err != nil {
			return err
		}
	}

	// Backwards compatibility section.
	if a.oldResourcePatternType != "" {
		a.resourcePatternType = a.oldResourcePatternType
	}
	if a.resourceType == "" && a.resourceName == "" {
		return nil
	}
	parsedType, err := kmsg.ParseACLResourceType(a.resourceType)
	if err != nil {
		return fmt.Errorf("unable to parse %s: %v", resourceFlag, err)
	}

	if parsedType == kmsg.ACLResourceTypeCluster {
		if a.resourceName != "" && a.resourceName != kafkaCluster {
			return fmt.Errorf("invalid name for %s", resourceNameFlag)
		}
		a.resourceName = kafkaCluster
	}
	if len(a.resourceName) == 0 {
		return fmt.Errorf("invalid empty %s", resourceNameFlag)
	}

	switch parsedType {
	case kmsg.ACLResourceTypeTopic:
		a.topics = append(a.topics, a.resourceName)
	case kmsg.ACLResourceTypeGroup:
		a.groups = append(a.groups, a.resourceName)
	case kmsg.ACLResourceTypeTransactionalId:
		a.txnIDs = append(a.txnIDs, a.resourceName)
	case kmsg.ACLResourceTypeCluster:
		a.cluster = true
	}

	return nil
}

func (a *acls) parseCommon() error {
	for _, op := range a.operations {
		parsed, err := kmsg.ParseACLOperation(op)
		if err != nil {
			return fmt.Errorf("invalid operation %q", op)
		}
		a.parsed.operations = append(a.parsed.operations, parsed)
	}
	if a.resourcePatternType == "" {
		a.resourcePatternType = "literal"
	}
	pattern, err := kmsg.ParseACLResourcePatternType(a.resourcePatternType)
	if err != nil {
		return fmt.Errorf("invalid resource pattern type %q", a.resourcePatternType)
	}
	a.parsed.pattern = pattern
	return nil
}

func (a *acls) createCreations() (*kadm.ACLBuilder, error) {
	if err := a.backcompat(false); err != nil {
		return nil, err
	}
	if err := a.parseCommon(); err != nil {
		return nil, err
	}

	PrefixRole(a.allowRoles)
	PrefixRole(a.denyRoles)

	a.allowPrincipals = append(a.allowPrincipals, a.allowRoles...)
	a.denyPrincipals = append(a.denyPrincipals, a.denyRoles...)

	// Using empty lists / non-Maybe functions when building create ACLs is
	// fine, since creation does not opt in to "any" when things are empty.
	b := kadm.NewACLs().
		ResourcePatternType(a.parsed.pattern).
		MaybeOperations(a.parsed.operations...). // avoid defaulting to "any" if none are provided
		Topics(a.topics...).
		Groups(a.groups...).
		MaybeClusters(a.cluster). // avoid opting in to all clusters by default
		TransactionalIDs(a.txnIDs...).
		Allow(a.allowPrincipals...).
		AllowHosts(a.allowHosts...).
		Deny(a.denyPrincipals...).
		DenyHosts(a.denyHosts...)

	b.PrefixUserExcept(rolePrefix) // add "User:" prefix to everything if needed

	return b, b.ValidateCreate()
}

func (a *acls) createDeletionsAndDescribes(
	list bool,
) (*kadm.ACLBuilder, error) {
	if err := a.backcompat(list); err != nil {
		return nil, err
	}
	if err := a.parseCommon(); err != nil {
		return nil, err
	}

	PrefixRole(a.allowRoles)
	PrefixRole(a.denyRoles)

	a.allowPrincipals = append(a.allowPrincipals, a.allowRoles...)
	a.denyPrincipals = append(a.denyPrincipals, a.denyRoles...)

	// Deleting & describing works on a filter basis: empty matches all.
	// The builder opts in to all when using functions if the input slice
	// is empty, but we can use the Maybe functions to avoid optin into all
	// by default.
	b := kadm.NewACLs().
		ResourcePatternType(a.parsed.pattern).
		Operations(a.parsed.operations...).
		MaybeTopics(a.topics...).
		MaybeGroups(a.groups...).
		MaybeClusters(a.cluster).
		MaybeTransactionalIDs(a.txnIDs...).
		MaybeAllow(a.allowPrincipals...).
		MaybeAllowHosts(a.allowHosts...).
		MaybeDeny(a.denyPrincipals...).
		MaybeDenyHosts(a.denyHosts...)

	// Resources: if no resources are specified, we use all resources.
	if !b.HasResource() {
		b.AnyResource()
	}
	// User & host: when unspecified, we default to everything. This means
	// that if a user wants to specifically filter for allowed or denied,
	// they must either allow or deny flags.
	if !b.HasPrincipals() {
		b.Allow()
		b.Deny()
	}
	if !b.HasHosts() {
		b.AllowHosts()
		b.DenyHosts()
	}

	b.PrefixUserExcept(rolePrefix) // add "User:" prefix to everything if needed

	return b, b.ValidateFilter()
}
