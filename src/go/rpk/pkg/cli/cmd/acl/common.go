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
	"errors"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/types"
)

const (
	resourceFlag       = "resource"      // deprecated
	resourceNameFlag   = "resource-name" // deprecated
	namePatternFlag    = "name-pattern"  // deprecated
	topicFlag          = "topic"
	groupFlag          = "group"
	clusterFlag        = "cluster"
	txnIDFlag          = "transactional-id"
	patternFlag        = "resource-pattern-type"
	allowPrincipalFlag = "allow-principal"
	allowHostFlag      = "allow-host"
	denyPrincipalFlag  = "deny-principal"
	denyHostFlag       = "deny-host"
	operationFlag      = "operation"
	anyFlag            = "any"

	kafkaCluster = "kafka-cluster"
)

var (
	allIndividualFlags = []string{
		topicFlag,
		groupFlag,
		clusterFlag,
		txnIDFlag,
		patternFlag,
		allowPrincipalFlag,
		allowHostFlag,
		denyPrincipalFlag,
		denyHostFlag,
		operationFlag,
	}

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
		Principal           string
		Host                string
		ResourceType        kmsg.ACLResourceType
		ResourceName        string
		ResourcePatternType kmsg.ACLResourcePatternType
		Operation           kmsg.ACLOperation
		Permission          kmsg.ACLPermissionType
	}
	aclWithMessage struct {
		Principal           string
		Host                string
		ResourceType        kmsg.ACLResourceType
		ResourceName        string
		ResourcePatternType kmsg.ACLResourcePatternType
		Operation           kmsg.ACLOperation
		Permission          kmsg.ACLPermissionType
		Message             string
	}
)

// A helper function to ensure we print an empty string.
func unptr(str *string) string {
	if str == nil {
		return ""
	}
	return *str
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
	topics              []string
	groups              []string
	cluster             bool
	txnIDs              []string
	resourcePatternType string
	allowPrincipals     []string
	allowHosts          []string
	denyPrincipals      []string
	denyHosts           []string
	operations          []string

	// delete & list
	any []string

	parsed parsed
}

// parsed contains the parsed results of any flags that need parsing.
type parsed struct {
	// clusters contains either no elements, or one "kafka-cluster" element.
	clusters   []string
	operations []kmsg.ACLOperation
	pattern    kmsg.ACLResourcePatternType

	anyAll            bool
	anyTopic          bool
	anyGroup          bool
	anyCluster        bool
	anyTxn            bool
	anyPattern        bool
	anyAllowPrincipal bool
	anyAllowHost      bool
	anyDenyPrincipal  bool
	anyDenyHost       bool
	anyOperation      bool
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
	if len(a.any) > 0 ||
		len(a.allowPrincipals) > 0 ||
		len(a.allowHosts) > 0 ||
		len(a.denyPrincipals) > 0 ||
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

	// If no permissions were specified, or any was specified, or both
	// allow and deny were specified, then the user is asking for any
	// permission.
	permAny = permAny || len(a.listPermissions) == 0
	permAny = permAny || (permDeny && permAllow)
	// If they are asking for any permission, then they are asking for deny
	// or allow. We use these two below.
	permAllow = permAllow || permAny
	permDeny = permDeny || permAny

	for _, migrate := range []struct {
		perm    bool
		source  []string
		anyFlag string
		dest    *[]string
	}{
		{permAllow, a.listPrincipals, allowPrincipalFlag, &a.allowPrincipals},
		{permAllow, a.listHosts, allowHostFlag, &a.allowHosts},
		{permDeny, a.listPrincipals, denyPrincipalFlag, &a.denyPrincipals},
		{permDeny, a.listHosts, denyHostFlag, &a.denyHosts},
	} {
		if !migrate.perm {
			continue
		}
		if len(migrate.source) == 0 {
			a.any = append(a.any, migrate.anyFlag)
			continue
		}
		for _, value := range migrate.source {
			*migrate.dest = append(*migrate.dest, value)
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
	if a.cluster {
		a.parsed.clusters = []string{kafkaCluster}
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

func (a *acls) parseAny() error {
	// For deletions, we never opt in by default to match anything. The
	// --any flag can be used to opt in other flags.
	p := &a.parsed
	if len(a.any) == 1 && (a.any[0] == "all" || a.any[0] == "any") {
		a.any = allIndividualFlags
		a.parsed.anyAll = true
	}

	// We set a fake element into our slices so that the ranges
	// in our createXyz loops iterate. We do not use this fake
	// element because we check the relevant anyXyz field.
	for _, any := range a.any {
		var set *[]string
		doSet := func() { *set = []string{"injection for ranges"} }
		switch any {
		default:
			return fmt.Errorf("unrecognized --any value %s", any)

		case topicFlag:
			set = &a.topics
			p.anyTopic = true

		case groupFlag:
			set = &a.groups
			p.anyGroup = true

		case clusterFlag:
			set = &a.parsed.clusters
			p.anyCluster = true

		case txnIDFlag:
			set = &a.txnIDs
			p.anyTxn = true

		case patternFlag:
			doSet = func() { a.parsed.pattern = kmsg.ACLResourcePatternTypeAny }
			p.anyPattern = true

		case allowPrincipalFlag:
			set = &a.allowPrincipals
			p.anyAllowPrincipal = true

		case allowHostFlag:
			set = &a.allowHosts
			p.anyAllowHost = true

		case denyPrincipalFlag:
			set = &a.denyPrincipals
			p.anyDenyPrincipal = true

		case denyHostFlag:
			set = &a.denyHosts
			p.anyDenyHost = true

		case operationFlag:
			doSet = func() { a.parsed.operations = []kmsg.ACLOperation{kmsg.ACLOperationAny} }
			p.anyOperation = true
		}

		doSet()
	}
	return nil
}

func (a *acls) createCreations() ([]kmsg.CreateACLsRequestCreation, error) {
	if err := a.backcompat(false); err != nil {
		return nil, err
	}
	if err := a.parseCommon(); err != nil {
		return nil, err
	}

	// ANY operation, and ANY / MATCH pattern are invalid for creations.
	for _, op := range a.parsed.operations {
		if op == kmsg.ACLOperationAny {
			return nil, fmt.Errorf("invalid --%s for creating ACLs: %v", operationFlag, op)
		}
	}
	switch a.parsed.pattern {
	case kmsg.ACLResourcePatternTypeLiteral,
		kmsg.ACLResourcePatternTypePrefixed:
	default:
		return nil, fmt.Errorf("invalid --%s for creating ACLs: %v", patternFlag, a.parsed.pattern)
	}

	// We do not require principals nor hosts; on return, if we create no
	// ACLs, we will exit saying so.
	//
	// Any allow or deny principals with a corresponding empty hosts
	// defaults the host to '*', meaning we allow or deny all hosts.
	if len(a.allowPrincipals) != 0 && len(a.allowHosts) == 0 {
		a.allowHosts = []string{"*"}
	}
	if len(a.denyPrincipals) != 0 && len(a.denyHosts) == 0 {
		a.denyHosts = []string{"*"}
	}
	if len(a.allowHosts) != 0 && len(a.allowPrincipals) == 0 {
		return nil, fmt.Errorf("invalid --%s with no --%s", allowHostFlag, allowPrincipalFlag)
	}
	if len(a.denyHosts) != 0 && len(a.denyPrincipals) == 0 {
		return nil, fmt.Errorf("invalid --%s with no --%s", denyHostFlag, denyPrincipalFlag)
	}

	// The creations below are ordered / sorted such that our responses
	// will have nicer output, because we receive responses in the order
	// that we issue the creations.
	var creations []kmsg.CreateACLsRequestCreation
	for _, typeNames := range []struct {
		t     kmsg.ACLResourceType
		names []string
	}{
		{kmsg.ACLResourceTypeTopic, a.topics},
		{kmsg.ACLResourceTypeGroup, a.groups},
		{kmsg.ACLResourceTypeCluster, a.parsed.clusters},
		{kmsg.ACLResourceTypeTransactionalId, a.txnIDs},
	} {
		for _, name := range typeNames.names {
			for _, op := range a.parsed.operations {
				for _, perm := range []struct {
					principals []string
					hosts      []string
					permType   kmsg.ACLPermissionType
				}{
					{a.allowPrincipals, a.allowHosts, kmsg.ACLPermissionTypeAllow},
					{a.denyPrincipals, a.denyHosts, kmsg.ACLPermissionTypeDeny},
				} {
					for _, principal := range perm.principals {
						for _, host := range perm.hosts {
							creation := kmsg.NewCreateACLsRequestCreation()
							creation.ResourceType = typeNames.t
							creation.ResourceName = name
							creation.ResourcePatternType = a.parsed.pattern
							creation.Operation = op
							creation.Principal = principal
							creation.Host = host
							creation.PermissionType = perm.permType
							creations = append(creations, creation)
						}
					}
				}
			}
		}
	}
	types.Sort(creations)
	return creations, nil
}

func (a *acls) createDeletionsAndDescribes(
	list bool,
) ([]kmsg.DeleteACLsRequestFilter, []*kmsg.DescribeACLsRequest, error) {
	if err := a.backcompat(list); err != nil {
		return nil, nil, err
	}
	if err := a.parseCommon(); err != nil {
		return nil, nil, err
	}
	if err := a.parseAny(); err != nil {
		return nil, nil, err
	}

	// Asking for **everything** allows us to take a single-request shortcut.
	if a.parsed.anyAll {
		deletion := kmsg.NewDeleteACLsRequestFilter()
		describe := kmsg.NewPtrDescribeACLsRequest()

		deletion.ResourceType = kmsg.ACLResourceTypeAny
		describe.ResourceType = kmsg.ACLResourceTypeAny

		deletion.ResourcePatternType = kmsg.ACLResourcePatternTypeAny
		describe.ResourcePatternType = kmsg.ACLResourcePatternTypeAny

		deletion.Operation = kmsg.ACLOperationAny
		describe.Operation = kmsg.ACLOperationAny

		deletion.PermissionType = kmsg.ACLPermissionTypeAny
		describe.PermissionType = kmsg.ACLPermissionTypeAny

		return []kmsg.DeleteACLsRequestFilter{deletion}, []*kmsg.DescribeACLsRequest{describe}, nil
	}

	// We require either both hosts & principals to be specified, or an
	// "any" opt in for the missing host / principal.
	if len(a.allowHosts) != 0 && len(a.allowPrincipals) == 0 && !a.parsed.anyAllowPrincipal {
		return nil, nil, fmt.Errorf("invalid --%[1]s with no --%[2]s and no --%[3]s=%[2]s", allowHostFlag, allowPrincipalFlag, anyFlag)
	}
	if len(a.allowPrincipals) != 0 && len(a.allowHosts) == 0 && !a.parsed.anyAllowHost {
		return nil, nil, fmt.Errorf("invalid --%[1]s with no --%[2]s and no --%[3]s=%[2]s", allowPrincipalFlag, allowHostFlag, anyFlag)
	}
	if len(a.denyHosts) != 0 && len(a.denyPrincipals) == 0 && !a.parsed.anyDenyPrincipal {
		return nil, nil, fmt.Errorf("invalid --%[1]s with no --%[2]s and no --%[3]s=%[2]s", denyHostFlag, denyPrincipalFlag, anyFlag)
	}
	if len(a.denyPrincipals) != 0 && len(a.denyHosts) == 0 && !a.parsed.anyDenyHost {
		return nil, nil, fmt.Errorf("invalid --%[1]s with no --%[2]s and no --%[3]s=%[2]s", denyPrincipalFlag, denyHostFlag, anyFlag)
	}

	var deletions []kmsg.DeleteACLsRequestFilter
	var describes []*kmsg.DescribeACLsRequest
	for _, typeNames := range []struct {
		t     kmsg.ACLResourceType
		names []string
		any   bool
	}{
		{kmsg.ACLResourceTypeTopic, a.topics, a.parsed.anyTopic},
		{kmsg.ACLResourceTypeGroup, a.groups, a.parsed.anyGroup},
		{kmsg.ACLResourceTypeCluster, a.parsed.clusters, a.parsed.anyCluster},
		{kmsg.ACLResourceTypeTransactionalId, a.txnIDs, a.parsed.anyTxn},
	} {
		for _, name := range typeNames.names {
			for _, op := range a.parsed.operations {
				for _, perm := range []struct {
					principals   []string
					anyPrincipal bool
					hosts        []string
					anyHost      bool
					permType     kmsg.ACLPermissionType
				}{
					{
						a.allowPrincipals,
						a.parsed.anyAllowPrincipal,
						a.allowHosts,
						a.parsed.anyAllowHost,
						kmsg.ACLPermissionTypeAllow,
					},
					{
						a.denyPrincipals,
						a.parsed.anyDenyPrincipal,
						a.denyHosts,
						a.parsed.anyDenyHost,
						kmsg.ACLPermissionTypeDeny,
					},
				} {
					for _, principal := range perm.principals {
						for _, host := range perm.hosts {
							deletion := kmsg.NewDeleteACLsRequestFilter()
							describe := kmsg.NewPtrDescribeACLsRequest()

							deletion.ResourceType = typeNames.t
							describe.ResourceType = typeNames.t

							if !typeNames.any {
								deletion.ResourceName = kmsg.StringPtr(name)
								describe.ResourceName = kmsg.StringPtr(name)
							}

							deletion.ResourcePatternType = a.parsed.pattern
							describe.ResourcePatternType = a.parsed.pattern

							deletion.Operation = op
							describe.Operation = op

							if !perm.anyPrincipal {
								deletion.Principal = kmsg.StringPtr(principal)
								describe.Principal = kmsg.StringPtr(principal)
							}

							if !perm.anyHost {
								deletion.Host = kmsg.StringPtr(host)
								describe.Host = kmsg.StringPtr(host)
							}

							deletion.PermissionType = perm.permType
							describe.PermissionType = perm.permType

							deletions = append(deletions, deletion)
							describes = append(describes, describe)
						}
					}
				}
			}
		}
	}
	types.Sort(deletions)
	types.Sort(describes)
	return deletions, describes, nil
}
