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

	"github.com/redpanda-data/common-go/rpsr"
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
	registryFlag       = "registry-global"
	subjectFlag        = "registry-subject"
	subsystemFlag      = "subsystem"

	kafkaCluster = "kafka-cluster"

	rolePrefix = "RedpandaRole:"

	groupPrefix = "Group:"
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
		Principal           string `json:"principal"`
		Host                string `json:"host"`
		ResourceType        string `json:"resource_type"`
		ResourceName        string `json:"resource_name"`
		ResourcePatternType string `json:"resource_pattern_type"`
		Operation           string `json:"operation"`
		Permission          string `json:"permission"`
	}
	aclWithMessage struct {
		Principal           string `json:"principal"`
		Host                string `json:"host"`
		ResourceType        string `json:"resource_type"`
		ResourceName        string `json:"resource_name"`
		ResourcePatternType string `json:"resource_pattern_type"`
		Operation           string `json:"operation"`
		Permission          string `json:"permission"`
		Message             string `json:"message"`
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

	// schema registry flags
	subjects []string
	registry bool

	// create & delete & list flags, to be parsed
	resourcePatternType string
	operations          []string

	kParsed  kafkaParsed
	srParsed schemaRegistryParsed
}

// kafkaParsed contains the results of flags that need parsing for Kafka ACLs.
type kafkaParsed struct {
	operations []kmsg.ACLOperation
	pattern    kmsg.ACLResourcePatternType
}

// schemaRegistryParsed contains the result of flags that need parsing for
// SR ACLs.
type schemaRegistryParsed struct {
	operations []rpsr.Operation
	pattern    rpsr.PatternType
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

func (a *acls) parseKafkaCommons() error {
	for _, op := range a.operations {
		parsed, err := kmsg.ParseACLOperation(op)
		if err != nil {
			return fmt.Errorf("invalid kafka operation %q", op)
		}
		a.kParsed.operations = append(a.kParsed.operations, parsed)
	}
	if a.resourcePatternType == "" {
		a.resourcePatternType = "literal"
	}
	pattern, err := kmsg.ParseACLResourcePatternType(a.resourcePatternType)
	if err != nil {
		return fmt.Errorf("invalid kafka resource pattern type %q", a.resourcePatternType)
	}
	a.kParsed.pattern = pattern
	return nil
}

func (a *acls) parseSRCommons() error {
	for _, op := range a.operations {
		parsed, err := rpsr.ParseOperation(op)
		if err != nil {
			return fmt.Errorf("invalid schema registry operation %q", op)
		}
		a.srParsed.operations = append(a.srParsed.operations, parsed)
	}
	if a.resourcePatternType == "" {
		a.resourcePatternType = "literal"
	}
	pattern, err := rpsr.ParsePatternType(a.resourcePatternType)
	if err != nil {
		return fmt.Errorf("invalid schema registry resource pattern type %q", a.resourcePatternType)
	}
	a.srParsed.pattern = pattern
	return nil
}

func (a *acls) createCreations() (*kadm.ACLBuilder, []rpsr.ACL, error) {
	kC, err := a.createKafkaACLCreation()
	if err != nil {
		return nil, nil, fmt.Errorf("unable to create kafka ACLs: %v", err)
	}
	if a.hasSR() {
		srC, err := a.createSRACLCreation()
		if err != nil {
			return nil, nil, fmt.Errorf("unable to create schema registry ACLs: %v", err)
		}
		return kC, srC, nil
	}
	return kC, nil, nil
}

func (a *acls) createKafkaACLCreation() (*kadm.ACLBuilder, error) {
	if err := a.backcompat(false); err != nil {
		return nil, err
	}
	if err := a.parseKafkaCommons(); err != nil {
		return nil, err
	}

	PrefixRole(a.allowRoles)
	PrefixRole(a.denyRoles)

	a.allowPrincipals = append(a.allowPrincipals, a.allowRoles...)
	a.denyPrincipals = append(a.denyPrincipals, a.denyRoles...)

	// Using empty lists / non-Maybe functions when building create ACLs is
	// fine, since creation does not opt in to "any" when things are empty.
	b := kadm.NewACLs().
		ResourcePatternType(a.kParsed.pattern).
		MaybeOperations(a.kParsed.operations...). // avoid defaulting to "any" if none are provided
		Topics(a.topics...).
		Groups(a.groups...).
		MaybeClusters(a.cluster). // avoid opting in to all clusters by default
		TransactionalIDs(a.txnIDs...).
		Allow(a.allowPrincipals...).
		AllowHosts(a.allowHosts...).
		Deny(a.denyPrincipals...).
		DenyHosts(a.denyHosts...)

	b.PrefixUserExcept(rolePrefix, groupPrefix) // add "User:" prefix to everything if needed

	return b, b.ValidateCreate()
}

func (a *acls) createSRACLCreation() ([]rpsr.ACL, error) {
	if err := a.parseSRCommons(); err != nil {
		return nil, err
	}

	PrefixRole(a.allowRoles)
	PrefixRole(a.denyRoles)

	a.allowPrincipals = append(a.allowPrincipals, a.allowRoles...)
	a.denyPrincipals = append(a.denyPrincipals, a.denyRoles...)

	b := rpsr.NewACLBuilder().
		Pattern(a.srParsed.pattern).
		Operations(a.srParsed.operations...).
		Subjects(a.subjects...).
		MaybeRegistry(a.registry).
		AllowPrincipals(a.allowPrincipals...).
		DenyPrincipals(a.denyPrincipals...)

	// Kafka ACL creation currently defaults to any ('*') if we have a
	// principal; we match the behavior.
	if b.HasAllowedPrincipals() {
		b.AllowHostsOrAll(a.allowHosts...)
	} else {
		b.AllowHosts(a.allowHosts...)
	}
	if b.HasDeniedPrincipals() {
		b.DenyHostsOrAll(a.denyHosts...)
	} else {
		b.DenyHosts(a.denyHosts...)
	}

	b.PrefixUserExcept(rolePrefix, groupPrefix)

	return b.ValidateAndBuildCreate()
}

func (a *acls) createDeletionsAndDescribes(list, isKafka, isSR bool) (kBuilder *kadm.ACLBuilder, srACLs []rpsr.ACL, err error) {
	// If is none, then is all (i.e: no flags specified, or just common flags).
	if !isKafka && !isSR {
		isKafka, isSR = true, true
	}
	if isKafka {
		kBuilder, err = a.createKafkaDeletionsAndDescribes(list)
		if err != nil {
			return nil, nil, fmt.Errorf("unable to create kafka ACLs: %v", err)
		}
	}
	if isSR {
		srACLs, err = a.createSRDeletionsAndDescribes()
		if err != nil {
			return nil, nil, fmt.Errorf("unable to create schema registry ACLs: %v", err)
		}
	}
	return kBuilder, srACLs, nil
}

func (a *acls) createKafkaDeletionsAndDescribes(list bool) (*kadm.ACLBuilder, error) {
	if err := a.backcompat(list); err != nil {
		return nil, err
	}
	if err := a.parseKafkaCommons(); err != nil {
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
		ResourcePatternType(a.kParsed.pattern).
		Operations(a.kParsed.operations...).
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

	b.PrefixUserExcept(rolePrefix, groupPrefix) // add "User:" prefix to everything if needed

	return b, b.ValidateFilter()
}

func (a *acls) createSRDeletionsAndDescribes() ([]rpsr.ACL, error) {
	if err := a.parseSRCommons(); err != nil {
		return nil, err
	}

	PrefixRole(a.allowRoles)
	PrefixRole(a.denyRoles)

	a.allowPrincipals = append(a.allowPrincipals, a.allowRoles...)
	a.denyPrincipals = append(a.denyPrincipals, a.denyRoles...)

	b := rpsr.NewACLBuilder().
		PatternOrAny(a.srParsed.pattern).
		OperationsOrAny(a.srParsed.operations...).
		Subjects(a.subjects...).
		MaybeRegistry(a.registry).
		AllowPrincipals(a.allowPrincipals...).
		DenyPrincipals(a.denyPrincipals...).
		AllowHosts(a.allowHosts...).
		DenyHosts(a.denyHosts...)

	// User & Hosts: when unspecified, we default to everything.
	if !b.HasPrincipals() {
		b.AllowPrincipalsOrAny()
		b.DenyPrincipalsOrAny()
	}
	if !b.HasHosts() {
		b.AllowHostsOrAny()
		b.DenyHostsOrAny()
	}
	// Resources: if no resources are specified, we use all resources.
	if !b.HasResources() {
		b.AnyResources()
	}

	b.PrefixUserExcept(rolePrefix, groupPrefix)

	return b.BuildFilter(), nil
}

func (a *acls) hasSR() bool {
	if a.registry || len(a.subjects) > 0 {
		return true
	}
	return false
}

// kafkaOrSRFilters returns whether Kafka or Schema Registry flags were set.
// If checkSubsystem is true and --subsystem is set, it ensures only compatible
// flags are used for the selected subsystem(s).
func kafkaOrSRFilters(cmd *cobra.Command, checkSubsystem bool) (isKafka, isSR bool, err error) {
	kFlags := []string{topicFlag, groupFlag, clusterFlag, txnIDFlag}
	srFlags := []string{registryFlag, subjectFlag}

	subsystemChanged := cmd.Flags().Changed(subsystemFlag)
	var subsystems []string
	if subsystemChanged {
		subsystems, err = cmd.Flags().GetStringSlice(subsystemFlag)
		if err != nil {
			return false, false, err
		}
	}

	// If we're checking the subsystem and it changed:
	if checkSubsystem && subsystemChanged {
		onlyKafka := false
		onlyRegistry := false

		for _, sub := range subsystems {
			switch strings.ToLower(sub) {
			case "kafka":
				onlyKafka = true
			case "registry":
				onlyRegistry = true
			default:
				return false, false, fmt.Errorf("unsupported subsystem %q: only 'kafka' and 'registry' are supported", sub)
			}
		}

		if onlyKafka && !onlyRegistry {
			for _, flag := range srFlags {
				if cmd.Flags().Changed(flag) {
					return false, false, fmt.Errorf("specified kafka subsystem only, but the Schema Registry flag %q was specified", flag)
				}
			}
			return true, false, nil
		}

		if onlyRegistry && !onlyKafka {
			for _, flag := range kFlags {
				if cmd.Flags().Changed(flag) {
					return false, false, fmt.Errorf("specified Schema Registry subsystem only, but the Kafka flag %q was specified", flag)
				}
			}
			return false, true, nil
		}
		// If both kafka and registry are included, fall through to flag-based detection.
	}

	for _, flag := range kFlags {
		if cmd.Flags().Changed(flag) {
			isKafka = true
			break
		}
	}
	for _, flag := range srFlags {
		if cmd.Flags().Changed(flag) {
			isSR = true
			break
		}
	}
	return isKafka, isSR, nil
}
