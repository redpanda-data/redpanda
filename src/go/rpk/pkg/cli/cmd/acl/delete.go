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
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/ui"
	"golang.org/x/sync/errgroup"
)

func NewDeleteACLsCommand(
	admin func() (sarama.ClusterAdmin, error),
) *cobra.Command {
	var (
		resource        string
		resourceName    string
		namePattern     string
		operations      []string
		allowPrincipals []string
		allowHosts      []string
		denyPrincipals  []string
		denyHosts       []string
	)
	command := &cobra.Command{
		Use:          "delete",
		Short:        "Delete ACLs",
		SilenceUsage: true,
		Args: func(_ *cobra.Command, _ []string) error {
			if len(allowPrincipals) == 0 &&
				len(denyPrincipals) == 0 {
				return fmt.Errorf(
					"at least one of --%s or --%s must be set",
					allowPrincipalFlag,
					denyPrincipalFlag,
				)
			}
			err := checkSupported(
				supportedResources(),
				resource,
				resourceFlag,
			)
			if err != nil {
				return err
			}
			if namePattern != "" {
				err = checkSupported(
					supportedPatterns(),
					namePattern,
					namePatternFlag,
				)
				if err != nil {
					return err
				}
			}
			return nil
		},
		RunE: func(ccmd *cobra.Command, args []string) error {
			adm, err := admin()
			if err != nil {
				return err
			}

			if resource == "cluster" {
				resourceName = defaultClusterResourceName
			}

			return executeDeleteACLs(
				adm,
				resource,
				resourceName,
				namePattern,
				operations,
				allowPrincipals,
				allowHosts,
				denyPrincipals,
				denyHosts,
			)
		},
	}

	command.Flags().StringVar(
		&resource,
		resourceFlag,
		"",
		fmt.Sprintf(
			"The target resource for the ACLs that will be deleted. Supported"+
				" values: %s.",
			strings.Join(supportedResources(), ", "),
		),
	)
	command.MarkFlagRequired(resourceFlag)
	command.Flags().StringVar(
		&resourceName,
		resourceNameFlag,
		"",
		"The name of the target resource for the ACL that will be deleted.",
	)
	command.Flags().StringVar(
		&namePattern,
		namePatternFlag,
		"literal",
		fmt.Sprintf(
			"The name pattern type to be used when matching the"+
				" resource names. Supported values: %s.",
			strings.Join(supportedPatterns(), ", "),
		),
	)
	command.Flags().StringSliceVar(
		&operations,
		operationFlag,
		[]string{},
		fmt.Sprintf(
			"Operation that the ACLs that will be deleted allow or deny. Can be"+
				" passed many times. Supported values: %s.",
			strings.Join(supportedOperations(), ", "),
		),
	)
	command.Flags().StringSliceVar(
		&allowPrincipals,
		allowPrincipalFlag,
		[]string{},
		"Principal to which the ACLs that will be deleted grant"+
			" permission. Can be passed many times.",
	)
	command.Flags().StringSliceVar(
		&allowHosts,
		allowHostFlag,
		[]string{},
		"Host from which access will be granted. Can be passed many times.",
	)
	command.Flags().StringSliceVar(
		&denyPrincipals,
		denyPrincipalFlag,
		[]string{},
		"Principal to which that the ACLs that will be deleted deny"+
			" permission. Can be passed many times.",
	)
	command.Flags().StringSliceVar(
		&denyHosts,
		denyHostFlag,
		[]string{},
		"Host from which access will be denied. Can be passed many times.",
	)
	return command
}

func executeDeleteACLs(
	adm sarama.ClusterAdmin,
	resource, resourceName, namePattern string,
	operations, allowPrincipals, allowHosts, denyPrincipals, denyHosts []string,
) error {
	var resType sarama.AclResourceType
	err := resType.UnmarshalText([]byte(resource))
	if err != nil {
		return err
	}
	var pat sarama.AclResourcePatternType
	err = pat.UnmarshalText([]byte(namePattern))
	if err != nil {
		return err
	}

	filters, err := calculateDeleteACLFilters(
		resource,
		resourceName,
		namePattern,
		operations,
		allowPrincipals,
		denyPrincipals,
		allowHosts,
		denyHosts,
	)
	if err != nil {
		return err
	}
	grp, _ := errgroup.WithContext(context.Background())
	aclsChan := make(chan []sarama.MatchingAcl, len(filters))
	for _, filter := range filters {
		grp.Go(deleteACLs(adm, *filter, aclsChan))
	}

	err = grp.Wait()

	close(aclsChan)

	acls := []sarama.MatchingAcl{}
	for matchingACLs := range aclsChan {
		acls = append(acls, matchingACLs...)
	}

	sort.Slice(acls, func(i, j int) bool {
		a := acls[i]
		b := acls[j]
		return strings.Compare(a.ResourceName, b.ResourceName) < 0
	})

	printMatchingACLs(acls)

	return err
}

func deleteACLs(
	adm sarama.ClusterAdmin,
	filter sarama.AclFilter,
	acls chan<- []sarama.MatchingAcl,
) func() error {
	return func() error {
		resType, _ := filter.ResourceType.MarshalText()
		pat, _ := filter.ResourcePatternTypeFilter.MarshalText()
		op, _ := filter.Operation.MarshalText()
		perm, _ := filter.PermissionType.MarshalText()
		filterInfo := fmt.Sprintf("resource type: '%s',"+
			" name pattern type '%s', operation '%s', permission '%s'",
			string(resType),
			string(pat),
			string(op),
			string(perm),
		)
		as, err := adm.DeleteACL(filter, false)
		if err == nil {
			acls <- as
			return nil
		}

		msg := fmt.Sprintf(
			"couldn't list ACLs with filter %s",
			filterInfo,
		)
		if filter.ResourceName != nil {
			msg = fmt.Sprintf("%s, resource name %s", msg, *filter.ResourceName)
		}
		if filter.Principal != nil {
			msg = fmt.Sprintf("%s, principal %s", msg, *filter.Principal)
		}

		if filter.Host != nil {
			msg = fmt.Sprintf("%s, host %s", msg, *filter.Host)
		}
		return errors.Wrap(err, msg)
	}
}

// Returns the set of ACL filters for deleting ACLs
func calculateDeleteACLFilters(
	resource, resourceName, resourceNamePattern string,
	operations, allowPrincipals, denyPrincipals, allowHosts, denyHosts []string,
) ([]*sarama.AclFilter, error) {
	filters := []*sarama.AclFilter{}
	allowPrincipalsFilter := []*string{}
	denyPrincipalsFilter := []*string{}
	var allowHostsFilter []*string
	var denyHostsFilter []*string
	var ops []sarama.AclOperation
	var err error

	for _, ppal := range allowPrincipals {
		p := ppal
		allowPrincipalsFilter = append(allowPrincipalsFilter, &p)
	}

	for _, ppal := range denyPrincipals {
		p := ppal
		denyPrincipalsFilter = append(denyPrincipalsFilter, &p)
	}

	if len(allowHosts) == 0 {
		allowHostsFilter = []*string{nil}
	} else {
		for _, host := range allowHosts {
			h := host
			allowHostsFilter = append(allowHostsFilter, &h)
		}
	}

	if len(denyHosts) == 0 {
		denyHostsFilter = []*string{nil}
	} else {
		for _, host := range denyHosts {
			h := host
			denyHostsFilter = append(denyHostsFilter, &h)
		}
	}

	if len(operations) == 0 {
		ops = []sarama.AclOperation{sarama.AclOperationAny}
	} else {
		ops, err = parseOperations(operations)
		if err != nil {
			return nil, err
		}
	}

	var resName *string = nil
	if resourceName != "" {
		resName = &resourceName
	}
	resType := sarama.AclResourceAny
	if resource != "" {
		err = resType.UnmarshalText([]byte(resource))
		if err != nil {
			return nil, err
		}
	}
	pat := sarama.AclPatternAny
	if resourceNamePattern != "" {
		err = pat.UnmarshalText([]byte(resourceNamePattern))
		if err != nil {
			return nil, err
		}
	}

	for _, op := range ops {
		for _, ap := range allowPrincipalsFilter {
			for _, ah := range allowHostsFilter {
				filter := &sarama.AclFilter{
					ResourceType:              resType,
					ResourceName:              resName,
					ResourcePatternTypeFilter: pat,
					Principal:                 ap,
					Host:                      ah,
					Operation:                 op,
					PermissionType:            sarama.AclPermissionAllow,
				}
				filters = append(filters, filter)
			}
		}

		for _, dp := range denyPrincipalsFilter {
			for _, dh := range denyHostsFilter {
				filter := &sarama.AclFilter{
					ResourceType:              resType,
					ResourceName:              resName,
					ResourcePatternTypeFilter: pat,
					Principal:                 dp,
					Host:                      dh,
					Operation:                 op,
					PermissionType:            sarama.AclPermissionDeny,
				}
				filters = append(filters, filter)
			}
		}
	}
	return filters, nil
}

func printMatchingACLs(matchingACLs []sarama.MatchingAcl) {
	if len(matchingACLs) == 0 {
		log.Info("No ACLs matched the given filters, so none were deleted.")
		return
	}
	spacer := []string{""}
	t := ui.NewRpkTable(log.StandardLogger().Out)
	t.SetColWidth(80)
	t.SetAutoWrapText(true)
	t.SetHeader([]string{"Deleted", "Principal", "Host", "Operation", "Permission Type", "Resource Type", "Resource Name", "Error Message"})
	t.Append(spacer)

	for _, acl := range matchingACLs {
		resType, _ := acl.ResourceType.MarshalText()
		op, _ := acl.Operation.MarshalText()
		perm, _ := acl.PermissionType.MarshalText()
		deleted := "yes"
		errMsg := "None"
		if acl.ErrMsg != nil {
			deleted = "no"
			errMsg = *acl.ErrMsg
		}
		t.Append([]string{
			deleted,
			acl.Principal,
			acl.Host,
			string(op),
			string(perm),
			string(resType),
			acl.ResourceName,
			errMsg,
		})
	}
	t.Append(spacer)
	t.Render()
}
