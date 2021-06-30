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
	"strings"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/ui"
	"golang.org/x/sync/errgroup"
)

func NewListACLsCommand(
	admin func() (sarama.ClusterAdmin, error),
) *cobra.Command {
	var (
		resource     string
		resourceName string
		namePattern  string
		operations   []string
		permissions  []string
		principals   []string
		hosts        []string
	)
	command := &cobra.Command{
		Use:          "list",
		Aliases:      []string{"ls"},
		Short:        "List ACLs",
		SilenceUsage: true,
		RunE: func(ccmd *cobra.Command, args []string) error {
			adm, err := admin()
			if err != nil {
				return err
			}

			return executeListACLs(
				adm,
				resource,
				resourceName,
				namePattern,
				operations,
				permissions,
				principals,
				hosts,
			)
		},
	}

	command.Flags().StringVar(
		&resource,
		resourceFlag,
		"",
		fmt.Sprintf(
			"Resource type to filter by. Supported values: %s.",
			strings.Join(supportedResources(), ", "),
		),
	)
	command.Flags().StringVar(
		&resourceName,
		resourceNameFlag,
		"",
		"The name of the resource of the given type.",
	)
	command.Flags().StringVar(
		&namePattern,
		namePatternFlag,
		"",
		fmt.Sprintf(
			"The name pattern type to be used when matching affected"+
				" resources. Supported values: %s.",
			strings.Join(supportedPatterns(), ", "),
		),
	)
	command.Flags().StringSliceVar(
		&operations,
		operationFlag,
		[]string{},
		fmt.Sprintf(
			"Operation to filter by. Can be passed multiple times to filter by many operations. Supported"+
				" values: %s.",
			strings.Join(supportedOperations(), ", "),
		),
	)
	command.Flags().StringSliceVar(
		&permissions,
		"permission",
		[]string{},
		fmt.Sprintf(
			"Permission to filter by. Can be"+
				" passed many times to filter by multiple permission types. Supported values: %s.",
			strings.Join(supportedPermissions(), ", "),
		),
	)
	command.Flags().StringSliceVar(
		&principals,
		"principal",
		[]string{},
		"Principal to filter by. Can be passed multiple times to filter by many principals.",
	)
	command.Flags().StringSliceVar(
		&hosts,
		"host",
		[]string{},
		"Host to filter by. Can be passed multiple times to filter by many hosts.",
	)
	return command
}

func executeListACLs(
	adm sarama.ClusterAdmin,
	resource, resourceName, namePattern string,
	operations, permissions, principals, hosts []string,
) error {
	filters, err := calculateListACLFilters(
		resource,
		resourceName,
		namePattern,
		operations,
		permissions,
		principals,
		hosts,
	)
	if err != nil {
		return err
	}

	grp, _ := errgroup.WithContext(context.Background())
	aclsChan := make(chan []sarama.ResourceAcls, len(filters))
	for _, filter := range filters {
		grp.Go(listACLs(adm, *filter, aclsChan))
	}

	err = grp.Wait()

	close(aclsChan)

	acls := []sarama.ResourceAcls{}
	for resACLs := range aclsChan {
		acls = append(acls, resACLs...)
	}

	printResourceACLs(acls)

	return err
}

func listACLs(
	adm sarama.ClusterAdmin,
	filter sarama.AclFilter,
	acls chan<- []sarama.ResourceAcls,
) func() error {
	return func() error {
		filterInfo := fmt.Sprintf("resource type: '%s',"+
			" name pattern type '%s', operation '%s', permission '%s'",
			filter.ResourceType.String(),
			filter.ResourcePatternTypeFilter.String(),
			filter.Operation.String(),
			filter.PermissionType.String(),
		)
		log.Debugf("Listing ACLs with filter %s", filterInfo)
		as, err := adm.ListAcls(filter)
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

// Returns the set of ACL filters
func calculateListACLFilters(
	resource, resourceName, resourceNamePattern string,
	operations, permissions, principals, hosts []string,
) ([]*sarama.AclFilter, error) {
	filters := []*sarama.AclFilter{}
	var principalsFilter []*string
	var hostsFilter []*string
	var ops []sarama.AclOperation
	var perms []sarama.AclPermissionType
	var err error

	if len(principals) == 0 {
		principalsFilter = []*string{nil}
	} else {
		for _, ppal := range principals {
			p := ppal
			principalsFilter = append(principalsFilter, &p)
		}
	}

	if len(hosts) == 0 {
		hostsFilter = []*string{nil}
	} else {
		for _, host := range hosts {
			h := host
			hostsFilter = append(hostsFilter, &h)
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

	if len(permissions) == 0 {
		perms = []sarama.AclPermissionType{sarama.AclPermissionAny}
	} else {
		perms, err = parsePermissions(permissions)
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

	for _, p := range perms {
		for _, op := range ops {
			for _, ap := range principalsFilter {
				for _, ah := range hostsFilter {
					filter := &sarama.AclFilter{
						ResourceType:              resType,
						ResourceName:              resName,
						ResourcePatternTypeFilter: pat,
						Principal:                 ap,
						Host:                      ah,
						Operation:                 op,
						PermissionType:            p,
					}
					filters = append(filters, filter)
				}
			}
		}
	}
	return filters, nil
}

func printResourceACLs(resACLs []sarama.ResourceAcls) {
	if len(resACLs) == 0 {
		log.Info("No ACLs found for the given filters.")
		return
	}
	spacer := []string{""}
	t := ui.NewRpkTable(log.StandardLogger().Out)
	t.SetColWidth(80)
	t.SetAutoWrapText(true)
	t.SetHeader([]string{"Principal", "Host", "Operation", "Permission Type", "Resource Type", "Resource Name", "Resource Pattern Type"})
	t.Append(spacer)
	for _, resACL := range resACLs {
		resType := resACL.Resource.ResourceType.String()
		resName := resACL.ResourceName
		resNamePattern := resACL.Resource.ResourcePatternType.String()
		for _, acl := range resACL.Acls {
			t.Append([]string{
				acl.Principal,
				acl.Host,
				acl.Operation.String(),
				acl.PermissionType.String(),
				resType,
				resName,
				resNamePattern,
			})
		}
	}
	t.Append(spacer)
	t.Render()
}
