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
	"golang.org/x/sync/errgroup"
)

func NewCreateACLsCommand(
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
		Use:          "create",
		Short:        "Create ACLs",
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

			if resource == "cluster" {
				resourceName = defaultClusterResourceName
			}

			err := checkSupported(
				supportedResources(),
				resource,
				resourceFlag,
			)
			if err != nil {
				return err
			}

			return checkSupported(
				supportedPatterns(),
				namePattern,
				namePatternFlag,
			)
		},
		RunE: func(ccmd *cobra.Command, args []string) error {
			adm, err := admin()
			if err != nil {
				return err
			}

			return executeCreateACLs(
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
			"The target resource for the ACL. Supported values: %s.",
			strings.Join(supportedResources(), ", "),
		),
	)
	command.MarkFlagRequired(resourceFlag)
	command.Flags().StringVar(
		&resourceName,
		resourceNameFlag,
		"",
		"The name of the target resource for the ACL.",
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
			"Operation that the principal will be allowed or denied. Can be"+
				" passed many times. Supported values: %s.",
			strings.Join(supportedOperations(), ", "),
		),
	)
	command.Flags().StringSliceVar(
		&allowPrincipals,
		allowPrincipalFlag,
		[]string{},
		"Principal to which permissions will be granted. Can be passed many times.",
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
		"Principal to which permissions will be denied. Can be passed many times.",
	)
	command.Flags().StringSliceVar(
		&denyHosts,
		denyHostFlag,
		[]string{},
		"Host from which access will be denied. Can be passed many times.",
	)
	return command
}

func executeCreateACLs(
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

	res := sarama.Resource{
		ResourceType:        resType,
		ResourceName:        resourceName,
		ResourcePatternType: pat,
	}

	acls, err := calculateACLs(
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
	for _, acl := range acls {
		a := acl
		grp.Go(createACL(adm, res, a))
	}

	return grp.Wait()
}

// Returns the set of ACLs
func calculateACLs(
	operations, allowPrincipals, denyPrincipals, allowHosts, denyHosts []string,
) ([]*sarama.Acl, error) {
	var err error
	acls := []*sarama.Acl{}
	if len(allowPrincipals) == 0 && len(denyPrincipals) == 0 {
		return nil, errors.New("empty allow & deny principals list")
	}
	if len(allowPrincipals) != 0 && len(allowHosts) == 0 {
		allowHosts = []string{"*"}
	}
	if len(denyPrincipals) != 0 && len(denyHosts) == 0 {
		denyHosts = []string{"*"}
	}

	ops := []sarama.AclOperation{sarama.AclOperationAll}
	if len(operations) != 0 {
		ops, err = parseOperations(operations)
		if err != nil {
			return nil, err
		}
	}

	for _, op := range ops {
		for _, ap := range allowPrincipals {
			for _, ah := range allowHosts {
				acl := &sarama.Acl{
					Principal:      ap,
					Host:           ah,
					Operation:      op,
					PermissionType: sarama.AclPermissionAllow,
				}
				acls = append(acls, acl)
			}
		}

		for _, dp := range denyPrincipals {
			for _, dh := range denyHosts {
				acl := &sarama.Acl{
					Principal:      dp,
					Host:           dh,
					Operation:      op,
					PermissionType: sarama.AclPermissionDeny,
				}
				acls = append(acls, acl)
			}
		}
	}
	return acls, nil
}

func createACL(
	adm sarama.ClusterAdmin, res sarama.Resource, acl *sarama.Acl,
) func() error {
	return func() error {
		op, _ := acl.Operation.MarshalText()
		perm, _ := acl.PermissionType.MarshalText()
		msg := fmt.Sprintf(
			"ACL for principal '%s' with host '%s', operation '%s' and"+
				" permission '%s'",
			acl.Principal,
			acl.Host,
			string(op),
			string(perm),
		)
		err := adm.CreateACL(res, *acl)
		if err == nil {
			log.Infof("Created %s", msg)
			return nil
		}
		return errors.Wrap(err, fmt.Sprintf("Couldn't create %s", msg))
	}
}
