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
	"fmt"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var helpOperations bool
	cmd := &cobra.Command{
		Use:   "acl",
		Short: "Manage ACLs and SASL users",
		Long:  helpACLs,
		Args:  cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, _ []string) {
			if helpOperations {
				fmt.Print(helpACLOperations)
				return
			}
			cmd.Help()
		},
	}

	cmd.Flags().BoolVar(&helpOperations, "help-operations", false, "Print more help about ACL operations")

	cmd.AddCommand(newCreateCommand(fs, p))
	cmd.AddCommand(newDeleteCommand(fs, p))
	cmd.AddCommand(newListCommand(fs, p))
	return cmd
}

const helpACLs = `Manage ACLs and SASL users.

This command space creates, lists, and deletes ACLs, as well as creates SASL
users. The help text below is specific to ACLs. To learn about SASL users,
check the help text under the "user" command.

When using SASL, ACLs allow or deny you access to certain requests. The
"create", "delete", and "list" commands help you manage your ACLs.

An ACL is made up of five components:

  * a principal (the user) or role
  * a host, which the principal (or role) is allowed or denied requests from
  * what resource to access (such as topic name, group ID)
  * the operation (such as read, write)
  * the permission: whether to allow or deny the above

ACL commands work on a multiplicative basis. If creating, specifying two
principals and two permissions creates four ACLs: both permissions for the
first principal, as well as both permissions for the second principal. Adding
two resources further doubles the ACLs created.

It is recommended to be as specific as possible when granting ACLs. Granting
more ACLs than necessary per principal may inadvertently allow clients to do
things they should not, such as deleting topics or joining the wrong consumer
group.

PRINCIPALS

All ACLs require a principal or a role. A principal is composed of a user
and a type. Within Redpanda, only the "User" type is supported. Having
prefixes for new types ensures that potential future authorizers can add
authorization using other types, such as "Group".

When you create a user, you need to add ACLs for it before it can be used. You
can create / delete / list ACLs for that user with either "User:bar" or "bar"
in the --allow-principal and --deny-principal flags. This command will add the
"User:" prefix for you if it is missing. The wildcard '*' matches any user.
Creating an ACL with user '*' grants or denies the permission for all users.

ROLES

You can bind ACLs to a role. A role has only one part: the name. In contrast
to principals, there is no need to supply the type. If a type-like prefix is
present, it is treated as text rather than as principal type information.

When you create a role, you must bind or associate ACLs to it before it can be
used. You can create / delete / list ACLs for that role with "<name>" in the
--allow-role and --deny-role flags. Note that the wildcard role name '*' is not
permitted here. For example 'rpk security acl create --allow-role '*' ...' will 
produce an error.

HOSTS

Hosts can be seen as an extension of the principal, and effectively gate where
the principal can connect from. When creating ACLs, unless otherwise specified,
the default host is the wildcard '*' which allows or denies the principal from
all hosts (where allow & deny are based on whether --allow-principal or
--deny-principal is used). If specifying hosts, you must pair the --allow-host
flag with the --allow-principal flag, and the --deny-host flag with the
--deny-principal flag.

RESOURCES

A resource is what an ACL allows or denies access to. There are four resources
within Redpanda: topics, groups, the cluster itself, and transactional IDs.
Names for each of these resources can be specified with their respective flags.

Resources combine with the operation that is allowed or denied on that
resource. The next section describes which operations are required for which
requests, and further fleshes out the concept of a resource.

By default, resources are specified on an exact name match (a "literal" match).
The --resource-pattern-type flag can be used to specify that a resource name is
"prefixed", meaning to allow anything with the given prefix. A literal name of
"foo" will match only the topic "foo", while the prefixed name of "foo-" will
match both "foo-bar" and "foo-baz". The special wildcard resource name '*'
matches any name of the given resource type (--topic '*' matches all topics).

OPERATIONS

Pairing with resources, operations are the actions that are allowed or denied.
Redpanda has the following operations:

    ALL                 Allows all operations below.
    READ                Allows reading a given resource.
    WRITE               Allows writing to a given resource.
    CREATE              Allows creating a given resource.
    DELETE              Allows deleting a given resource.
    ALTER               Allows altering non-configurations.
    DESCRIBE            Allows querying non-configurations.
    DESCRIBE_CONFIGS    Allows describing configurations.
    ALTER_CONFIGS       Allows altering configurations.

Check --help-operations to see which operations are required for which
requests. In flag form to set up a general producing/consuming client, you can
invoke 'rpk security acl create' three times with the following (including your
--allow-principal):

    --operation write,read,describe --topic [topics]
    --operation describe,read --group [group.id]
    --operation describe,write --transactional-id [transactional.id]

PERMISSIONS

A client can be allowed access or denied access. By default, all permissions
are denied. You only need to specifically deny a permission if you allow a wide
set of permissions and then want to deny a specific permission in that set.
You could allow all operations, and then specifically deny writing to topics.

MANAGEMENT

Creating ACLs works on a specific ACL basis, but listing and deleting ACLs
works on filters. Filters allow matching many ACLs to be printed listed and
deleted at once. Because this can be risky for deleting, the delete command
prompts for confirmation by default. More details and examples for creating,
listing, and deleting can be seen in each of the commands.

Using SASL requires setting "enable_sasl: true" in the redpanda section of your
redpanda.yaml. User management is a separate, simpler concept that is
described in the user command.
`

const helpACLOperations = `Brokers support many operations for many resources:

    ALL                 Allows all operations below.
    READ                Allows reading a given resource.
    WRITE               Allows writing to a given resource.
    CREATE              Allows creating a given resource.
    DELETE              Allows deleting a given resource.
    ALTER               Allows altering non-configurations.
    DESCRIBE            Allows querying non-configurations.
    DESCRIBE_CONFIGS    Allows describing configurations.
    ALTER_CONFIGS       Allows altering configurations.

The following lists the operations needed for each individual client request,
where "on RESOURCE" corresponds to the resource flag, and "for xyz" corresponds
to the resource name(s) in the request:

PRODUCING/CONSUMING

    Produce      WRITE on TOPIC for topics
                 WRITE on TRANSACTIONAL_ID for the transaction.id

    Fetch        READ on TOPIC for topics

    ListOffsets  DESCRIBE on TOPIC for topics

    Metadata     DESCRIBE on TOPIC for topics
                 CREATE on CLUSTER for kafka-cluster (if automatically creating topics)
                 or, CREATE on TOPIC for topics (if automatically creating topics)

    InitProducerID  IDEMPOTENT_WRITE on CLUSTER
                    or, WRITE on any TOPIC
                    or, WRITE on TRANSACTIONAL_ID for transactional.id (if using transactions)

    OffsetForLeaderEpoch  DESCRIBE on TOPIC for topics

GROUP CONSUMING

    FindCoordinator  DESCRIBE on GROUP for group
                     DESCRIBE on TRANSACTIONAL_ID for transactional.id (transactions)

    OffsetCommit     READ on GROUP for groups
                     READ on TOPIC for topics

    OffsetFetch      DESCRIBE on GROUP for groups
                     DESCRIBE on TOPIC for topics

    OffsetDelete     DELETE on GROUP for groups
                     READ on TOPIC for topics

    JoinGroup        READ on GROUP for group
    Heartbeat        READ on GROUP for group
    LeaveGroup       READ on GROUP for group
    SyncGroup        READ on GROUP for group

TRANSACTIONS (including FindCoordinator above)

    AddPartitionsToTxn  WRITE on TRANSACTIONAL_ID for transactional.id
                        WRITE on TOPIC for topics

    AddOffsetsToTxn     WRITE on TRANSACTIONAL_ID for transactional.id
                        READ on GROUP for group

    EndTxn              WRITE on TRANSACTIONAL_ID for transactional.id

    TxnOffsetCommit     WRITE on TRANSACTIONAL_ID for transactional.id
                        READ on GROUP for group
                        READ on TOPIC for topics

ADMIN

    CreateTopics      CREATE on TOPIC for topics
                      or, CREATE on CLUSTER for kafka-cluster
                      DESCRIBE_CONFIGS on TOPIC for topics, for returning topic configs on create

    CreatePartitions  ALTER on TOPIC for topics

    DeleteTopics      DELETE on TOPIC for topics
                      DESCRIBE on TOPIC for topics, if deleting by topic id (in addition to prior ACL)

    DeleteRecords     DELETE on TOPIC for topics

    DescribeGroup     DESCRIBE on GROUP for groups

    ListGroups        DESCRIBE on GROUP for groups
                      or, DESCRIBE on CLUSTER for kafka-cluster

    DeleteGroups      DELETE on GROUP for groups

    CreateACLs        ALTER on CLUSTER for kafka-cluster
    DeleteACLs        ALTER on CLUSTER for kafka-cluster
    DescribeACLs      DESCRIBE on CLUSTER for kafka-cluster

    DescribeConfigs   DESCRIBE_CONFIGS on CLUSTER for kafka-cluster (broker describing)
                      DESCRIBE_CONFIGS on TOPIC for topics (topic describing)

    AlterConfigs      ALTER_CONFIGS on CLUSTER for kafka-cluster (broker altering)
    (or Incremental)  ALTER_CONFIGS on TOPIC for topics (topic altering)

    AlterPartitionAssignments   ALTER on CLUSTER for kafka-cluster
    ListPartitionReassignments  DESCRIBE on CLUSTER for kafka-cluster

    AlterReplicaLogDirs    ALTER on CLUSTER for kafka-cluster
    DescribeLogDirs        DESCRIBE on CLUSTER for kafka-cluster

    AlterClientQuotas      ALTER on CLUSTER for kafka-cluster
    DescribeClientQuotas   DESCRIBE_CONFIGS on CLUSTER for kafka-cluster
     
    AlterUserScramCreds    ALTER on CLUSTER for kafka-cluster
    DescribeUserScramCreds DESCRIBE_CONFIGS on CLUSTER for kafka-cluster

    DescribeProducers      READ on TOPIC for topics
    DescribeTransactions   DESCRIBE on TRANSACTIONAL_ID for transactional.id
                           DESCRIBE on TOPIC for topics
    ListTransactions       DESCRIBE on TRANSACTIONAL_ID for transactional.id
`
