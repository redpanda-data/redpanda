---
title: RPK commands
order: 7
---

# RPK commands

## What is RPK?

`rpk` is a CLI utility tool that helps you manage Redpanda.

With it you can interact with Redpanda in all sorts of ways. You can manage your ACLs, tune for performance, manage topics, and more. 

It also has a version as a Docker container named `rpk container`.

Here are all of the rpk commands.


## rpk

 rpk is the Redpanda CLI & toolbox.

```bash 
Usage:
  rpk [command]
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>-h, --help</td><td>-</td><td>      help for rpk</td></tr><tr><td>-v, --verbose</td><td>-</td><td>   Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk acl

 Manage ACLs and SASL users.

This command space creates, lists, and deletes ACLs, as well as creates SASL
users. The help text below is specific to ACLs. To learn about SASL users,
check the help text under the "user" command.

When using SASL, ACLs allow or deny you access to certain requests. The
"create", "delete", and "list" commands help you manage your ACLs.

An ACL is made up of five components:

  * a principal (the user)
  * a host the principal is allowed or denied requests from
  * what resource to access (topic name, group ID, ...)
  * the operation (read, write, ...)
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

All ACLs require a principal. A principal is composed of two parts: the type
and the name. Within Redpanda, only one type is supported, "User". The reason
for the prefix is that a potential future authorizer may add support for
authorizing by Group or anything else.

When you create a user, you need to add ACLs for it before it can be used. You
can create / delete / list ACLs for that user with either "User:bar" or "bar"
in the --allow-principal and --deny-principal flags. This command will add the
"User:" prefix for you if it is missing. The wildcard '*' matches any user.
Creating an ACL with user '*' grants or denies the permission for all users.

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
invoke 'rpk acl create' three times with the following (including your
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

```bash 
Usage:
  rpk acl [flags]
  rpk acl [command]
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>--admin-api-tls-cert</td><td>string</td><td>          The certificate to be used for TLS authentication with the Admin API.</td></tr><tr><td>--admin-api-tls-enabled</td><td>-</td><td>             Enable TLS for the Admin API (not necessary if specifying custom certs).</td></tr><tr><td>--admin-api-tls-key</td><td>string</td><td>           The certificate key to be used for TLS authentication with the Admin API.</td></tr><tr><td>--admin-api-tls-truststore</td><td>string</td><td>    The truststore to be used for TLS communication with the Admin API.</td></tr><tr><td>--brokers</td><td>strings</td><td>                    Comma-separated list of broker ip:port pairs (e.g. --brokers '192.168.78.34:9092,192.168.78.35:9092,192.179.23.54:9092' ). Alternatively, you may set the REDPANDA_BROKERS environment variable with the comma-separated list of broker addresses.</td></tr><tr><td>--config</td><td>string</td><td>                      Redpanda config file, if not set the file will be searched for in the default locations</td></tr><tr><td>-h, --help</td><td>-</td><td>                              help for acl</td></tr><tr><td>--help-operations</td><td>-</td><td>                   Print more help about ACL operations.</td></tr><tr><td>--password</td><td>string</td><td>                    SASL password to be used for authentication.</td></tr><tr><td>--sasl-mechanism</td><td>string</td><td>              The authentication mechanism to use. Supported values: SCRAM-SHA-256, SCRAM-SHA-512.</td></tr><tr><td>--tls-cert</td><td>string</td><td>                    The certificate to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-enabled</td><td>-</td><td>                       Enable TLS for the Kafka API (not necessary if specifying custom certs).</td></tr><tr><td>--tls-key</td><td>string</td><td>                     The certificate key to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-truststore</td><td>string</td><td>              The truststore to be used for TLS communication with the broker.</td></tr><tr><td>--user</td><td>string</td><td>                        SASL user to be used for authentication.</td></tr><tr><td>-v, --verbose</td><td>-</td><td>   Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk acl create

 Create ACLs.

See the 'rpk acl' help text for a full write up on ACLs. Following the
multiplying effect of combining flags, the create command works on a
straightforward basis: every ACL combination is a created ACL.

As mentioned in the 'rpk acl' help text, if no host is specified, an allowed
principal is allowed access from all hosts. The wildcard principal '*' allows
all principals. At least one principal, one host, one resource, and one
operation is required to create a single ACL.

Allow all permissions to user bar on topic "foo" and group "g":
    --allow-principal bar --operation all --topic foo --group g
Allow read permissions to all users on topics biz and baz:
    --allow-principal '*' --operation read --topic biz,baz
Allow write permissions to user buzz to transactional id "txn":
    --allow-principal User:buzz --operation write --transactional-id txn

```bash 
Usage:
  rpk acl create [flags]
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>--allow-host</td><td>strings</td><td>              hosts from which access will be granted (repeatable)</td></tr><tr><td>--allow-principal</td><td>strings</td><td>         principals for which these permissions will be granted (repeatable)</td></tr><tr><td>--cluster</td><td>-</td><td>                        whether to grant ACLs to the cluster</td></tr><tr><td>--deny-host</td><td>strings</td><td>               hosts from from access will be denied (repeatable)</td></tr><tr><td>--deny-principal</td><td>strings</td><td>          principal for which these permissions will be denied (repeatable)</td></tr><tr><td>--group</td><td>strings</td><td>                   group to grant ACLs for (repeatable)</td></tr><tr><td>-h, --help</td><td>-</td><td>                           help for create</td></tr><tr><td>--operation</td><td>strings</td><td>               operation to grant (repeatable)</td></tr><tr><td>--topic</td><td>strings</td><td>                   topic to grant ACLs for (repeatable)</td></tr><tr><td>--transactional-id</td><td>strings</td><td>        transactional IDs to grant ACLs for (repeatable)</td></tr><tr><td>--admin-api-tls-cert</td><td>string</td><td>          The certificate to be used for TLS authentication with the Admin API.</td></tr><tr><td>--admin-api-tls-enabled</td><td>-</td><td>             Enable TLS for the Admin API (not necessary if specifying custom certs).</td></tr><tr><td>--admin-api-tls-key</td><td>string</td><td>           The certificate key to be used for TLS authentication with the Admin API.</td></tr><tr><td>--admin-api-tls-truststore</td><td>string</td><td>    The truststore to be used for TLS communication with the Admin API.</td></tr><tr><td>--brokers</td><td>strings</td><td>                    Comma-separated list of broker ip:port pairs (e.g. --brokers '192.168.78.34:9092,192.168.78.35:9092,192.179.23.54:9092' ). Alternatively, you may set the REDPANDA_BROKERS environment variable with the comma-separated list of broker addresses.</td></tr><tr><td>--config</td><td>string</td><td>                      Redpanda config file, if not set the file will be searched for in the default locations</td></tr><tr><td>--password</td><td>string</td><td>                    SASL password to be used for authentication.</td></tr><tr><td>--sasl-mechanism</td><td>string</td><td>              The authentication mechanism to use. Supported values: SCRAM-SHA-256, SCRAM-SHA-512.</td></tr><tr><td>--tls-cert</td><td>string</td><td>                    The certificate to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-enabled</td><td>-</td><td>                       Enable TLS for the Kafka API (not necessary if specifying custom certs).</td></tr><tr><td>--tls-key</td><td>string</td><td>                     The certificate key to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-truststore</td><td>string</td><td>              The truststore to be used for TLS communication with the broker.</td></tr><tr><td>--user</td><td>string</td><td>                        SASL user to be used for authentication.</td></tr><tr><td>-v, --verbose</td><td>-</td><td>                           Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk acl delete

 Delete ACLs.

See the 'rpk acl' help text for a full write up on ACLs. Delete flags work in a
similar multiplying effect as creating ACLs, but delete is more advanced:
deletion works on a filter basis. Any unspecified flag defaults to matching
everything (all operations, or all allowed principals, etc). To ensure that you
do not accidentally delete more than you intend, this command prints everything
that matches your input filters and prompts for a confirmation before the
delete request is issued. Anything matching more than 10 ACLs doubly confirms.

As mentioned, not specifying flags matches everything. If no resources are
specified, all resources are matched. If no operations are specified, all
operations are matched. You can also opt in to matching everything with "any":
--operation any matches any operation.

The --resource-pattern-type, defaulting to "any", configures how to filter
resource names:
  * "any" returns exact name matches of either prefixed or literal pattern type
  * "match" returns wildcard matches, prefix patterns that match your input, and literal matches
  * "prefix" returns prefix patterns that match your input (prefix "fo" matches "foo")
  * "literal" returns exact name matches

```bash 
Usage:
  rpk acl delete [flags]
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>--allow-host</td><td>strings</td><td>              allowed host ACLs to remove (repeatable)</td></tr><tr><td>--allow-principal</td><td>strings</td><td>         allowed principal ACLs to remove (repeatable)</td></tr><tr><td>--cluster</td><td>-</td><td>                        whether to remove ACLs to the cluster</td></tr><tr><td>--deny-host</td><td>strings</td><td>               denied host ACLs to remove (repeatable)</td></tr><tr><td>--deny-principal</td><td>strings</td><td>          denied principal ACLs to remove (repeatable)</td></tr><tr><td>-d, --dry</td><td>-</td><td>                            dry run: validate what would be deleted</td></tr><tr><td>--group</td><td>strings</td><td>                   group to remove ACLs for (repeatable)</td></tr><tr><td>-h, --help</td><td>-</td><td>                           help for delete</td></tr><tr><td>--no-confirm</td><td>-</td><td>                     disable confirmation prompt</td></tr><tr><td>--operation</td><td>strings</td><td>               operation to remove (repeatable)</td></tr><tr><td>-f, --print-filters</td><td>-</td><td>                  print the filters that were requested (failed filters are always printed)</td></tr><tr><td>--topic</td><td>strings</td><td>                   topic to remove ACLs for (repeatable)</td></tr><tr><td>--transactional-id</td><td>strings</td><td>        transactional IDs to remove ACLs for (repeatable)</td></tr><tr><td>--admin-api-tls-cert</td><td>string</td><td>          The certificate to be used for TLS authentication with the Admin API.</td></tr><tr><td>--admin-api-tls-enabled</td><td>-</td><td>             Enable TLS for the Admin API (not necessary if specifying custom certs).</td></tr><tr><td>--admin-api-tls-key</td><td>string</td><td>           The certificate key to be used for TLS authentication with the Admin API.</td></tr><tr><td>--admin-api-tls-truststore</td><td>string</td><td>    The truststore to be used for TLS communication with the Admin API.</td></tr><tr><td>--brokers</td><td>strings</td><td>                    Comma-separated list of broker ip:port pairs (e.g. --brokers '192.168.78.34:9092,192.168.78.35:9092,192.179.23.54:9092' ). Alternatively, you may set the REDPANDA_BROKERS environment variable with the comma-separated list of broker addresses.</td></tr><tr><td>--config</td><td>string</td><td>                      Redpanda config file, if not set the file will be searched for in the default locations</td></tr><tr><td>--password</td><td>string</td><td>                    SASL password to be used for authentication.</td></tr><tr><td>--sasl-mechanism</td><td>string</td><td>              The authentication mechanism to use. Supported values: SCRAM-SHA-256, SCRAM-SHA-512.</td></tr><tr><td>--tls-cert</td><td>string</td><td>                    The certificate to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-enabled</td><td>-</td><td>                       Enable TLS for the Kafka API (not necessary if specifying custom certs).</td></tr><tr><td>--tls-key</td><td>string</td><td>                     The certificate key to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-truststore</td><td>string</td><td>              The truststore to be used for TLS communication with the broker.</td></tr><tr><td>--user</td><td>string</td><td>                        SASL user to be used for authentication.</td></tr><tr><td>-v, --verbose</td><td>-</td><td>                           Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk acl list

 List ACLs.

See the 'rpk acl' help text for a full write up on ACLs. List flags work in a
similar multiplying effect as creating ACLs, but list is more advanced:
listing works on a filter basis. Any unspecified flag defaults to matching
everything (all operations, or all allowed principals, etc).

As mentioned, not specifying flags matches everything. If no resources are
specified, all resources are matched. If no operations are specified, all
operations are matched. You can also opt in to matching everything with "any":
--operation any matches any operation.

The --resource-pattern-type, defaulting to "any", configures how to filter
resource names:
  * "any" returns exact name matches of either prefixed or literal pattern type
  * "match" returns wildcard matches, prefix patterns that match your input, and literal matches
  * "prefix" returns prefix patterns that match your input (prefix "fo" matches "foo")
  * "literal" returns exact name matches

```bash 
Usage:
  rpk acl list [flags]

Aliases:
  list, ls, describe
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>--allow-host</td><td>strings</td><td>              allowed host ACLs to match (repeatable)</td></tr><tr><td>--allow-principal</td><td>strings</td><td>         allowed principal ACLs to match (repeatable)</td></tr><tr><td>--cluster</td><td>-</td><td>                        whether to match ACLs to the cluster</td></tr><tr><td>--deny-host</td><td>strings</td><td>               denied host ACLs to match (repeatable)</td></tr><tr><td>--deny-principal</td><td>strings</td><td>          denied principal ACLs to match (repeatable)</td></tr><tr><td>--group</td><td>strings</td><td>                   group to match ACLs for (repeatable)</td></tr><tr><td>-h, --help</td><td>-</td><td>                           help for list</td></tr><tr><td>--operation</td><td>strings</td><td>               operation to match (repeatable)</td></tr><tr><td>-f, --print-filters</td><td>-</td><td>                  print the filters that were requested (failed filters are always printed)</td></tr><tr><td>--topic</td><td>strings</td><td>                   topic to match ACLs for (repeatable)</td></tr><tr><td>--transactional-id</td><td>strings</td><td>        transactional IDs to match ACLs for (repeatable)</td></tr><tr><td>--admin-api-tls-cert</td><td>string</td><td>          The certificate to be used for TLS authentication with the Admin API.</td></tr><tr><td>--admin-api-tls-enabled</td><td>-</td><td>             Enable TLS for the Admin API (not necessary if specifying custom certs).</td></tr><tr><td>--admin-api-tls-key</td><td>string</td><td>           The certificate key to be used for TLS authentication with the Admin API.</td></tr><tr><td>--admin-api-tls-truststore</td><td>string</td><td>    The truststore to be used for TLS communication with the Admin API.</td></tr><tr><td>--brokers</td><td>strings</td><td>                    Comma-separated list of broker ip:port pairs (e.g. --brokers '192.168.78.34:9092,192.168.78.35:9092,192.179.23.54:9092' ). Alternatively, you may set the REDPANDA_BROKERS environment variable with the comma-separated list of broker addresses.</td></tr><tr><td>--config</td><td>string</td><td>                      Redpanda config file, if not set the file will be searched for in the default locations</td></tr><tr><td>--password</td><td>string</td><td>                    SASL password to be used for authentication.</td></tr><tr><td>--sasl-mechanism</td><td>string</td><td>              The authentication mechanism to use. Supported values: SCRAM-SHA-256, SCRAM-SHA-512.</td></tr><tr><td>--tls-cert</td><td>string</td><td>                    The certificate to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-enabled</td><td>-</td><td>                       Enable TLS for the Kafka API (not necessary if specifying custom certs).</td></tr><tr><td>--tls-key</td><td>string</td><td>                     The certificate key to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-truststore</td><td>string</td><td>              The truststore to be used for TLS communication with the broker.</td></tr><tr><td>--user</td><td>string</td><td>                        SASL user to be used for authentication.</td></tr><tr><td>-v, --verbose</td><td>-</td><td>                           Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk acl user

 Manage SASL users.

If SASL is enabled, a SASL user is what you use to talk to Redpanda, and ACLs
control what your user has access to. See 'rpk acl --help' for more information
about ACLs, and 'rpk acl user create --help' for more information about
creating SASL users. Using SASL requires setting "enable_sasl: true" in the
redpanda section of your redpanda.yaml.

```bash 
Usage:
  rpk acl user [command]
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>--api-urls</td><td>strings</td><td>    The comma-separated list of Admin API addresses (|IP|:|port|). You must specify one for each node.</td></tr><tr><td>-h, --help</td><td>-</td><td>               help for user</td></tr><tr><td>--admin-api-tls-cert</td><td>string</td><td>          The certificate to be used for TLS authentication with the Admin API.</td></tr><tr><td>--admin-api-tls-enabled</td><td>-</td><td>             Enable TLS for the Admin API (not necessary if specifying custom certs).</td></tr><tr><td>--admin-api-tls-key</td><td>string</td><td>           The certificate key to be used for TLS authentication with the Admin API.</td></tr><tr><td>--admin-api-tls-truststore</td><td>string</td><td>    The truststore to be used for TLS communication with the Admin API.</td></tr><tr><td>--brokers</td><td>strings</td><td>                    Comma-separated list of broker ip:port pairs (e.g. --brokers '192.168.78.34:9092,192.168.78.35:9092,192.179.23.54:9092' ). Alternatively, you may set the REDPANDA_BROKERS environment variable with the comma-separated list of broker addresses.</td></tr><tr><td>--config</td><td>string</td><td>                      Redpanda config file, if not set the file will be searched for in the default locations</td></tr><tr><td>--password</td><td>string</td><td>                    SASL password to be used for authentication.</td></tr><tr><td>--sasl-mechanism</td><td>string</td><td>              The authentication mechanism to use. Supported values: SCRAM-SHA-256, SCRAM-SHA-512.</td></tr><tr><td>--tls-cert</td><td>string</td><td>                    The certificate to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-enabled</td><td>-</td><td>                       Enable TLS for the Kafka API (not necessary if specifying custom certs).</td></tr><tr><td>--tls-key</td><td>string</td><td>                     The certificate key to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-truststore</td><td>string</td><td>              The truststore to be used for TLS communication with the broker.</td></tr><tr><td>--user</td><td>string</td><td>                        SASL user to be used for authentication.</td></tr><tr><td>-v, --verbose</td><td>-</td><td>                           Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk acl user create

 Create a SASL user.

This command creates a single SASL user with the given password, optionally
with a custom "mechanism". SASL consists of three parts: a username, a
password, and a mechanism. The mechanism determines which authentication flow
the client will use for this user/pass.

Redpanda currently supports two mechanisms: SCRAM-SHA-256, the default, and
SCRAM-SHA-512, which is the same flow but uses sha512 rather than sha256.

Using SASL requires setting "enable_sasl: true" in the redpanda section of your
redpanda.yaml. Before a created SASL account can be used, you must also create
ACLs to grant the account access to certain resources in your cluster. See the
acl help text for more info.

```bash 
Usage:
  rpk acl user create [USER} -p [PASS] [flags]
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>-h, --help</td><td>-</td><td>               help for create</td></tr><tr><td>--admin-api-tls-cert</td><td>string</td><td>          The certificate to be used for TLS authentication with the Admin API.</td></tr><tr><td>--admin-api-tls-enabled</td><td>-</td><td>             Enable TLS for the Admin API (not necessary if specifying custom certs).</td></tr><tr><td>--admin-api-tls-key</td><td>string</td><td>           The certificate key to be used for TLS authentication with the Admin API.</td></tr><tr><td>--admin-api-tls-truststore</td><td>string</td><td>    The truststore to be used for TLS communication with the Admin API.</td></tr><tr><td>--api-urls</td><td>strings</td><td>                   The comma-separated list of Admin API addresses (|IP|:|port|). You must specify one for each node.</td></tr><tr><td>--brokers</td><td>strings</td><td>                    Comma-separated list of broker ip:port pairs (e.g. --brokers '192.168.78.34:9092,192.168.78.35:9092,192.179.23.54:9092' ). Alternatively, you may set the REDPANDA_BROKERS environment variable with the comma-separated list of broker addresses.</td></tr><tr><td>--config</td><td>string</td><td>                      Redpanda config file, if not set the file will be searched for in the default locations</td></tr><tr><td>--password</td><td>string</td><td>                    SASL password to be used for authentication.</td></tr><tr><td>--sasl-mechanism</td><td>string</td><td>              The authentication mechanism to use. Supported values: SCRAM-SHA-256, SCRAM-SHA-512.</td></tr><tr><td>--tls-cert</td><td>string</td><td>                    The certificate to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-enabled</td><td>-</td><td>                       Enable TLS for the Kafka API (not necessary if specifying custom certs).</td></tr><tr><td>--tls-key</td><td>string</td><td>                     The certificate key to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-truststore</td><td>string</td><td>              The truststore to be used for TLS communication with the broker.</td></tr><tr><td>--user</td><td>string</td><td>                        SASL user to be used for authentication.</td></tr><tr><td>-v, --verbose</td><td>-</td><td>                           Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk acl user delete

 Delete a SASL user.

This command deletes the specified SASL account from Redpanda. This does not
delete any ACLs that may exist for this user.

```bash 
Usage:
  rpk acl user delete [USER] [flags]
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>-h, --help</td><td>-</td><td>   help for delete</td></tr><tr><td>--admin-api-tls-cert</td><td>string</td><td>          The certificate to be used for TLS authentication with the Admin API.</td></tr><tr><td>--admin-api-tls-enabled</td><td>-</td><td>             Enable TLS for the Admin API (not necessary if specifying custom certs).</td></tr><tr><td>--admin-api-tls-key</td><td>string</td><td>           The certificate key to be used for TLS authentication with the Admin API.</td></tr><tr><td>--admin-api-tls-truststore</td><td>string</td><td>    The truststore to be used for TLS communication with the Admin API.</td></tr><tr><td>--api-urls</td><td>strings</td><td>                   The comma-separated list of Admin API addresses (|IP|:|port|). You must specify one for each node.</td></tr><tr><td>--brokers</td><td>strings</td><td>                    Comma-separated list of broker ip:port pairs (e.g. --brokers '192.168.78.34:9092,192.168.78.35:9092,192.179.23.54:9092' ). Alternatively, you may set the REDPANDA_BROKERS environment variable with the comma-separated list of broker addresses.</td></tr><tr><td>--config</td><td>string</td><td>                      Redpanda config file, if not set the file will be searched for in the default locations</td></tr><tr><td>--password</td><td>string</td><td>                    SASL password to be used for authentication.</td></tr><tr><td>--sasl-mechanism</td><td>string</td><td>              The authentication mechanism to use. Supported values: SCRAM-SHA-256, SCRAM-SHA-512.</td></tr><tr><td>--tls-cert</td><td>string</td><td>                    The certificate to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-enabled</td><td>-</td><td>                       Enable TLS for the Kafka API (not necessary if specifying custom certs).</td></tr><tr><td>--tls-key</td><td>string</td><td>                     The certificate key to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-truststore</td><td>string</td><td>              The truststore to be used for TLS communication with the broker.</td></tr><tr><td>--user</td><td>string</td><td>                        SASL user to be used for authentication.</td></tr><tr><td>-v, --verbose</td><td>-</td><td>                           Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk acl user list

 List SASL users.

```bash 
Usage:
  rpk acl user list [flags]

Aliases:
  list, ls
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>-h, --help</td><td>-</td><td>   help for list</td></tr><tr><td>--admin-api-tls-cert</td><td>string</td><td>          The certificate to be used for TLS authentication with the Admin API.</td></tr><tr><td>--admin-api-tls-enabled</td><td>-</td><td>             Enable TLS for the Admin API (not necessary if specifying custom certs).</td></tr><tr><td>--admin-api-tls-key</td><td>string</td><td>           The certificate key to be used for TLS authentication with the Admin API.</td></tr><tr><td>--admin-api-tls-truststore</td><td>string</td><td>    The truststore to be used for TLS communication with the Admin API.</td></tr><tr><td>--api-urls</td><td>strings</td><td>                   The comma-separated list of Admin API addresses (|IP|:|port|). You must specify one for each node.</td></tr><tr><td>--brokers</td><td>strings</td><td>                    Comma-separated list of broker ip:port pairs (e.g. --brokers '192.168.78.34:9092,192.168.78.35:9092,192.179.23.54:9092' ). Alternatively, you may set the REDPANDA_BROKERS environment variable with the comma-separated list of broker addresses.</td></tr><tr><td>--config</td><td>string</td><td>                      Redpanda config file, if not set the file will be searched for in the default locations</td></tr><tr><td>--password</td><td>string</td><td>                    SASL password to be used for authentication.</td></tr><tr><td>--sasl-mechanism</td><td>string</td><td>              The authentication mechanism to use. Supported values: SCRAM-SHA-256, SCRAM-SHA-512.</td></tr><tr><td>--tls-cert</td><td>string</td><td>                    The certificate to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-enabled</td><td>-</td><td>                       Enable TLS for the Kafka API (not necessary if specifying custom certs).</td></tr><tr><td>--tls-key</td><td>string</td><td>                     The certificate key to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-truststore</td><td>string</td><td>              The truststore to be used for TLS communication with the broker.</td></tr><tr><td>--user</td><td>string</td><td>                        SASL user to be used for authentication.</td></tr><tr><td>-v, --verbose</td><td>-</td><td>                           Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk cluster

 Interact with a Redpanda cluster.

```bash 
Usage:
  rpk cluster [command]
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>--brokers</td><td>strings</td><td>          Comma-separated list of broker ip:port pairs (e.g. --brokers '192.168.78.34:9092,192.168.78.35:9092,192.179.23.54:9092' ). Alternatively, you may set the REDPANDA_BROKERS environment variable with the comma-separated list of broker addresses.</td></tr><tr><td>--config</td><td>string</td><td>            Redpanda config file, if not set the file will be searched for in the default locations</td></tr><tr><td>-h, --help</td><td>-</td><td>                    help for cluster</td></tr><tr><td>--password</td><td>string</td><td>          SASL password to be used for authentication.</td></tr><tr><td>--sasl-mechanism</td><td>string</td><td>    The authentication mechanism to use. Supported values: SCRAM-SHA-256, SCRAM-SHA-512.</td></tr><tr><td>--tls-cert</td><td>string</td><td>          The certificate to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-enabled</td><td>-</td><td>             Enable TLS for the Kafka API (not necessary if specifying custom certs).</td></tr><tr><td>--tls-key</td><td>string</td><td>           The certificate key to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-truststore</td><td>string</td><td>    The truststore to be used for TLS communication with the broker.</td></tr><tr><td>--user</td><td>string</td><td>              SASL user to be used for authentication.</td></tr><tr><td>-v, --verbose</td><td>-</td><td>   Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk cluster metadata

 Request broker metadata.

The Kafka protocol's metadata contains information about brokers, topics, and
the cluster as a whole.

This command only runs if specific sections of metadata are requested. There
are currently three sections: the cluster, the list of brokers, and the topics.
If no section is specified, this defaults to printing all sections.

If the topic section is requested, all topics are requested by default unless
some are manually specified as arguments. Expanded per-partition information
can be printed with the -d flag, and internal topics can be printed with the -i
flag.

In the broker section, the controller node is suffixed with *.

```bash 
Usage:
  rpk cluster metadata [flags]

Aliases:
  metadata, status, info
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>-h, --help</td><td>-</td><td>                    help for metadata</td></tr><tr><td>-b, --print-brokers</td><td>-</td><td>           print brokers section</td></tr><tr><td>-c, --print-cluster</td><td>-</td><td>           print cluster section</td></tr><tr><td>-d, --print-detailed-topics</td><td>-</td><td>   print per-partition information for topics (implies -t)</td></tr><tr><td>-i, --print-internal-topics</td><td>-</td><td>   print internal topics (if all topics requested, implies -t)</td></tr><tr><td>-t, --print-topics</td><td>-</td><td>            print topics section (implied if any topics are specified)</td></tr><tr><td>--brokers</td><td>strings</td><td>          Comma-separated list of broker ip:port pairs (e.g. --brokers '192.168.78.34:9092,192.168.78.35:9092,192.179.23.54:9092' ). Alternatively, you may set the REDPANDA_BROKERS environment variable with the comma-separated list of broker addresses.</td></tr><tr><td>--config</td><td>string</td><td>            Redpanda config file, if not set the file will be searched for in the default locations</td></tr><tr><td>--password</td><td>string</td><td>          SASL password to be used for authentication.</td></tr><tr><td>--sasl-mechanism</td><td>string</td><td>    The authentication mechanism to use. Supported values: SCRAM-SHA-256, SCRAM-SHA-512.</td></tr><tr><td>--tls-cert</td><td>string</td><td>          The certificate to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-enabled</td><td>-</td><td>             Enable TLS for the Kafka API (not necessary if specifying custom certs).</td></tr><tr><td>--tls-key</td><td>string</td><td>           The certificate key to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-truststore</td><td>string</td><td>    The truststore to be used for TLS communication with the broker.</td></tr><tr><td>--user</td><td>string</td><td>              SASL user to be used for authentication.</td></tr><tr><td>-v, --verbose</td><td>-</td><td>                 Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk container

 Manage a local container cluster.

```bash 
Usage:
  rpk container [command]
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>-h, --help</td><td>-</td><td>   help for container</td></tr><tr><td>-v, --verbose</td><td>-</td><td>   Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk container purge

 Stop and remove an existing local container cluster's data.

```bash 
Usage:
  rpk container purge [flags]
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>-h, --help</td><td>-</td><td>   help for purge</td></tr><tr><td>-v, --verbose</td><td>-</td><td>   Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk container start

 Start a local container cluster.

```bash 
Usage:
  rpk container start [flags]
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>-h, --help</td><td>-</td><td>           help for start</td></tr><tr><td>-n, --nodes</td><td>-</td><td> uint     The number of nodes to start (default 1)</td></tr><tr><td>--retries</td><td>-</td><td> uint   The amount of times to check for the cluster before considering it unstable and exiting. (default 10)</td></tr><tr><td>-v, --verbose</td><td>-</td><td>   Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk container stop

 Stop an existing local container cluster.

```bash 
Usage:
  rpk container stop [flags]
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>-h, --help</td><td>-</td><td>   help for stop</td></tr><tr><td>-v, --verbose</td><td>-</td><td>   Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk debug

 Debug the local Redpanda process.

```bash 
Usage:
  rpk debug [command]
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>-h, --help</td><td>-</td><td>   help for debug</td></tr><tr><td>-v, --verbose</td><td>-</td><td>   Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk debug bundle

 'rpk debug bundle' collects environment data that can help debug and diagnose
issues with a redpanda cluster, a broker, or the machine it's running on. It
then bundles the collected data into a zip file.

The following are the data sources that are bundled in the compressed file:

 - Kafka metadata: Broker configs, topic configs, start/committed/end offsets,
   groups, group commits.

 - Data directory structure: A file describing the data directory's contents.

 - redpanda configuration: The redpanda configuration file (redpanda.yaml;
   SASL credentials are stripped).

 - /proc/cpuinfo: CPU information like make, core count, cache, frequency.

 - /proc/interrupts: IRQ distribution across CPU cores.

 - Resource usage data: CPU usage percentage, free memory available for the
   redpanda process.

 - Clock drift: The ntp clock delta (using pool.ntp.org as a reference) & round
   trip time.

 - Kernel logs: The kernel logs ring buffer (syslog).

 - Broker metrics: The local broker's Prometheus metrics, fetched through its
   admin API.

 - DNS: The DNS info as reported by 'dig', using the hosts in
   /etc/resolv.conf.

 - Disk usage: The disk usage for the data directory, as output by 'du'.

 - redpanda logs: The redpanda logs written to journald. If --logs-since or
   --logs-until are passed, then only the logs within the resulting time frame
   will be included.

 - Socket info: The active sockets data output by 'ss'.

 - Running process info: As reported by 'top'.

 - Virtual memory stats: As reported by 'vmstat'.

 - Network config: As reported by 'ip addr'.

 - lspci: List the PCI buses and the devices connected to them.

 - dmidecode: The DMI table contents. Only included if this command is run
   as root.

```bash 
Usage:
  rpk debug bundle [flags]
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>--admin-api-tls-cert</td><td>string</td><td>          The certificate to be used for TLS authentication with the Admin API.</td></tr><tr><td>--admin-api-tls-enabled</td><td>-</td><td>             Enable TLS for the Admin API (not necessary if specifying custom certs).</td></tr><tr><td>--admin-api-tls-key</td><td>string</td><td>           The certificate key to be used for TLS authentication with the Admin API.</td></tr><tr><td>--admin-api-tls-truststore</td><td>string</td><td>    The truststore to be used for TLS communication with the Admin API.</td></tr><tr><td>--admin-url</td><td>string</td><td>                   The address to the broker's admin API. Defaults to the one in the config file.</td></tr><tr><td>--brokers</td><td>strings</td><td>                    Comma-separated list of broker ip:port pairs (e.g. --brokers '192.168.78.34:9092,192.168.78.35:9092,192.179.23.54:9092' ). Alternatively, you may set the REDPANDA_BROKERS environment variable with the comma-separated list of broker addresses.</td></tr><tr><td>--config</td><td>string</td><td>                      Redpanda config file, if not set the file will be searched for in the default locations</td></tr><tr><td>-h, --help</td><td>-</td><td>                              help for bundle</td></tr><tr><td>--logs-since</td><td>string</td><td>                  Include log entries on or newer than the specified date. (journalctl date format, e.g. YYYY-MM-DD)</td></tr><tr><td>--logs-until</td><td>string</td><td>                  Include log entries on or older than the specified date. (journalctl date format, e.g. YYYY-MM-DD)</td></tr><tr><td>--password</td><td>string</td><td>                    SASL password to be used for authentication.</td></tr><tr><td>--sasl-mechanism</td><td>string</td><td>              The authentication mechanism to use. Supported values: SCRAM-SHA-256, SCRAM-SHA-512.</td></tr><tr><td>--timeout</td><td>duration</td><td>                   How long to wait for child commands to execute (e.g. '30s', '1.5m') (default 10s)</td></tr><tr><td>--tls-cert</td><td>string</td><td>                    The certificate to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-enabled</td><td>-</td><td>                       Enable TLS for the Kafka API (not necessary if specifying custom certs).</td></tr><tr><td>--tls-key</td><td>string</td><td>                     The certificate key to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-truststore</td><td>string</td><td>              The truststore to be used for TLS communication with the broker.</td></tr><tr><td>--user</td><td>string</td><td>                        SASL user to be used for authentication.</td></tr><tr><td>-v, --verbose</td><td>-</td><td>   Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk generate

 Generate a configuration template for related services.

```bash 
Usage:
  rpk generate [command]
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>-h, --help</td><td>-</td><td>   help for generate</td></tr><tr><td>-v, --verbose</td><td>-</td><td>   Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk generate grafana-dashboard

 Generate a Grafana dashboard for redpanda metrics.

```bash 
Usage:
  rpk generate grafana-dashboard [flags]
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>--datasource</td><td>string</td><td>          The name of the Prometheus datasource as configured in your grafana instance.</td></tr><tr><td>-h, --help</td><td>-</td><td>                      help for grafana-dashboard</td></tr><tr><td>-v, --verbose</td><td>-</td><td>   Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk generate prometheus-config

 
Generate the Prometheus configuration to scrape redpanda nodes. This command's
output should be added to the 'scrape_configs' array in your Prometheus
instance's YAML config file.

If --seed-addr is passed, it will be used to discover the rest of the cluster
hosts via redpanda's Kafka API. If --node-addrs is passed, they will be used
directly. Otherwise, 'rpk generate prometheus-conf' will read the redpanda
config file and use the node IP configured there. --config may be passed to
especify an arbitrary config file.

```bash 
Usage:
  rpk generate prometheus-config [flags]
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>--config</td><td>string</td><td>         The path to the redpanda config file</td></tr><tr><td>-h, --help</td><td>-</td><td>                 help for prometheus-config</td></tr><tr><td>--node-addrs</td><td>strings</td><td>    </td></tr><tr><td>-delimited</td><td>-</td><td> list of the addresses (|host:port|) of all the redpanda nodes</td></tr><tr><td>--seed-addr</td><td>string</td><td>      The URL of a redpanda node with which to discover the rest</td></tr><tr><td>-v, --verbose</td><td>-</td><td>   Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk generate shell-completion

 
Shell completion can help autocomplete rpk commands when you press tab.

# Bash

Bash autocompletion relies on the bash-completion package. You can test if you
have this by running "type _init_completion", if you do not, you can install
the package through your package manager.

If you have bash-completion installed, and the command still fails, you likely
need to add the following line to your ~/.bashrc:

    source /usr/share/bash-completion/bash_completion

To ensure autocompletion of rpk exists in all shell sessions, add the following
to your ~/.bashrc:

    command -v rpk >/dev/null && . <(rpk generate shell-completion bash)

Alternatively, to globally enable rpk completion, you can run the following:

    rpk generate shell-completion bash > /etc/bash_completion.d/rpk

# Zsh

To enable autocompletion in any zsh session for any user, run this once:

    rpk generate shell-completion zsh > "${fpath[1]}/_rpk"

You can also place that command in your ~/.zshrc to ensure that when you update
rpk, you update autocompletion. If you initially require sudo to edit that
file, you can chmod it to be world writeable, after which you will always be
able to update it from ~/.zshrc.

If shell completion is not already enabled in your zsh environment, also
add the following to your ~/.zshrc:

    autoload -U compinit; compinit

# Fish

To enable autocompletion in any fish session, run:

    rpk generate shell-completion fish > ~/.config/fish/completions/rpk.fish

```bash 
Usage:
  rpk generate shell-completion [flags]
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>-h, --help</td><td>-</td><td>   help for shell-completion</td></tr><tr><td>-v, --verbose</td><td>-</td><td>   Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk group

 Describe, list, and delete consumer groups and manage their offsets.

Consumer groups allow you to horizontally scale consuming from topics. A
non-group consumer consumes all records from all partitions you assign it. In
contrast, consumer groups allow many consumers to coordinate and divide work.
If you have two members in a group consuming topics A and B, each with three
partitions, then both members consume three partitions. If you add another
member to the group, then each of the three members will consume two
partitions. This allows you to horizontally scale consuming of topics.

The unit of scaling is a single partition. If you add more consumers to a group
than there are are total partitions to consume, then some consumers will be
idle. More commonly, you have many more partitions than consumer group members
and each member consumes a chunk of available partitions. One scenario where
you may want more members than partitions is if you want active standby's to
take over load immediately if any consuming member dies.

How group members divide work is entirely client driven (the "partition
assignment strategy" or "balancer" depending on the client). Brokers know
nothing about how consumers are assigning partitions. A broker's role in group
consuming is to choose which member is the leader of a group, forward that
member's assignment to every other member, and ensure all members are alive
through heartbeats.

Consumers periodically commit their progress when consuming partitions. Through
these commits, you can monitor just how far behind a consumer is from the
latest messages in a partition. This is called "lag". Large lag implies that
the client is having problems, which could be from the server being too slow,
or the client being oversubscribed in the number of partitions it is consuming,
or the server being in a bad state that requires restarting or removing from
the server pool, and so on.

You can manually manage offsets for a group, which allows you to rewind or
forward commits. If you notice that a recent deploy of your consumers had a
bug, you may want to stop all members, rewind the commits to before the latest
deploy, and restart the members with a patch.

This command allows you to list all groups, describe a group (to view the
members and their lag), and manage offsets.

```bash 
Usage:
  rpk group [command]

Aliases:
  group, g
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>--brokers</td><td>strings</td><td>          Comma-separated list of broker ip:port pairs (e.g. --brokers '192.168.78.34:9092,192.168.78.35:9092,192.179.23.54:9092' ). Alternatively, you may set the REDPANDA_BROKERS environment variable with the comma-separated list of broker addresses.</td></tr><tr><td>--config</td><td>string</td><td>            Redpanda config file, if not set the file will be searched for in the default locations</td></tr><tr><td>-h, --help</td><td>-</td><td>                    help for group</td></tr><tr><td>--password</td><td>string</td><td>          SASL password to be used for authentication.</td></tr><tr><td>--sasl-mechanism</td><td>string</td><td>    The authentication mechanism to use. Supported values: SCRAM-SHA-256, SCRAM-SHA-512.</td></tr><tr><td>--tls-cert</td><td>string</td><td>          The certificate to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-enabled</td><td>-</td><td>             Enable TLS for the Kafka API (not necessary if specifying custom certs).</td></tr><tr><td>--tls-key</td><td>string</td><td>           The certificate key to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-truststore</td><td>string</td><td>    The truststore to be used for TLS communication with the broker.</td></tr><tr><td>--user</td><td>string</td><td>              SASL user to be used for authentication.</td></tr><tr><td>-v, --verbose</td><td>-</td><td>   Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk group delete

 Delete groups from brokers.

Older versions of the Kafka protocol included a retention_millis field in
offset commit requests. Group commits persisted for this retention and then
eventually expired. Once all commits for a group expired, the group would be
considered deleted.

The retention field was removed because it proved problematic for infrequently
committing consumers: the offsets could be expired for a group that was still
active. If clients use new enough versions of OffsetCommit (versions that have
removed the retention field), brokers expire offsets only when the group is
empty for offset.retention.minutes. Redpanda does not currently support that
configuration (see #2904), meaning offsets for empty groups expire only when
they are explicitly deleted.

You may want to delete groups to clean up offsets sooner than when they
automatically are cleaned up, such as when you create temporary groups for
quick investigation or testing. This command helps you do that.

```bash 
Usage:
  rpk group delete [GROUPS...] [flags]
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>-h, --help</td><td>-</td><td>   help for delete</td></tr><tr><td>--brokers</td><td>strings</td><td>          Comma-separated list of broker ip:port pairs (e.g. --brokers '192.168.78.34:9092,192.168.78.35:9092,192.179.23.54:9092' ). Alternatively, you may set the REDPANDA_BROKERS environment variable with the comma-separated list of broker addresses.</td></tr><tr><td>--config</td><td>string</td><td>            Redpanda config file, if not set the file will be searched for in the default locations</td></tr><tr><td>--password</td><td>string</td><td>          SASL password to be used for authentication.</td></tr><tr><td>--sasl-mechanism</td><td>string</td><td>    The authentication mechanism to use. Supported values: SCRAM-SHA-256, SCRAM-SHA-512.</td></tr><tr><td>--tls-cert</td><td>string</td><td>          The certificate to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-enabled</td><td>-</td><td>             Enable TLS for the Kafka API (not necessary if specifying custom certs).</td></tr><tr><td>--tls-key</td><td>string</td><td>           The certificate key to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-truststore</td><td>string</td><td>    The truststore to be used for TLS communication with the broker.</td></tr><tr><td>--user</td><td>string</td><td>              SASL user to be used for authentication.</td></tr><tr><td>-v, --verbose</td><td>-</td><td>                 Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk group describe

 Describe group offset status & lag.

This command describes group members, calculates their lag, and prints detailed
information about the members.

```bash 
Usage:
  rpk group describe [GROUPS...] [flags]
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>-h, --help</td><td>-</td><td>   help for describe</td></tr><tr><td>--brokers</td><td>strings</td><td>          Comma-separated list of broker ip:port pairs (e.g. --brokers '192.168.78.34:9092,192.168.78.35:9092,192.179.23.54:9092' ). Alternatively, you may set the REDPANDA_BROKERS environment variable with the comma-separated list of broker addresses.</td></tr><tr><td>--config</td><td>string</td><td>            Redpanda config file, if not set the file will be searched for in the default locations</td></tr><tr><td>--password</td><td>string</td><td>          SASL password to be used for authentication.</td></tr><tr><td>--sasl-mechanism</td><td>string</td><td>    The authentication mechanism to use. Supported values: SCRAM-SHA-256, SCRAM-SHA-512.</td></tr><tr><td>--tls-cert</td><td>string</td><td>          The certificate to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-enabled</td><td>-</td><td>             Enable TLS for the Kafka API (not necessary if specifying custom certs).</td></tr><tr><td>--tls-key</td><td>string</td><td>           The certificate key to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-truststore</td><td>string</td><td>    The truststore to be used for TLS communication with the broker.</td></tr><tr><td>--user</td><td>string</td><td>              SASL user to be used for authentication.</td></tr><tr><td>-v, --verbose</td><td>-</td><td>                 Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk group list

 List all groups.

This command lists all groups currently known to Redpanda, including empty
groups that have not yet expired. The BROKER column is which broker node is the
coordinator for the group. This command can be used to track down unknown
groups, or to list groups that need to be cleaned up.

```bash 
Usage:
  rpk group list [flags]

Aliases:
  list, ls
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>-h, --help</td><td>-</td><td>   help for list</td></tr><tr><td>--brokers</td><td>strings</td><td>          Comma-separated list of broker ip:port pairs (e.g. --brokers '192.168.78.34:9092,192.168.78.35:9092,192.179.23.54:9092' ). Alternatively, you may set the REDPANDA_BROKERS environment variable with the comma-separated list of broker addresses.</td></tr><tr><td>--config</td><td>string</td><td>            Redpanda config file, if not set the file will be searched for in the default locations</td></tr><tr><td>--password</td><td>string</td><td>          SASL password to be used for authentication.</td></tr><tr><td>--sasl-mechanism</td><td>string</td><td>    The authentication mechanism to use. Supported values: SCRAM-SHA-256, SCRAM-SHA-512.</td></tr><tr><td>--tls-cert</td><td>string</td><td>          The certificate to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-enabled</td><td>-</td><td>             Enable TLS for the Kafka API (not necessary if specifying custom certs).</td></tr><tr><td>--tls-key</td><td>string</td><td>           The certificate key to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-truststore</td><td>string</td><td>    The truststore to be used for TLS communication with the broker.</td></tr><tr><td>--user</td><td>string</td><td>              SASL user to be used for authentication.</td></tr><tr><td>-v, --verbose</td><td>-</td><td>                 Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk group seek

 Modify a group's current offsets.

This command allows you to modify a group's offsets. Sometimes, you may need to
rewind a group if you had a mistaken deploy, or fast-forward a group if it is
falling behind on messages that can be skipped.

The --to option allows you to seek to the start of partitions, end of
partitions, or after a specific timestamp. The default is to seek any topic
previously committed. Using --topics allows to you set commits for only the
specified topics; all other commits will remain untouched. Topics with no
commits will not be committed unless allowed with --allow-new-topics.

The --to-group option allows you to seek to commits that are in another group.
This is a merging operation: if g1 is consuming topics A and B, and g2 is
consuming only topic B, "rpk group seek g1 --to-group g2" will update g1's
commits for topic B only. The --topics flag can be used to further narrow which
topics are updated. Unlike --to, all non-filtered topics are committed, even
topics not yet being consumed, meaning --allow-new-topics is not needed.

The --to-file option allows to seek to offsets specified in a text file with
the following format:
    [TOPIC] [PARTITION] [OFFSET]
    [TOPIC] [PARTITION] [OFFSET]
    ...
Each line contains the topic, the partition, and the offset to seek to. As with
the prior options, --topics allows filtering which topics are updated. Similar
to --to-group, all non-filtered topics are committed, even topics not yet being
consumed, meaning --allow-new-topics is not needed.

The --to, --to-group, and --to-file options are mutually exclusive. If you are
not authorized to describe or read some topics used in a group, you will not be
able to modify offsets for those topics.

EXAMPLES

Seek group G to June 1st, 2021:
    rpk group seek g --to 1622505600
    or, rpk group seek g --to 1622505600000
    or, rpk group seek g --to 1622505600000000000
Seek group X to the commits of group Y topic foo:
    rpk group seek X --to-group Y --topics foo
Seek group G's topics foo, bar, and biz to the end:
    rpk group seek G --to end --topics foo,bar,biz
Seek group G to the beginning of a topic it was not previously consuming:
    rpk group seek G --to start --topics foo --allow-new-topics

```bash 
Usage:
  rpk group seek [GROUP] --to (start|end|timestamp) --to-group ... --topics ... [flags]
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>--allow-new-topics</td><td>-</td><td>     allow seeking to new topics not currently consumed (implied with --to-group or --to-file)</td></tr><tr><td>-h, --help</td><td>-</td><td>                 help for seek</td></tr><tr><td>--to</td><td>string</td><td>             where to seek (start, end, unix second|millisecond|nanosecond)</td></tr><tr><td>--to-file</td><td>string</td><td>        seek to offsets as specified in the file</td></tr><tr><td>--to-group</td><td>string</td><td>       seek to the commits of another group</td></tr><tr><td>--topics</td><td>stringArray</td><td>    only seek these topics, if any are specified</td></tr><tr><td>--brokers</td><td>strings</td><td>          Comma-separated list of broker ip:port pairs (e.g. --brokers '192.168.78.34:9092,192.168.78.35:9092,192.179.23.54:9092' ). Alternatively, you may set the REDPANDA_BROKERS environment variable with the comma-separated list of broker addresses.</td></tr><tr><td>--config</td><td>string</td><td>            Redpanda config file, if not set the file will be searched for in the default locations</td></tr><tr><td>--password</td><td>string</td><td>          SASL password to be used for authentication.</td></tr><tr><td>--sasl-mechanism</td><td>string</td><td>    The authentication mechanism to use. Supported values: SCRAM-SHA-256, SCRAM-SHA-512.</td></tr><tr><td>--tls-cert</td><td>string</td><td>          The certificate to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-enabled</td><td>-</td><td>             Enable TLS for the Kafka API (not necessary if specifying custom certs).</td></tr><tr><td>--tls-key</td><td>string</td><td>           The certificate key to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-truststore</td><td>string</td><td>    The truststore to be used for TLS communication with the broker.</td></tr><tr><td>--user</td><td>string</td><td>              SASL user to be used for authentication.</td></tr><tr><td>-v, --verbose</td><td>-</td><td>                 Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk help

 Help provides help for any command in the application.
Simply type rpk help [path to command] for full details.

```bash 
Usage:
  rpk help [command] [flags]
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>-h, --help</td><td>-</td><td>   help for help</td></tr><tr><td>-v, --verbose</td><td>-</td><td>   Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk iotune

 Measure filesystem performance and create IO configuration file.

```bash 
Usage:
  rpk iotune [flags]
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>--config</td><td>string</td><td>          Redpanda config file, if not set the file will be searched for in the default locations.</td></tr><tr><td>--directories</td><td>strings</td><td>    List of directories to evaluate</td></tr><tr><td>--duration</td><td>duration</td><td>      Duration of tests.The value passed is a sequence of decimal numbers, each with optional fraction and a unit suffix, such as '300ms', '1.5s' or '2h45m'. Valid time units are 'ns', 'us' (or 'µs'), 'ms', 's', 'm', 'h' (default 10m0s)</td></tr><tr><td>-h, --help</td><td>-</td><td>                  help for iotune</td></tr><tr><td>--timeout</td><td>duration</td><td>       The maximum time after -- to wait for iotune to complete. The value passed is a sequence of decimal numbers, each with optional fraction and a unit suffix, such as '300ms', '1.5s' or '2h45m'. Valid time units are 'ns', 'us' (or 'µs'), 'ms', 's', 'm', 'h' (default 1h0m0s)</td></tr><tr><td>-v, --verbose</td><td>-</td><td>   Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk plugin

 List, download, update, and remove rpk plugins.
	
Plugins augment rpk with new commands.

For a plugin to be used, it must be somewhere discoverable by rpk in your
$PATH. All plugins follow a defined naming scheme:

```bash
rpk-|name|
rpk.ac-|name|
```


All plugins are prefixed with either rpk- or rpk.ac-. When rpk starts up, it
searches all directories in your $PATH for any executable binary that begins
with either of those prefixes. For any binary it finds, rpk adds a command for
that name to the rpk command space itself.

No plugin name can shadow an existing rpk command, and only one plugin can
exist under a given name at once. Plugins are added to the rpk command space on
a first-seen basis. If you have two plugins rpk-foo, and the second is
discovered later on in the $PATH directories, then only the first will be used.
The second will be ignored.

Plugins that have an rpk.ac- prefix indicate that they support the
--help-autocomplete flag. If rpk sees this, rpk will exec the plugin with that
flag when rpk starts up, and the plugin will return all commands it supports as
well as short and long help test for each command. Rpk uses this return to
build a shadow command space within rpk itself so that it looks as if the
plugin exists within rpk. This is particularly useful if you enable
autocompletion.

The expected return for plugins from --help-autocomplete is an array of the
following:

  type pluginHelp struct {
          Path    string   `json:"path,omitempty"`
          Short   string   `json:"short,omitempty"`
          Long    string   `json:"long,omitempty"`
          Example string   `json:"example,omitempty"`
          Args    []string `json:"args,omitempty"`
  }

where "path" is an underscore delimited argument path to a command. For
example, "foo_bar_baz" corresponds to the command "rpk foo bar baz".

```bash 
Usage:
  rpk plugin [command]
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>-h, --help</td><td>-</td><td>   help for plugin</td></tr><tr><td>-v, --verbose</td><td>-</td><td>   Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk plugin install

 Install an rpk plugin.

An rpk plugin must be saved in a directory that is in your $PATH. By default,
this command installs plugins to the first directory in your $PATH. This can
be overridden by specifying the --bin-dir flag.

```bash 
Usage:
  rpk plugin install [PLUGIN] [flags]

Aliases:
  install, download
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>-h, --help</td><td>-</td><td>         help for install</td></tr><tr><td>-u, --update</td><td>-</td><td>       update a locally installed plugin if it differs from the current remote version</td></tr><tr><td>-v, --verbose</td><td>-</td><td>   Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk plugin list

 List all available plugins.

By default, this command fetches the remote manifest and prints plugins
available for download. Any plugin that is already downloaded is prefixed with
an asterisk. If a locally installed plugin has a different sha256sum as the one
specified in the manifest, or if the sha256sum could not be calculated for the
local plugin, an additional message is printed.

You can specify --local to print all locally installed plugins, as well as
whether you have "shadowed" plugins (the same plugin specified multiple times).

```bash 
Usage:
  rpk plugin list [flags]
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>-h, --help</td><td>-</td><td>    help for list</td></tr><tr><td>-l, --local</td><td>-</td><td>   list locally installed plugins and shadowed plugins</td></tr><tr><td>-v, --verbose</td><td>-</td><td>   Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk plugin uninstall

 Uninstall / remove an existing local plugin.

This command lists locally installed plugins and removes the first plugin that
matches the requested removal. If --include-shadowed is specified, this command
also removes all shadowed plugins of the same name.

```bash 
Usage:
  rpk plugin uninstall [NAME] [flags]

Aliases:
  uninstall, rm
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>-h, --help</td><td>-</td><td>               help for uninstall</td></tr><tr><td>--include-shadowed</td><td>-</td><td>   also remove shadowed plugins that have the same name</td></tr><tr><td>-v, --verbose</td><td>-</td><td>   Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk redpanda

 Interact with a local Redpanda process

```bash 
Usage:
  rpk redpanda [command]
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>-h, --help</td><td>-</td><td>   help for redpanda</td></tr><tr><td>-v, --verbose</td><td>-</td><td>   Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk redpanda admin

 Talk to the Redpanda admin listener.

```bash 
Usage:
  rpk redpanda admin [command]
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>--admin-api-tls-cert</td><td>string</td><td>          The certificate to be used for TLS authentication with the Admin API.</td></tr><tr><td>--admin-api-tls-enabled</td><td>-</td><td>             Enable TLS for the Admin API (not necessary if specifying custom certs).</td></tr><tr><td>--admin-api-tls-key</td><td>string</td><td>           The certificate key to be used for TLS authentication with the Admin API.</td></tr><tr><td>--admin-api-tls-truststore</td><td>string</td><td>    The truststore to be used for TLS communication with the Admin API.</td></tr><tr><td>--config</td><td>string</td><td>                      rpk config file, if not set the file will be searched for in the default locations</td></tr><tr><td>-h, --help</td><td>-</td><td>                              help for admin</td></tr><tr><td>--hosts</td><td>strings</td><td>                      A comma-separated list of Admin API addresses (|IP|:|port|). You must specify one for each node.</td></tr><tr><td>-v, --verbose</td><td>-</td><td>   Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk redpanda admin brokers

 View and configure Redpanda brokers through the admin listener.

```bash 
Usage:
  rpk redpanda admin brokers [command]
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>-h, --help</td><td>-</td><td>   help for brokers</td></tr><tr><td>--admin-api-tls-cert</td><td>string</td><td>          The certificate to be used for TLS authentication with the Admin API.</td></tr><tr><td>--admin-api-tls-enabled</td><td>-</td><td>             Enable TLS for the Admin API (not necessary if specifying custom certs).</td></tr><tr><td>--admin-api-tls-key</td><td>string</td><td>           The certificate key to be used for TLS authentication with the Admin API.</td></tr><tr><td>--admin-api-tls-truststore</td><td>string</td><td>    The truststore to be used for TLS communication with the Admin API.</td></tr><tr><td>--config</td><td>string</td><td>                      rpk config file, if not set the file will be searched for in the default locations</td></tr><tr><td>--hosts</td><td>strings</td><td>                      A comma-separated list of Admin API addresses (|IP|:|port|). You must specify one for each node.</td></tr><tr><td>-v, --verbose</td><td>-</td><td>                           Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk redpanda admin brokers decommission

 Decommission the given broker.

Decommissioning a broker removes it from the cluster.

A decommission request is sent to every broker in the cluster, only the cluster
leader handles the request.

```bash 
Usage:
  rpk redpanda admin brokers decommission [BROKER ID] [flags]
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>-h, --help</td><td>-</td><td>   help for decommission</td></tr><tr><td>--admin-api-tls-cert</td><td>string</td><td>          The certificate to be used for TLS authentication with the Admin API.</td></tr><tr><td>--admin-api-tls-enabled</td><td>-</td><td>             Enable TLS for the Admin API (not necessary if specifying custom certs).</td></tr><tr><td>--admin-api-tls-key</td><td>string</td><td>           The certificate key to be used for TLS authentication with the Admin API.</td></tr><tr><td>--admin-api-tls-truststore</td><td>string</td><td>    The truststore to be used for TLS communication with the Admin API.</td></tr><tr><td>--config</td><td>string</td><td>                      rpk config file, if not set the file will be searched for in the default locations</td></tr><tr><td>--hosts</td><td>strings</td><td>                      A comma-separated list of Admin API addresses (|IP|:|port|). You must specify one for each node.</td></tr><tr><td>-v, --verbose</td><td>-</td><td>                           Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk redpanda admin brokers list

 List the brokers in your cluster.

```bash 
Usage:
  rpk redpanda admin brokers list [flags]

Aliases:
  list, ls
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>-h, --help</td><td>-</td><td>   help for list</td></tr><tr><td>--admin-api-tls-cert</td><td>string</td><td>          The certificate to be used for TLS authentication with the Admin API.</td></tr><tr><td>--admin-api-tls-enabled</td><td>-</td><td>             Enable TLS for the Admin API (not necessary if specifying custom certs).</td></tr><tr><td>--admin-api-tls-key</td><td>string</td><td>           The certificate key to be used for TLS authentication with the Admin API.</td></tr><tr><td>--admin-api-tls-truststore</td><td>string</td><td>    The truststore to be used for TLS communication with the Admin API.</td></tr><tr><td>--config</td><td>string</td><td>                      rpk config file, if not set the file will be searched for in the default locations</td></tr><tr><td>--hosts</td><td>strings</td><td>                      A comma-separated list of Admin API addresses (|IP|:|port|). You must specify one for each node.</td></tr><tr><td>-v, --verbose</td><td>-</td><td>                           Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk redpanda admin brokers recommission

 Recommission the given broker if is is still decommissioning.

Recommissioning can stop an active decommission.

Once a broker is decommissioned, it cannot be recommissioned through this
command.

A recommission request is sent to every broker in the cluster, only
the cluster leader handles the request.

```bash 
Usage:
  rpk redpanda admin brokers recommission [BROKER ID] [flags]
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>-h, --help</td><td>-</td><td>   help for recommission</td></tr><tr><td>--admin-api-tls-cert</td><td>string</td><td>          The certificate to be used for TLS authentication with the Admin API.</td></tr><tr><td>--admin-api-tls-enabled</td><td>-</td><td>             Enable TLS for the Admin API (not necessary if specifying custom certs).</td></tr><tr><td>--admin-api-tls-key</td><td>string</td><td>           The certificate key to be used for TLS authentication with the Admin API.</td></tr><tr><td>--admin-api-tls-truststore</td><td>string</td><td>    The truststore to be used for TLS communication with the Admin API.</td></tr><tr><td>--config</td><td>string</td><td>                      rpk config file, if not set the file will be searched for in the default locations</td></tr><tr><td>--hosts</td><td>strings</td><td>                      A comma-separated list of Admin API addresses (|IP|:|port|). You must specify one for each node.</td></tr><tr><td>-v, --verbose</td><td>-</td><td>                           Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk redpanda admin config

 View or modify Redpanda configuration through the admin listener.

```bash 
Usage:
  rpk redpanda admin config [command]
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>-h, --help</td><td>-</td><td>   help for config</td></tr><tr><td>--admin-api-tls-cert</td><td>string</td><td>          The certificate to be used for TLS authentication with the Admin API.</td></tr><tr><td>--admin-api-tls-enabled</td><td>-</td><td>             Enable TLS for the Admin API (not necessary if specifying custom certs).</td></tr><tr><td>--admin-api-tls-key</td><td>string</td><td>           The certificate key to be used for TLS authentication with the Admin API.</td></tr><tr><td>--admin-api-tls-truststore</td><td>string</td><td>    The truststore to be used for TLS communication with the Admin API.</td></tr><tr><td>--config</td><td>string</td><td>                      rpk config file, if not set the file will be searched for in the default locations</td></tr><tr><td>--hosts</td><td>strings</td><td>                      A comma-separated list of Admin API addresses (|IP|:|port|). You must specify one for each node.</td></tr><tr><td>-v, --verbose</td><td>-</td><td>                           Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk redpanda admin config log-level

 Manage a broker's log level.

```bash 
Usage:
  rpk redpanda admin config log-level [command]
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>-h, --help</td><td>-</td><td>   help for log-level</td></tr><tr><td>--admin-api-tls-cert</td><td>string</td><td>          The certificate to be used for TLS authentication with the Admin API.</td></tr><tr><td>--admin-api-tls-enabled</td><td>-</td><td>             Enable TLS for the Admin API (not necessary if specifying custom certs).</td></tr><tr><td>--admin-api-tls-key</td><td>string</td><td>           The certificate key to be used for TLS authentication with the Admin API.</td></tr><tr><td>--admin-api-tls-truststore</td><td>string</td><td>    The truststore to be used for TLS communication with the Admin API.</td></tr><tr><td>--config</td><td>string</td><td>                      rpk config file, if not set the file will be searched for in the default locations</td></tr><tr><td>--hosts</td><td>strings</td><td>                      A comma-separated list of Admin API addresses (|IP|:|port|). You must specify one for each node.</td></tr><tr><td>-v, --verbose</td><td>-</td><td>                           Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk redpanda admin config log-level set

 Set broker logger's log level.

This command temporarily changes a broker logger's log level. Each Redpanda
broker has many loggers, and each can be individually changed. Any change
to a logger persists for a limited amount of time, so as to ensure you do
not accidentally enable debug logging permanently.

It is optional to specify a logger; if you do not, this command will prompt
from the set of available loggers.

The special logger "all" enables all loggers. Alternatively, you can specify
many loggers at once. To see all possible loggers, run the following command:

  redpanda --help-loggers

This command accepts loggers that it does not know of to ensure you can
independently update your redpanda installations from rpk. The success or
failure of enabling each logger is individually printed.

```bash 
Usage:
  rpk redpanda admin config log-level set [LOGGERS...] [flags]
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>-e, --expiry-seconds</td><td>int</td><td>    seconds to persist this log level override before redpanda reverts to its previous settings (if 0, persist until shutdown) (default 300)</td></tr><tr><td>-h, --help</td><td>-</td><td>                 help for set</td></tr><tr><td>--host</td><td>string</td><td>           either an index into admin_api hosts to issue the request to, or a hostname</td></tr><tr><td>--admin-api-tls-cert</td><td>string</td><td>          The certificate to be used for TLS authentication with the Admin API.</td></tr><tr><td>--admin-api-tls-enabled</td><td>-</td><td>             Enable TLS for the Admin API (not necessary if specifying custom certs).</td></tr><tr><td>--admin-api-tls-key</td><td>string</td><td>           The certificate key to be used for TLS authentication with the Admin API.</td></tr><tr><td>--admin-api-tls-truststore</td><td>string</td><td>    The truststore to be used for TLS communication with the Admin API.</td></tr><tr><td>--config</td><td>string</td><td>                      rpk config file, if not set the file will be searched for in the default locations</td></tr><tr><td>--hosts</td><td>strings</td><td>                      A comma-separated list of Admin API addresses (|IP|:|port|). You must specify one for each node.</td></tr><tr><td>-v, --verbose</td><td>-</td><td>                           Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk redpanda admin config print

 Display the current Redpanda configuration.

```bash 
Usage:
  rpk redpanda admin config print [flags]

Aliases:
  print, dump, list, ls, display
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>-h, --help</td><td>-</td><td>          help for print</td></tr><tr><td>--host</td><td>string</td><td>    either an index into admin_api hosts to issue the request to, or a hostname</td></tr><tr><td>--admin-api-tls-cert</td><td>string</td><td>          The certificate to be used for TLS authentication with the Admin API.</td></tr><tr><td>--admin-api-tls-enabled</td><td>-</td><td>             Enable TLS for the Admin API (not necessary if specifying custom certs).</td></tr><tr><td>--admin-api-tls-key</td><td>string</td><td>           The certificate key to be used for TLS authentication with the Admin API.</td></tr><tr><td>--admin-api-tls-truststore</td><td>string</td><td>    The truststore to be used for TLS communication with the Admin API.</td></tr><tr><td>--config</td><td>string</td><td>                      rpk config file, if not set the file will be searched for in the default locations</td></tr><tr><td>--hosts</td><td>strings</td><td>                      A comma-separated list of Admin API addresses (|IP|:|port|). You must specify one for each node.</td></tr><tr><td>-v, --verbose</td><td>-</td><td>                           Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk redpanda check

 Check if system meets redpanda requirements.

```bash 
Usage:
  rpk redpanda check [flags]
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>--config</td><td>string</td><td>       Redpanda config file, if not set the file will be searched for in the default locations.</td></tr><tr><td>-h, --help</td><td>-</td><td>               help for check</td></tr><tr><td>--timeout</td><td>duration</td><td>    The maximum amount of time to wait for the checks and tune processes to complete. The value passed is a sequence of decimal numbers, each with optional fraction and a unit suffix, such as '300ms', '1.5s' or '2h45m'. Valid time units are 'ns', 'us' (or 'µs'), 'ms', 's', 'm', 'h' (default 2s)</td></tr><tr><td>-v, --verbose</td><td>-</td><td>   Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk redpanda config

 Edit configuration.

```bash 
Usage:
  rpk redpanda config [command]
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>-h, --help</td><td>-</td><td>   help for config</td></tr><tr><td>-v, --verbose</td><td>-</td><td>   Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk redpanda config bootstrap

 Initialize the configuration to bootstrap a cluster. --id is mandatory. bootstrap will expect the machine it's running on to have only one private non-loopback IP address associated to it, and use it in the configuration as the node's address. If it has multiple IPs, --self must be specified. In that case, the given IP will be used without checking whether it's among the machine's addresses or not. The elements in --ips must be separated by a comma, no spaces. If omitted, the node will be configured as a root node, that other ones can join later.

```bash 
Usage:
  rpk redpanda config bootstrap --id <id> [--self <ip>] [--ips <ip1,ip2,...>] [flags]
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>--config</td><td>string</td><td>    Redpanda config file, if not set the file will be searched for in the default location.</td></tr><tr><td>-h, --help</td><td>-</td><td>            help for bootstrap</td></tr><tr><td>--id</td><td>int</td><td>           This node's ID (required). (default -1)</td></tr><tr><td>--ips</td><td>strings</td><td>      The list of known node addresses or hostnames</td></tr><tr><td>--self</td><td>string</td><td>      Hint at this node's IP address from within the list passed in --ips</td></tr><tr><td>-v, --verbose</td><td>-</td><td>   Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk redpanda config init

 Init the node after install, by setting the node's UUID.

```bash 
Usage:
  rpk redpanda config init [flags]
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>--config</td><td>string</td><td>    Redpanda config file, if not set the file will be searched for in the default location.</td></tr><tr><td>-h, --help</td><td>-</td><td>            help for init</td></tr><tr><td>-v, --verbose</td><td>-</td><td>   Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk redpanda config set

 Set configuration values, such as the node IDs or the list of seed servers.

```bash 
Usage:
  rpk redpanda config set <key> <value> [flags]
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>--config</td><td>string</td><td>    Redpanda config file, if not set the file will be searched for in the default location.</td></tr><tr><td>-h, --help</td><td>-</td><td>            help for set</td></tr><tr><td>-v, --verbose</td><td>-</td><td>   Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk redpanda mode

 Enable a default configuration mode.

```bash 
Usage:
  rpk redpanda mode <mode> [flags]
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>--config</td><td>string</td><td>    Redpanda config file, if not set the file will be searched for in the default locations.</td></tr><tr><td>-h, --help</td><td>-</td><td>            help for mode</td></tr><tr><td>-v, --verbose</td><td>-</td><td>   Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk redpanda start

 Start redpanda.

```bash 
Usage:
  rpk redpanda start [flags]
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>--advertise-kafka-addr</td><td>strings</td><td>         A comma-separated list of Kafka addresses to advertise (|name|://|host|:|port|)</td></tr><tr><td>--advertise-pandaproxy-addr</td><td>strings</td><td>    A comma-separated list of Pandaproxy addresses to advertise (|name|://|host|:|port|)</td></tr><tr><td>--advertise-rpc-addr</td><td>string</td><td>            The advertised RPC address (|host|:|port|)</td></tr><tr><td>--check</td><td>-</td><td>                               When set to false will disable system checking before starting redpanda (default true)</td></tr><tr><td>--config</td><td>string</td><td>                        Redpanda config file, if not set the file will be searched for in the default locations.</td></tr><tr><td>-h, --help</td><td>-</td><td>                                help for start</td></tr><tr><td>--install-dir</td><td>string</td><td>                   Directory where redpanda has been installed</td></tr><tr><td>--kafka-addr</td><td>strings</td><td>                   A comma-separated list of Kafka listener addresses to bind to (|name|://|host|:|port|)</td></tr><tr><td>--node-id</td><td>int</td><td>                          The node ID. Must be an integer and must be unique within a cluster</td></tr><tr><td>--pandaproxy-addr</td><td>strings</td><td>              A comma-separated list of Pandaproxy listener addresses to bind to (|name|://|host|:|port|)</td></tr><tr><td>--rpc-addr</td><td>string</td><td>                      The RPC address to bind to (|host|:|port|)</td></tr><tr><td>--schema-registry-addr</td><td>strings</td><td>         A comma-separated list of Schema Registry listener addresses to bind to (|name|://|host|:|port|)</td></tr><tr><td>-s, --seeds</td><td>strings</td><td>                        A comma-separated list of seed node addresses (|host|[:|port|]) to connect to</td></tr><tr><td>--timeout</td><td>duration</td><td>                     The maximum time to wait for the checks and tune processes to complete. The value passed is a sequence of decimal numbers, each with optional fraction and a unit suffix, such as '300ms', '1.5s' or '2h45m'. Valid time units are 'ns', 'us' (or 'µs'), 'ms', 's', 'm', 'h' (default 10s)</td></tr><tr><td>--tune</td><td>-</td><td>                                When present will enable tuning before starting redpanda</td></tr><tr><td>--well-known-io</td><td>string</td><td>                 The cloud vendor and VM type, in the format |vendor|:|vm type|:|storage type|</td></tr><tr><td>-v, --verbose</td><td>-</td><td>   Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk redpanda stop

 Stop a local redpanda process. 'rpk stop'
first sends SIGINT, and waits for the specified timeout. Then, if redpanda
hasn't stopped, it sends SIGTERM. Lastly, it sends SIGKILL if it's still
running.

```bash 
Usage:
  rpk redpanda stop [flags]
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>--config</td><td>string</td><td>       Redpanda config file, if not set the file will be searched for in the default locations.</td></tr><tr><td>-h, --help</td><td>-</td><td>               help for stop</td></tr><tr><td>--timeout</td><td>duration</td><td>    The maximum amount of time to wait for redpanda to stop,after each signal is sent. The value passed is asequence of decimal numbers, each with optional fraction and a unit suffix, such as '300ms', '1.5s' or '2h45m'. Valid time units are 'ns', 'us' (or 'µs'), 'ms', 's', 'm', 'h' (default 5s)</td></tr><tr><td>-v, --verbose</td><td>-</td><td>   Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk redpanda tune

 Sets the OS parameters to tune system performance. Available tuners: all, aio_events, ballast_file, disk_scheduler, fstrim, cpu, net, clocksource, swappiness, transparent_hugepages, coredump, disk_irq, disk_nomerges, disk_write_cache.
 In order to get more information about the tuners, run `rpk redpanda tune help <tuner name>`

```bash 
Usage:
  rpk redpanda tune <list of elements to tune> [flags]
  rpk redpanda tune [command]
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>--config</td><td>string</td><td>           Redpanda config file, if not set the file will be searched for in the default locations.</td></tr><tr><td>-r, --dirs</td><td>strings</td><td>            List of *data* directories or places to store data, i.e.: '/var/vectorized/redpanda/', usually your XFS filesystem on an NVMe SSD device.</td></tr><tr><td>-d, --disks</td><td>strings</td><td>           Lists of devices to tune f.e. 'sda1'</td></tr><tr><td>-h, --help</td><td>-</td><td>                   help for tune</td></tr><tr><td>--interactive</td><td>-</td><td>            Ask for confirmation on every step (e.g. tuner execution, configuration generation)</td></tr><tr><td>-m, --mode</td><td>string</td><td>             Operation Mode: one of: [sq, sq_split, mq]</td></tr><tr><td>-n, --nic</td><td>strings</td><td>             Network Interface Controllers to tune</td></tr><tr><td>--output-script</td><td>string</td><td>    If set tuners will generate tuning file that can later be used to tune the system</td></tr><tr><td>--reboot-allowed</td><td>-</td><td>         If set will allow tuners to tune boot parameters and request system reboot.</td></tr><tr><td>--timeout</td><td>duration</td><td>        The maximum time to wait for the tune processes to complete. The value passed is a sequence of decimal numbers, each with optional fraction and a unit suffix, such as '300ms', '1.5s' or '2h45m'. Valid time units are 'ns', 'us' (or 'µs'), 'ms', 's', 'm', 'h' (default 10s)</td></tr><tr><td>-v, --verbose</td><td>-</td><td>   Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk redpanda tune help

 Display detailed information about the tuner.

```bash 
Usage:
  rpk redpanda tune help <tuner> [flags]
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>-h, --help</td><td>-</td><td>   help for help</td></tr><tr><td>-v, --verbose</td><td>-</td><td>   Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk topic

 Create, delete, produce to and consume from Redpanda topics.

```bash 
Usage:
  rpk topic [command]
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>--brokers</td><td>strings</td><td>          Comma-separated list of broker ip:port pairs (e.g. --brokers '192.168.78.34:9092,192.168.78.35:9092,192.179.23.54:9092' ). Alternatively, you may set the REDPANDA_BROKERS environment variable with the comma-separated list of broker addresses.</td></tr><tr><td>--config</td><td>string</td><td>            Redpanda config file, if not set the file will be searched for in the default locations</td></tr><tr><td>-h, --help</td><td>-</td><td>                    help for topic</td></tr><tr><td>--password</td><td>string</td><td>          SASL password to be used for authentication.</td></tr><tr><td>--sasl-mechanism</td><td>string</td><td>    The authentication mechanism to use. Supported values: SCRAM-SHA-256, SCRAM-SHA-512.</td></tr><tr><td>--tls-cert</td><td>string</td><td>          The certificate to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-enabled</td><td>-</td><td>             Enable TLS for the Kafka API (not necessary if specifying custom certs).</td></tr><tr><td>--tls-key</td><td>string</td><td>           The certificate key to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-truststore</td><td>string</td><td>    The truststore to be used for TLS communication with the broker.</td></tr><tr><td>--user</td><td>string</td><td>              SASL user to be used for authentication.</td></tr><tr><td>-v, --verbose</td><td>-</td><td>   Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk topic add-partitions

 Add partitions to existing topics.

```bash 
Usage:
  rpk topic add-partitions [TOPICS...] --num [#] [flags]
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>-h, --help</td><td>-</td><td>      help for add-partitions</td></tr><tr><td>-n, --num</td><td>int</td><td>    numer of partitions to add to each topic</td></tr><tr><td>--brokers</td><td>strings</td><td>          Comma-separated list of broker ip:port pairs (e.g. --brokers '192.168.78.34:9092,192.168.78.35:9092,192.179.23.54:9092' ). Alternatively, you may set the REDPANDA_BROKERS environment variable with the comma-separated list of broker addresses.</td></tr><tr><td>--config</td><td>string</td><td>            Redpanda config file, if not set the file will be searched for in the default locations</td></tr><tr><td>--password</td><td>string</td><td>          SASL password to be used for authentication.</td></tr><tr><td>--sasl-mechanism</td><td>string</td><td>    The authentication mechanism to use. Supported values: SCRAM-SHA-256, SCRAM-SHA-512.</td></tr><tr><td>--tls-cert</td><td>string</td><td>          The certificate to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-enabled</td><td>-</td><td>             Enable TLS for the Kafka API (not necessary if specifying custom certs).</td></tr><tr><td>--tls-key</td><td>string</td><td>           The certificate key to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-truststore</td><td>string</td><td>    The truststore to be used for TLS communication with the broker.</td></tr><tr><td>--user</td><td>string</td><td>              SASL user to be used for authentication.</td></tr><tr><td>-v, --verbose</td><td>-</td><td>                 Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk topic alter-config

 Set, delete, add, and remove key/value configs for a topic.

This command allows you to incrementally alter the configuration for multiple
topics at a time.

Incremental altering supports four operations:

  1) Setting a key=value pair
  2) Deleting a key's value
  3) Appending a new value to a list-of-values key
  4) Subtracting (removing) an existing value from a list-of-values key

The --dry option will validate whether the requested configuration change is
valid, but does not apply it.

```bash 
Usage:
  rpk topic alter-config [TOPICS...] --set key=value --del key2,key3 [flags]
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>--append</td><td>stringArray</td><td>      key=value; value to append to a list-of-values key (repeatable)</td></tr><tr><td>-d, --delete</td><td>stringArray</td><td>      key to delete (repeatable)</td></tr><tr><td>--dry</td><td>-</td><td>                    dry run: validate the alter request, but do not apply</td></tr><tr><td>-h, --help</td><td>-</td><td>                   help for alter-config</td></tr><tr><td>-s, --set</td><td>stringArray</td><td>         key=value pair to set (repeatable)</td></tr><tr><td>--subtract</td><td>stringArray</td><td>    key=value; value to remove from list-of-values key (repeatable)</td></tr><tr><td>--brokers</td><td>strings</td><td>          Comma-separated list of broker ip:port pairs (e.g. --brokers '192.168.78.34:9092,192.168.78.35:9092,192.179.23.54:9092' ). Alternatively, you may set the REDPANDA_BROKERS environment variable with the comma-separated list of broker addresses.</td></tr><tr><td>--config</td><td>string</td><td>            Redpanda config file, if not set the file will be searched for in the default locations</td></tr><tr><td>--password</td><td>string</td><td>          SASL password to be used for authentication.</td></tr><tr><td>--sasl-mechanism</td><td>string</td><td>    The authentication mechanism to use. Supported values: SCRAM-SHA-256, SCRAM-SHA-512.</td></tr><tr><td>--tls-cert</td><td>string</td><td>          The certificate to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-enabled</td><td>-</td><td>             Enable TLS for the Kafka API (not necessary if specifying custom certs).</td></tr><tr><td>--tls-key</td><td>string</td><td>           The certificate key to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-truststore</td><td>string</td><td>    The truststore to be used for TLS communication with the broker.</td></tr><tr><td>--user</td><td>string</td><td>              SASL user to be used for authentication.</td></tr><tr><td>-v, --verbose</td><td>-</td><td>                 Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk topic consume

 Consume records from topics.

Consuming records reads from any amount of input topics, formats each record
according to --format, and prints them to STDOUT. The output formatter
understands a wide variety of formats.

The default output format "--format json" is a special format that outputs each
record as JSON. There may be more single-word-no-escapes formats added later.
Outside of these special formats, formatting follows the rules described below.

Formatting output is based on percent escapes and modifiers. Slashes can be
used for common escapes:

    \t \n \r \\ \xNN

prints tabs, newlines, carriage returns, slashes, or hex encoded characters.p

Percent encoding prints record fields, fetch partition fields, or extra values:

    %t    topic
    %T    topic length
    %k    key
    %K    key length
    %v    topic
    %V    value length
    %h    begin the header specification
    %H    number of headers
    %p    partition
    %o    offset
    %e    leader epoch
    %d    timestamp (formatting described below)
    %x    producer id
    %y    producer epoch

    %[    partition log start offset
    %|    partition last stable offset
    %]    partition high watermark

    %%    percent sign
    %{    left brace
    %}    right brace

    %i    the number of records formatted

MODIFIERS

Text and numbers can be formatted in many different ways, and the default
format can be changed within brace modifiers. %v prints a value, while %v{hex}
prints the value hex encoded. %T prints the length of a topic in ascii, while
%T{big8} prints the length of the topic as an eight byte big endian.

All modifiers go within braces following a percent-escape.

NUMBERS

Formatting number values can have the following modifiers:

     ascii       print the number as ascii (default)

     hex64       sixteen hex characters
     hex32       eight hex characters
     hex16       four hex characters
     hex8        two hex characters
     hex4        one hex character

     big64       eight byte big endian number
     big32       four byte big endian number
     big16       two byte big endian number
     big8        alias for byte

     little64    eight byte little endian number
     little32    four byte little endian number
     little16    two byte little endian number
     little8     alias for byte

     byte        one byte number

All numbers are truncated as necessary per the modifier. Printing %V{byte} for
a length 256 value will print a single null, whereas printing %V{big8} would
print the bytes 1 and 0.

When writing number sizes, the size corresponds to the size of the raw values,
not the size of encoded values. "%T% t{hex}" for the topic "foo" will print
"3 666f6f", not "6 666f6f".

TIMESTAMPS

By default, the timestamp field is printed as a millisecond number value. In
addition to the number modifiers above, timestamps can be printed with either
Go formatting or strftime formatting:

    %d{go[2006-01-02T15:04:05Z07:00]}
    %d{strftime[%F]}

An arbitrary amount of brackets (or braces, or # symbols) can wrap your date
formatting:

    %d{strftime### [%F] ###}

The above will print " [YYYY-MM-DD] ", while the surrounding three # on each
side are used to wrap the formatting. Further details on Go time formatting can
be found at https://pkg.go.dev/time, while further details on strftime
formatting can be read by checking "man strftime".

TEXT

Text fields without modifiers default to writing the raw bytes. Alternatively,
there are the following modifiers:

    %t{hex}
    %k{base64}
    %v{unpack[<bBhH>iIqQc.$]}

The hex modifier hex encodes the text, and the base64 modifier base64 encodes
the text with standard encoding. The unpack modifier has a further internal
specification, similar to timestamps above:

    x    pad character (does not parse input)
    <    switch what follows to little endian
    >    switch what follows to big endian

    b    signed byte
    B    unsigned byte
    h    int16  ("half word")
    H    uint16 ("half word")
    i    int32
    I    uint32
    q    int64  ("quad word")
    Q    uint64 ("quad word")

    c    any character
    .    alias for c
    s    consume the rest of the input as a string
    $    match the end of the line (append error string if anything remains)

Unpacking text can allow translating binary input into readable output. If a
value is a big-endian uint32, %v will print the raw four bytes, while
%v{unpack[>I]} will print the number in as ascii. If unpacking exhausts the
input before something is unpacked fully, an error message is appended to the
output.

HEADERS

Headers are formatted with percent encoding inside of the modifier:

    %h{ %k=%v{hex} }

will print all headers with a space before the key and after the value, an
equals sign between the key and value, and with the value hex encoded. Header
formatting actually just parses the internal format as a record format, so all
of the above rules about %K, %V, text, and numbers apply.

EXAMPLES

A key and value, separated by a space and ending in newline:
    -f '%k %v\n'
A key length as four big endian bytes, and the key as hex:
    -f '%K{big32}%k{hex}'
A little endian uint32 and a string unpacked from a value:
    -f '%v{unpack[is$]}'

MISC

The --offset flag allows for specifying where to begin consuming, and
optionally, where to stop consuming:

    start    consume from the beginning
    end      consume from the end
    +NNN     consume NNN after the start offset
    -NNN     consume NNN before the end offset
    N1-N2    consume from N1 to N2

If consuming a range of offsets, rpk does not currently quit after it has
consumed all partitions through the end range.

```bash 
Usage:
  rpk topic consume TOPICS... [flags]
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>--fetch-max-bytes</td><td>int32</td><td>      maximum amount of bytes per fetch request per broker (default 1048576)</td></tr><tr><td>--fetch-max-wait</td><td>duration</td><td>    maximum amount of time to wait when fetching from a broker before the broker replies (default 5s)</td></tr><tr><td>-g, --group</td><td>string</td><td>               group to use for consuming (incompatible with -p)</td></tr><tr><td>-h, --help</td><td>-</td><td>                      help for consume</td></tr><tr><td>--meta-only</td><td>-</td><td>                 print all record info except the record value (for -f json)</td></tr><tr><td>-n, --num</td><td>int</td><td>                    quit after consuming this number of records (0 is unbounded)</td></tr><tr><td>-p, --partitions</td><td>int32</td><td> int32Slice     comma delimited list of specific partitions to consume (default [])</td></tr><tr><td>--pretty-print</td><td>-</td><td>              pretty print each record over multiple lines (for -f json) (default true)</td></tr><tr><td>--read-committed</td><td>-</td><td>            opt in to reading only committed offsets</td></tr><tr><td>-r, --regex</td><td>-</td><td>                     parse topics as regex; consume any topic that matches any expression</td></tr><tr><td>--brokers</td><td>strings</td><td>          Comma-separated list of broker ip:port pairs (e.g. --brokers '192.168.78.34:9092,192.168.78.35:9092,192.179.23.54:9092' ). Alternatively, you may set the REDPANDA_BROKERS environment variable with the comma-separated list of broker addresses.</td></tr><tr><td>--config</td><td>string</td><td>            Redpanda config file, if not set the file will be searched for in the default locations</td></tr><tr><td>--password</td><td>string</td><td>          SASL password to be used for authentication.</td></tr><tr><td>--sasl-mechanism</td><td>string</td><td>    The authentication mechanism to use. Supported values: SCRAM-SHA-256, SCRAM-SHA-512.</td></tr><tr><td>--tls-cert</td><td>string</td><td>          The certificate to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-enabled</td><td>-</td><td>             Enable TLS for the Kafka API (not necessary if specifying custom certs).</td></tr><tr><td>--tls-key</td><td>string</td><td>           The certificate key to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-truststore</td><td>string</td><td>    The truststore to be used for TLS communication with the broker.</td></tr><tr><td>--user</td><td>string</td><td>              SASL user to be used for authentication.</td></tr><tr><td>-v, --verbose</td><td>-</td><td>                 Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk topic create

 Create topics.

All topics created with this command will have the same number of partitions,
replication factor, and key/value configs.

For example,

	create -c cleanup.policy=compact -r 3 -p 20 foo bar

will create two topics, foo and bar, each with 20 partitions, 3 replicas, and
the cleanup.policy=compact config option set.

```bash 
Usage:
  rpk topic create [TOPICS...] [flags]
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>-d, --dry</td><td>-</td><td>                        dry run: validate the topic creation request; do not create topics</td></tr><tr><td>-h, --help</td><td>-</td><td>                       help for create</td></tr><tr><td>-p, --partitions</td><td>int32</td><td>            number of partitions to create per topic (default 1)</td></tr><tr><td>-r, --replicas</td><td>int16</td><td>              replication factor; if -1, this will be the broker's default.replication.factor (default -1)</td></tr><tr><td>-c, --topic-config</td><td>stringArray</td><td>    key=value config parameters (repeatable; e.g. -c cleanup.policy=compact)</td></tr><tr><td>--brokers</td><td>strings</td><td>          Comma-separated list of broker ip:port pairs (e.g. --brokers '192.168.78.34:9092,192.168.78.35:9092,192.179.23.54:9092' ). Alternatively, you may set the REDPANDA_BROKERS environment variable with the comma-separated list of broker addresses.</td></tr><tr><td>--config</td><td>string</td><td>            Redpanda config file, if not set the file will be searched for in the default locations</td></tr><tr><td>--password</td><td>string</td><td>          SASL password to be used for authentication.</td></tr><tr><td>--sasl-mechanism</td><td>string</td><td>    The authentication mechanism to use. Supported values: SCRAM-SHA-256, SCRAM-SHA-512.</td></tr><tr><td>--tls-cert</td><td>string</td><td>          The certificate to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-enabled</td><td>-</td><td>             Enable TLS for the Kafka API (not necessary if specifying custom certs).</td></tr><tr><td>--tls-key</td><td>string</td><td>           The certificate key to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-truststore</td><td>string</td><td>    The truststore to be used for TLS communication with the broker.</td></tr><tr><td>--user</td><td>string</td><td>              SASL user to be used for authentication.</td></tr><tr><td>-v, --verbose</td><td>-</td><td>                 Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk topic delete

 Delete topics.

This command deletes all requested topics, printing the success or fail status
per topic.

The --regex flag (-r) opts into parsing the input topics as regular expressions
and deleting any non-internal topic that matches any of expressions. The input
expressions are wrapped with ^ and $ so that the expression must match the
whole topic name (which also prevents accidental delete-everything mistakes).

The topic list command accepts the same input regex format as this delete
command. If you want to check what your regular expressions will delete before
actually deleting them, you can check the output of 'rpk topic list -r'.

For example,

    delete foo bar            # deletes topics foo and bar
    delete -r '^f.*' '.*r$'   # deletes any topic starting with f and any topics ending in r
    delete -r '.*'            # deletes all topics
    delete -r .               # deletes any one-character topics

```bash 
Usage:
  rpk topic delete [TOPICS...] [flags]
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>-h, --help</td><td>-</td><td>    help for delete</td></tr><tr><td>-r, --regex</td><td>-</td><td>   parse topics as regex; delete any topic that matches any input topic expression</td></tr><tr><td>--brokers</td><td>strings</td><td>          Comma-separated list of broker ip:port pairs (e.g. --brokers '192.168.78.34:9092,192.168.78.35:9092,192.179.23.54:9092' ). Alternatively, you may set the REDPANDA_BROKERS environment variable with the comma-separated list of broker addresses.</td></tr><tr><td>--config</td><td>string</td><td>            Redpanda config file, if not set the file will be searched for in the default locations</td></tr><tr><td>--password</td><td>string</td><td>          SASL password to be used for authentication.</td></tr><tr><td>--sasl-mechanism</td><td>string</td><td>    The authentication mechanism to use. Supported values: SCRAM-SHA-256, SCRAM-SHA-512.</td></tr><tr><td>--tls-cert</td><td>string</td><td>          The certificate to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-enabled</td><td>-</td><td>             Enable TLS for the Kafka API (not necessary if specifying custom certs).</td></tr><tr><td>--tls-key</td><td>string</td><td>           The certificate key to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-truststore</td><td>string</td><td>    The truststore to be used for TLS communication with the broker.</td></tr><tr><td>--user</td><td>string</td><td>              SASL user to be used for authentication.</td></tr><tr><td>-v, --verbose</td><td>-</td><td>                 Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk topic describe

 Describe a topic.

This command prints detailed information about a topic. There are three
potential sections: a summary of the topic, the topic configs, and a detailed
partitions section. By default, the summary and configs sections are printed.

```bash 
Usage:
  rpk topic describe [TOPIC] [flags]

Aliases:
  describe, info
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>-h, --help</td><td>-</td><td>               help for describe</td></tr><tr><td>-a, --print-all</td><td>-</td><td>          print all sections</td></tr><tr><td>-c, --print-configs</td><td>-</td><td>      print the config section</td></tr><tr><td>-p, --print-partitions</td><td>-</td><td>   print the detailed partitions section</td></tr><tr><td>-s, --print-summary</td><td>-</td><td>      print the summary section</td></tr><tr><td>--brokers</td><td>strings</td><td>          Comma-separated list of broker ip:port pairs (e.g. --brokers '192.168.78.34:9092,192.168.78.35:9092,192.179.23.54:9092' ). Alternatively, you may set the REDPANDA_BROKERS environment variable with the comma-separated list of broker addresses.</td></tr><tr><td>--config</td><td>string</td><td>            Redpanda config file, if not set the file will be searched for in the default locations</td></tr><tr><td>--password</td><td>string</td><td>          SASL password to be used for authentication.</td></tr><tr><td>--sasl-mechanism</td><td>string</td><td>    The authentication mechanism to use. Supported values: SCRAM-SHA-256, SCRAM-SHA-512.</td></tr><tr><td>--tls-cert</td><td>string</td><td>          The certificate to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-enabled</td><td>-</td><td>             Enable TLS for the Kafka API (not necessary if specifying custom certs).</td></tr><tr><td>--tls-key</td><td>string</td><td>           The certificate key to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-truststore</td><td>string</td><td>    The truststore to be used for TLS communication with the broker.</td></tr><tr><td>--user</td><td>string</td><td>              SASL user to be used for authentication.</td></tr><tr><td>-v, --verbose</td><td>-</td><td>                 Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk topic list

 List topics, optionally listing specific topics.

This command lists all topics that you have access to by default. If specifying
topics or regular expressions, this command can be used to know exactly what
topics you would delete if using the same input to the delete command.

Alternatively, you can request specific topics to list, which can be used to
check authentication errors (do you not have access to a topic you were
expecting to see?), or to list all topics that match regular expressions.

The --regex flag (-r) opts into parsing the input topics as regular expressions
and listing any non-internal topic that matches any of expressions. The input
expressions are wrapped with ^ and $ so that the expression must match the
whole topic name. Regular expressions cannot be used to match internal topics,
as such, specifying both -i and -r will exit with failure.

Lastly, --detailed flag (-d) opts in to printing extra per-partition
information.

```bash 
Usage:
  rpk topic list [flags]

Aliases:
  list, ls
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>-d, --detailed</td><td>-</td><td>   print per-partition information for topics</td></tr><tr><td>-h, --help</td><td>-</td><td>       help for list</td></tr><tr><td>-i, --internal</td><td>-</td><td>   print internal topics</td></tr><tr><td>-r, --regex</td><td>-</td><td>      parse topics as regex; list any topic that matches any input topic expression</td></tr><tr><td>--brokers</td><td>strings</td><td>          Comma-separated list of broker ip:port pairs (e.g. --brokers '192.168.78.34:9092,192.168.78.35:9092,192.179.23.54:9092' ). Alternatively, you may set the REDPANDA_BROKERS environment variable with the comma-separated list of broker addresses.</td></tr><tr><td>--config</td><td>string</td><td>            Redpanda config file, if not set the file will be searched for in the default locations</td></tr><tr><td>--password</td><td>string</td><td>          SASL password to be used for authentication.</td></tr><tr><td>--sasl-mechanism</td><td>string</td><td>    The authentication mechanism to use. Supported values: SCRAM-SHA-256, SCRAM-SHA-512.</td></tr><tr><td>--tls-cert</td><td>string</td><td>          The certificate to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-enabled</td><td>-</td><td>             Enable TLS for the Kafka API (not necessary if specifying custom certs).</td></tr><tr><td>--tls-key</td><td>string</td><td>           The certificate key to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-truststore</td><td>string</td><td>    The truststore to be used for TLS communication with the broker.</td></tr><tr><td>--user</td><td>string</td><td>              SASL user to be used for authentication.</td></tr><tr><td>-v, --verbose</td><td>-</td><td>                 Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk topic produce

 Produce records to a topic.

Producing records reads from STDIN, parses input according to --format, and
produce records to Redpanda. The input formatter understands a wide variety of
formats.

Parsing input operates on either sizes or on delimiters, both of which can be
specified in the same formatting options. If using sizes to specify something,
the size must come before what it is specifying. Delimiters match on an exact
text basis. This command will quit with an error if any input fails to match
your specified format.

Slashes can be used for common escapes:

    \t \n \r \\ \xNN

matches tabs, newlines, carriage returns, slashes, and hex encoded characters.

Percent encoding reads into specific values of a record:

    %t    topic
    %T    topic length
    %k    key
    %K    key length
    %v    topic
    %V    value length
    %h    begin the header specification
    %H    number of headers
    %p    partition (if using the --partition flag)

Three escapes exist to parse characters that are used to modify the previous
escapes:

    %%    percent sign
    %{    left brace
    %}    right brace

MODIFIERS

Text and numbers can be read in multiple formats, and the default format can be
changed within brace modifiers. %v reads a value, while %v{hex} reads a value
and then hex decodes it before producing. %T reads the length of a topic from
the input, while %T{3} reads exactly three bytes for a topic from the input.

All modifiers go within braces following a percent-escape.

NUMBERS

Reading number values can have the following modifiers:

     ascii       parse numeric digits until a non-numeric (default)

     hex64       sixteen hex characters
     hex32       eight hex characters
     hex16       four hex characters
     hex8        two hex characters
     hex4        one hex character

     big64       eight byte big endian number
     big32       four byte big endian number
     big16       two byte big endian number
     big8        alias for byte

     little64    eight byte little endian number
     little32    four byte little endian number
     little16    two byte little endian number
     little8     alias for byte

     byte        one byte number

     <digits>    directly specify the length as this many digits

When reading number sizes, the size corresponds to the size of the encoded
values, not the decoded values. "%T{6}%t{hex}" will read six hex bytes and
decode into three.

TEXT

Reading text values can have the following modifiers:

    hex       read text then hex decode it
    base64    read text then std-encoding base64 it

HEADERS

Headers are parsed with an internal key / value specifier format. For example,
the following will read three headers that begin and end with a space and are
separated by an equal:

    %H{3}%h{ %k=%v }

EXAMPLES

In the below examples, we can parse many records at once. The produce command
reads input and tokenizes based on your specified format. Every time the format
is completely matched, a record is produced and parsing begins anew.

A key and value, separated by a space and ending in newline:
    -f '%k %v\n'
A four byte topic, four byte key, and four byte value:
    -f '%T{4}%K{4}%V{4}%t%k%v'
A value to a specific partition, if using a non-negative --partition flag:
    -f '%p %v\n'
A big-endian uint16 key size, the text " foo ", and then that key:
    -f '%K{big16} foo %k'

MISC

Producing requires a topic to produce to. The topic can be specified either
directly on as an argument, or in the input text through %t. A parsed topic
takes precedence over the default passed in topic. If no topic is specified
directly and no topic is parsed, this command will quit with an error.

The input format can parse partitions to produce directly to with %p. Doing so
requires specifying a non-negative --partition flag. Any parsed parstition
takes precedence over the --partition flag; specifying the flag is the main
requirement for being able to directly control which partition to produce to.

You can also specify an output format to write when a record is produced
successfully. The output format follows the same formatting rules as the topic
consume command. See that command's help text for a detailed description.

```bash 
Usage:
  rpk topic produce [TOPIC] [flags]
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>--acks</td><td>int</td><td>                     number of acks required for producing (-1=all, 0=none, 1=leader) (default -1)</td></tr><tr><td>--delivery-timeout</td><td>duration</td><td>    per-record delivery timeout, if non-zero, min 1s</td></tr><tr><td>-H, --header</td><td>stringArray</td><td>           headers in format key:value to add to each record (repeatable)</td></tr><tr><td>-h, --help</td><td>-</td><td>                        help for produce</td></tr><tr><td>-k, --key</td><td>string</td><td>                   a fixed key to use for each record (parsed input keys take precedence)</td></tr><tr><td>-p, --partition</td><td>int32</td><td>              partition to directly produce to, if non-negative (also allows %p parsing to set partitions) (default -1)</td></tr><tr><td>--brokers</td><td>strings</td><td>          Comma-separated list of broker ip:port pairs (e.g. --brokers '192.168.78.34:9092,192.168.78.35:9092,192.179.23.54:9092' ). Alternatively, you may set the REDPANDA_BROKERS environment variable with the comma-separated list of broker addresses.</td></tr><tr><td>--config</td><td>string</td><td>            Redpanda config file, if not set the file will be searched for in the default locations</td></tr><tr><td>--password</td><td>string</td><td>          SASL password to be used for authentication.</td></tr><tr><td>--sasl-mechanism</td><td>string</td><td>    The authentication mechanism to use. Supported values: SCRAM-SHA-256, SCRAM-SHA-512.</td></tr><tr><td>--tls-cert</td><td>string</td><td>          The certificate to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-enabled</td><td>-</td><td>             Enable TLS for the Kafka API (not necessary if specifying custom certs).</td></tr><tr><td>--tls-key</td><td>string</td><td>           The certificate key to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-truststore</td><td>string</td><td>    The truststore to be used for TLS communication with the broker.</td></tr><tr><td>--user</td><td>string</td><td>              SASL user to be used for authentication.</td></tr><tr><td>-v, --verbose</td><td>-</td><td>                 Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk version

 Check the current version.

```bash 
Usage:
  rpk version [flags]
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>-h, --help</td><td>-</td><td>   help for version</td></tr><tr><td>-v, --verbose</td><td>-</td><td>   Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk wasm

 Deploy and remove inline WASM engine scripts.

```bash 
Usage:
  rpk wasm [command]
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>--brokers</td><td>strings</td><td>          Comma-separated list of broker ip:port pairs (e.g. --brokers '192.168.78.34:9092,192.168.78.35:9092,192.179.23.54:9092' ). Alternatively, you may set the REDPANDA_BROKERS environment variable with the comma-separated list of broker addresses.</td></tr><tr><td>--config</td><td>string</td><td>            Redpanda config file, if not set the file will be searched for in the default locations</td></tr><tr><td>-h, --help</td><td>-</td><td>                    help for wasm</td></tr><tr><td>--password</td><td>string</td><td>          SASL password to be used for authentication.</td></tr><tr><td>--sasl-mechanism</td><td>string</td><td>    The authentication mechanism to use. Supported values: SCRAM-SHA-256, SCRAM-SHA-512.</td></tr><tr><td>--tls-cert</td><td>string</td><td>          The certificate to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-enabled</td><td>-</td><td>             Enable TLS for the Kafka API (not necessary if specifying custom certs).</td></tr><tr><td>--tls-key</td><td>string</td><td>           The certificate key to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-truststore</td><td>string</td><td>    The truststore to be used for TLS communication with the broker.</td></tr><tr><td>--user</td><td>string</td><td>              SASL user to be used for authentication.</td></tr><tr><td>-v, --verbose</td><td>-</td><td>   Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk wasm deploy

 Deploy inline WASM function.

```bash 
Usage:
  rpk wasm deploy [PATH] [flags]
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>--description</td><td>string</td><td>    optional description about what the wasm function does</td></tr><tr><td>-h, --help</td><td>-</td><td>                 help for deploy</td></tr><tr><td>--name</td><td>string</td><td>           unique deploy identifier attached to the instance of this script</td></tr><tr><td>--brokers</td><td>strings</td><td>          Comma-separated list of broker ip:port pairs (e.g. --brokers '192.168.78.34:9092,192.168.78.35:9092,192.179.23.54:9092' ). Alternatively, you may set the REDPANDA_BROKERS environment variable with the comma-separated list of broker addresses.</td></tr><tr><td>--config</td><td>string</td><td>            Redpanda config file, if not set the file will be searched for in the default locations</td></tr><tr><td>--password</td><td>string</td><td>          SASL password to be used for authentication.</td></tr><tr><td>--sasl-mechanism</td><td>string</td><td>    The authentication mechanism to use. Supported values: SCRAM-SHA-256, SCRAM-SHA-512.</td></tr><tr><td>--tls-cert</td><td>string</td><td>          The certificate to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-enabled</td><td>-</td><td>             Enable TLS for the Kafka API (not necessary if specifying custom certs).</td></tr><tr><td>--tls-key</td><td>string</td><td>           The certificate key to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-truststore</td><td>string</td><td>    The truststore to be used for TLS communication with the broker.</td></tr><tr><td>--user</td><td>string</td><td>              SASL user to be used for authentication.</td></tr><tr><td>-v, --verbose</td><td>-</td><td>                 Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk wasm generate

 Create an npm template project for inline WASM engine.

```bash 
Usage:
  rpk wasm generate [PROJECT DIRECTORY] [flags]
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>-h, --help</td><td>-</td><td>   help for generate</td></tr><tr><td>--brokers</td><td>strings</td><td>          Comma-separated list of broker ip:port pairs (e.g. --brokers '192.168.78.34:9092,192.168.78.35:9092,192.179.23.54:9092' ). Alternatively, you may set the REDPANDA_BROKERS environment variable with the comma-separated list of broker addresses.</td></tr><tr><td>--config</td><td>string</td><td>            Redpanda config file, if not set the file will be searched for in the default locations</td></tr><tr><td>--password</td><td>string</td><td>          SASL password to be used for authentication.</td></tr><tr><td>--sasl-mechanism</td><td>string</td><td>    The authentication mechanism to use. Supported values: SCRAM-SHA-256, SCRAM-SHA-512.</td></tr><tr><td>--tls-cert</td><td>string</td><td>          The certificate to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-enabled</td><td>-</td><td>             Enable TLS for the Kafka API (not necessary if specifying custom certs).</td></tr><tr><td>--tls-key</td><td>string</td><td>           The certificate key to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-truststore</td><td>string</td><td>    The truststore to be used for TLS communication with the broker.</td></tr><tr><td>--user</td><td>string</td><td>              SASL user to be used for authentication.</td></tr><tr><td>-v, --verbose</td><td>-</td><td>                 Enable verbose logging (default: false).</td></tr></tbody></table>

## rpk wasm remove

 Remove inline WASM function.

```bash 
Usage:
  rpk wasm remove [NAME] [flags]
``` 

#### Flags

<table>
<tbody>
<tr>
<td><strong> Value</strong>
</td>
<td><strong> Type</strong>
</td>
<td><strong> Description</strong>
</td>
</tr><tr><td>-h, --help</td><td>-</td><td>          help for remove</td></tr><tr><td>--brokers</td><td>strings</td><td>          Comma-separated list of broker ip:port pairs (e.g. --brokers '192.168.78.34:9092,192.168.78.35:9092,192.179.23.54:9092' ). Alternatively, you may set the REDPANDA_BROKERS environment variable with the comma-separated list of broker addresses.</td></tr><tr><td>--config</td><td>string</td><td>            Redpanda config file, if not set the file will be searched for in the default locations</td></tr><tr><td>--password</td><td>string</td><td>          SASL password to be used for authentication.</td></tr><tr><td>--sasl-mechanism</td><td>string</td><td>    The authentication mechanism to use. Supported values: SCRAM-SHA-256, SCRAM-SHA-512.</td></tr><tr><td>--tls-cert</td><td>string</td><td>          The certificate to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-enabled</td><td>-</td><td>             Enable TLS for the Kafka API (not necessary if specifying custom certs).</td></tr><tr><td>--tls-key</td><td>string</td><td>           The certificate key to be used for TLS authentication with the broker.</td></tr><tr><td>--tls-truststore</td><td>string</td><td>    The truststore to be used for TLS communication with the broker.</td></tr><tr><td>--user</td><td>string</td><td>              SASL user to be used for authentication.</td></tr><tr><td>-v, --verbose</td><td>-</td><td>                 Enable verbose logging (default: false).</td></tr></tbody></table>
