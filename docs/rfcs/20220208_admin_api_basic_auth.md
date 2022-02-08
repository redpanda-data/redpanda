- Feature Name: HTTP Basic Auth for admin API
- Status: implemented.
- Start Date: 2022-02-08
- Authors: John Spray
- Issue: https://github.com/redpanda-data/redpanda/issues/3702

# Executive Summary

Enable admin API clients to authenticate using SASL username/password credientials
passed into an HTTP "Basic" Authentication header. 

These checks will be off by default for backward compatibility, but enabled during
setup of new clusters.

## What is being proposed

A cluster configuration property:
* `admin_api_require_auth` (boolean, default false).

When this property is true, requests to the admin API will be rejected unless they
carry a valid username+password, and that username is the existing `superusers` list.

## Why (short reason)

Currently, users' only secure option for the admin API is to enable mTLS and
disable plain HTTP access, and then only issue mTLS client certs to users who
are permitted to perform administrative operations.

This causes two problems:
- Users who have not set up mTLS (and may not want to) have no authentication
  mechanism for the admin API.  They have to either refrain from using it entirely,
  or accept the security risk of having an unauthenticated listener (e.g. on 127.0.0.1:9644)
- Users who _have_ set up mTLS have no way of differentiating regular kafka clients
  from a privileged identity that should be able to perform administrative operations.

Resolving these issues unlocks use of the Admin API in 
production environments, including new centralized configuration workflows that
rely on admin API access.

## How (short plan)

The existing `superusers` configuration property will be used to discriminate between
regular users, and users that should be permitted to use the admin API.

The plaintext password in the Authentication header can be validated against stored
scram credentials, by passing the password through the same salt/hmac steps that we would use
if setting a new password.


# Guide-level explanation

## How do we teach this?

- Explain use of `--user` and `--password` on `rpk`.
- Explain that the credentials can be the same as a Kafka (SASL) username
- Explain concept of a superuser, and how the list of superusers can be
  edited like other configuration properties (i.e. via `rpk cluster config edit`)

## What if the user locks themselves out?

In extreme circumstances, they can disable auth using 
`rpk cluster config force-reset admin_api_require_auth` while Redpanda is offline.

However, unless the user forgets a password they should not be able to lock themselves
out: the API endpoint for modifying cluster config will ensure that when auth is enabled,
it is always enabled by an authenticated user who is already a superuser, so that a naive
user can't permanently lock themselves out just by enabling auth before they've declared
themselves a superuser.

## Introducing new named concepts.

All the concepts in use here already exist:
- HTTP Basic Authentication is a standard
- The concept of a superuser already exists, we are extending its definition
  to include access to the admin API.
- The concept of a SASL user account already exists.

# Reference-level explanation

## Detailed design

### Auth hooks in admin API

We are using seastar's built-in HTTP framework, which does not provide
generic request/response hooks: there is not a place to connect an authentication
callback.

As a substitute, we will introduce a helper wrapper around our method callbacks,
that applies authentication rules before running the inner function.

### Runtime configuration property changes

To support dynamic updates to `superusers` and `admin_api_require_sasl`, the
values of these properties will simply not be cached anywhere.  Reading them
out of the per-shard cluster configuration object at the start of each request
is a cheap operation.

### Which endpoints will be authenticated?

All endpoints will be be subject to authentication rules, with two exceptions:
- `/v1/metrics` - the metrics are not generally sensitive, and it is very
  convenient to be able to point prometheus at an endpoint without having
  to arrange secrets
- `/v1/status` - a node should be willing to tell any client whether it is up.

## Interaction with other features

All `rpk` commands that rely on admin API access will now require proper
`--user` and `--password` flags on systems where `admin_require_sasl` is true.

The kubernetes operator currently relies on unauthenticated access to the
admin API.   It also creates and stores superuser credentials when creating
a cluster, so extending it to use these credentials to access the API is straightforward,
as long as the end user does not themselves modify these superuser credentials.

During kubernetes cluster creation, the operator can bootstrap authentication as follows:
- Bring up nodes in an initial state with no external admin API connectivity, only
  exposing it on the pod-local IPs.
- Using admin API via pod local IPs, create superuser account
- Using admin API via pod local IPs, set `admin_api_require_sasl` to true.
- Add external admin API listeners and restart nodes.

Clearly the above sequence is simplified if the cluster does not have any external
connectivity to the admin API.

## Upgrades

SCRAM credentials may be either sha256 or sha512 type, whereas Authentication header does
not specify which to use.  For existing systems, we can only validate passwords by trying
both algorithms.

The credential store should be extended to record the algorithm for each credential.  This
might be done as part of implementing this feature, or we might choose to defer that until
the `serde` transition, for convenient backward compatible encoding.

## Telemetry & Observability

* WARN-level log messages on access denied events
* WARN-level log message on startup if an unauthenticated listener is running

## Drawbacks

* rpk command lines are made much more verbose with user/password information.
  * This is mitigated in cloud environments where the user is provided
    with an `rpk.sh` wrapper that automatically loads superuser credentials from
    kubernetes.
  * Bare metal customers are likely to do something similar.
* Risk of user "locking themselves out of the house" once the admin API is hidden
  behind the same credentials that the admin API is used to edit.
  * This is intrinsic to improving security
  * We will provide a recovery procedure for this case.
* Risk of user locking out the kubernetes operator, for example by deleting
  the superuser account that the operator uses.
  * This risk already existed in general, as the operator does not have
    a special privilege mechanism to protect its own credentials from the end user
  * If this occurs, SREs will need to use the same mechanisms as if the user
    had locked themselves out.
* Bare-metal users might store passwords in cleartext scripts
  * This is still less bad than the API being entirely unauthenticated.
  * If users have their own secrets infrastructure, they should be using it
    to store these secrets somewhere safe.

## Rationale and Alternatives

### Alternative: do nothing

### Alternative: HTTP Digest auth

This is not possible using existing stored SCRAM credentials, because
HTTP digest hashing is different from the hashing used in SCRAM credentials.

We could implement this for newly created credentials if we calculated
and stored the HTTP Digest MD5 hash of the password during account creation
(at which time, we have the plaintext password).

While HTTP digest auth is slightly better than basic auth in that it doesn't
transmit passwords in the clear, it has many security issues of its own, and
in practice either of these protocols should be transported over TLS for
a meaningful level of security.

### Alternative: restrict admin API to unauthenticated localhost listener

If the admin API only listened on 127.0.0.1, then users could SSH to a redpanda
node and drive admin operations from there.  This has several downsides:
- Security is still weak, as any process running on the redpanda node can
  access the admin API
- It is inconvenient to do all operations via an SSH session
- The admin API can't issue a redirect to another node (e.g. to redirect
  to controller leader) if all the nodes are only listening on localhost.

### Alternative: create separate admin API credentials (e.g. API keys)

It is convenient but not essential to use the same system of credentials
for the admin API that we use for Kafka protocol.  We could implement
a separate pool of API keys.

This approach would increase the surface area of the feature substantially,
as we would not be able to use the existing user CRUD commands, and would
be more complex to explain to the user compared with the existing user/password
framework they know from the world of Kafka.

### Alternative: shared secret in node config

For use cases like the kubernetes operator, it might be sufficient to simply
store a random string in the node configuration file (redpanda.yaml), and
use this as a shared secret.

This has several downsides:
* We would be storing a de-facto cleartext password
* Key changes would require node restart
* User would be unable to distinguish clients (for audit purposes) by
  user account if all shared a statically set token for admin API access.

### Alternative: only authenticate writes

For some use cases (e.g. monitoring), it might be convenient to give the user read-only
access without providing a secret.

However, this cannot be applied uniformly to all endpoints, for example the config
endpoint includes sensitive configuration properties like the cloud access key.  It would
also remove the ability to record an audit record of which users were accessing what
information.

### Alternative: per-endpoint RBAC

This isn't so much an alternative as a possible extension.  The authorization layer
can be extended in many ways in the future: this RFC is just about getting some
kind of authentication (SASL user account) combined with the most basic level
of authorization (the superuser list).

## Future work

Integration with operator, if enable admin API auth in the cloud is needed.  Currently
admin API is not exposed to users on FMC clusters.
