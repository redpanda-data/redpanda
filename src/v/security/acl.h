/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once
#include "kafka/types.h"
#include "model/fundamental.h"
#include "seastarx.h"
#include "utils/named_type.h"

#include <seastar/core/sstring.hh>
#include <seastar/net/inet_address.hh>

#include <absl/container/btree_map.h>
#include <absl/container/flat_hash_set.h>

#include <iosfwd>
#include <variant>

namespace security {

namespace details {
template<class T>
struct dependent_false : std::false_type {};
} // namespace details

// cluster is a resource type and the acl data model requires that resources
// have names, so this is a fixed name for that resource.
//
// tools that manage kafka APIs assume a fixed name for the cluster resource:
// `kafka-cluster` and put this string in requests that operate on cluster ACLs.
// This means that the name is effectively part of the protocol and we can adopt
// the same name.
using acl_cluster_name = named_type<ss::sstring, struct acl_cluster_name_type>;
inline const acl_cluster_name default_cluster_name("kafka-cluster");

/*
 * An ACL resource type.
 *
 * IMPORTANT: on-write value
 */
enum class resource_type : int8_t {
    topic = 0,
    group = 1,
    cluster = 2,
    transactional_id = 3,
};

template<typename T>
consteval resource_type get_resource_type() {
    if constexpr (std::is_same_v<T, model::topic>) {
        return resource_type::topic;
    } else if constexpr (std::is_same_v<T, kafka::group_id>) {
        return resource_type::group;
    } else if constexpr (std::is_same_v<T, security::acl_cluster_name>) {
        return resource_type::cluster;
    } else if constexpr (std::is_same_v<T, kafka::transactional_id>) {
        return resource_type::transactional_id;
    } else {
        static_assert(details::dependent_false<T>::value, "Unsupported type");
    }
}

/*
 * A pattern rule for matching ACL resource names.
 *
 * IMPORTANT: on-write value
 */
enum class pattern_type : int8_t {
    literal = 0,
    prefixed = 1,
};

/*
 * An operation on a resource.
 *
 * IMPORTANT: on-write value
 */
enum class acl_operation : int8_t {
    all = 0,
    read = 1,
    write = 2,
    create = 3,
    remove = 4,
    alter = 5,
    describe = 6,
    cluster_action = 7,
    describe_configs = 8,
    alter_configs = 9,
    idempotent_write = 10,
};

/*
 * Compute the implied operations based on the specified operation.
 */
std::vector<acl_operation> acl_implied_ops(acl_operation operation);

std::ostream& operator<<(std::ostream&, acl_operation);

/*
 * Grant or deny access.
 *
 * IMPORTANT: on-write value
 */
enum class acl_permission : int8_t {
    deny = 0,
    allow = 1,
};

std::ostream& operator<<(std::ostream&, acl_permission);

/*
 * Principal type
 *
 * Only `User` is currently supported, but when integrating with other identity
 * providers it may be useful to introduce a `Group` type.
 *
 * IMPORTANT: on-write value
 */
enum class principal_type : int8_t {
    user = 0,
    ephemeral_user = 1,
};

std::ostream& operator<<(std::ostream&, resource_type);
std::ostream& operator<<(std::ostream&, pattern_type);
std::ostream& operator<<(std::ostream&, principal_type);

/*
 * Kafka principal is (principal-type, principal)
 */
class acl_principal
  : public serde::
      envelope<acl_principal, serde::version<0>, serde::compat_version<0>> {
public:
    acl_principal() = default;
    acl_principal(principal_type type, ss::sstring name)
      : _type(type)
      , _name(std::move(name)) {}

    friend bool operator==(const acl_principal&, const acl_principal&)
      = default;

    template<typename H>
    friend H AbslHashValue(H h, const acl_principal& e) {
        return H::combine(std::move(h), e._type, e._name);
    }

    friend std::ostream& operator<<(std::ostream&, const acl_principal&);

    const ss::sstring& name() const { return _name; }
    principal_type type() const { return _type; }
    bool wildcard() const { return _name == "*"; }

    auto serde_fields() { return std::tie(_type, _name); }

private:
    principal_type _type;
    ss::sstring _name;
};

inline const acl_principal acl_wildcard_user(principal_type::user, "*");

/*
 * Resource pattern matches resources using a (type, name, pattern) tuple. The
 * pattern type changes how matching occurs (e.g. literal, name prefix).
 */
class resource_pattern
  : public serde::
      envelope<resource_pattern, serde::version<0>, serde::compat_version<0>> {
public:
    static constexpr const char* wildcard = "*";
    resource_pattern() = default;
    resource_pattern(resource_type type, ss::sstring name, pattern_type pattern)
      : _resource(type)
      , _name(std::move(name))
      , _pattern(pattern) {}

    friend bool operator==(const resource_pattern&, const resource_pattern&)
      = default;

    template<typename H>
    friend H AbslHashValue(H h, const resource_pattern& e) {
        return H::combine(std::move(h), e._resource, e._name, e._pattern);
    }

    friend std::ostream& operator<<(std::ostream&, const resource_pattern&);

    resource_type resource() const { return _resource; }
    const ss::sstring& name() const { return _name; }
    pattern_type pattern() const { return _pattern; }

    auto serde_fields() { return std::tie(_resource, _name, _pattern); }

private:
    resource_type _resource;
    ss::sstring _name;
    pattern_type _pattern;
};

/*
 * A host (or wildcard) in an ACL rule.
 */
class acl_host
  : public serde::
      envelope<acl_host, serde::version<0>, serde::compat_version<0>> {
public:
    acl_host() = default;
    explicit acl_host(const ss::sstring& host)
      : _addr(host) {}

    explicit acl_host(ss::net::inet_address host)
      : _addr(host) {}

    static acl_host wildcard_host() { return acl_host{}; }

    friend bool operator==(const acl_host&, const acl_host&) = default;

    template<typename H>
    friend H AbslHashValue(H h, const acl_host& host) {
        if (host._addr) {
            return H::combine(std::move(h), *host._addr);
        } else {
            return H::combine(std::move(h), ss::net::inet_address{});
        }
    }

    friend std::ostream& operator<<(std::ostream&, const acl_host&);

    std::optional<ss::net::inet_address> address() const { return _addr; }

    auto serde_fields() { return std::tie(_addr); }

private:
    std::optional<ss::net::inet_address> _addr;
};

inline const acl_host acl_wildcard_host = acl_host::wildcard_host();

/*
 * An ACL entry specifies if a principal (connected from a specific host) is
 * permitted to execute an operation on. When associated with a resource, it
 * describes if the principal can execute the operation on that resource.
 */
class acl_entry
  : public serde::
      envelope<acl_entry, serde::version<0>, serde::compat_version<0>> {
public:
    acl_entry() = default;
    acl_entry(
      acl_principal principal,
      acl_host host,
      acl_operation operation,
      acl_permission permission)
      : _principal(std::move(principal))
      , _host(host)
      , _operation(operation)
      , _permission(permission) {}

    friend bool operator==(const acl_entry&, const acl_entry&) = default;

    template<typename H>
    friend H AbslHashValue(H h, const acl_entry& e) {
        return H::combine(
          std::move(h), e._principal, e._host, e._operation, e._permission);
    }

    friend std::ostream& operator<<(std::ostream&, const acl_entry&);

    const acl_principal& principal() const { return _principal; }
    const acl_host& host() const { return _host; }
    acl_operation operation() const { return _operation; }
    acl_permission permission() const { return _permission; }

    auto serde_fields() {
        return std::tie(_principal, _host, _operation, _permission);
    }

private:
    acl_principal _principal;
    acl_host _host;
    acl_operation _operation;
    acl_permission _permission;
};

/*
 * An ACL binding is an association of resource(s) and an ACL entry. An ACL
 * binding describes if a principal may access resources.
 */
class acl_binding
  : public serde::
      envelope<acl_binding, serde::version<0>, serde::compat_version<0>> {
public:
    acl_binding() = default;
    acl_binding(resource_pattern pattern, acl_entry entry)
      : _pattern(std::move(pattern))
      , _entry(std::move(entry)) {}

    friend bool operator==(const acl_binding&, const acl_binding&) = default;

    template<typename H>
    friend H AbslHashValue(H h, const acl_binding& e) {
        return H::combine(std::move(h), e._pattern, e._entry);
    }

    friend std::ostream& operator<<(std::ostream&, const acl_binding&);

    const resource_pattern& pattern() const { return _pattern; }
    const acl_entry& entry() const { return _entry; }

    auto serde_fields() { return std::tie(_pattern, _entry); }

private:
    resource_pattern _pattern;
    acl_entry _entry;
};

/*
 * A filter for matching resources.
 */
class resource_pattern_filter
  : public serde::envelope<
      resource_pattern_filter,
      serde::version<0>,
      serde::compat_version<0>> {
public:
    enum class serialized_pattern_type {
        literal = 0,
        prefixed = 1,
        match = 2,
    };

    static serialized_pattern_type to_pattern(security::pattern_type from) {
        switch (from) {
        case security::pattern_type::literal:
            return serialized_pattern_type::literal;
        case security::pattern_type::prefixed:
            return serialized_pattern_type::prefixed;
        }
        __builtin_unreachable();
    }

    struct pattern_match {
        friend bool operator==(const pattern_match&, const pattern_match&)
          = default;

        friend std::ostream& operator<<(std::ostream&, const pattern_match&);
    };
    using pattern_filter_type = std::variant<pattern_type, pattern_match>;

    resource_pattern_filter() = default;

    resource_pattern_filter(
      std::optional<resource_type> type,
      std::optional<ss::sstring> name,
      std::optional<pattern_filter_type> pattern)
      : _resource(type)
      , _name(std::move(name))
      , _pattern(pattern) {}

    // NOLINTNEXTLINE(hicpp-explicit-conversions)
    resource_pattern_filter(const resource_pattern& resource)
      : resource_pattern_filter(
        resource.resource(), resource.name(), resource.pattern()) {}

    /*
     * A filter that matches any resource.
     */
    static const resource_pattern_filter& any() {
        static const resource_pattern_filter filter(
          std::nullopt, std::nullopt, std::nullopt);
        return filter;
    }

    bool matches(const resource_pattern& pattern) const;
    std::vector<resource_pattern> to_resource_patterns() const;

    std::optional<resource_type> resource() const { return _resource; }
    const std::optional<ss::sstring>& name() const { return _name; }
    std::optional<pattern_filter_type> pattern() const { return _pattern; }

    friend void read_nested(
      iobuf_parser& in,
      resource_pattern_filter& filter,
      size_t const bytes_left_limit);

    friend void write(iobuf& out, resource_pattern_filter filter);

    friend bool
    operator==(const resource_pattern_filter&, const resource_pattern_filter&)
      = default;

    friend std::ostream&
    operator<<(std::ostream&, const resource_pattern_filter&);

private:
    std::optional<resource_type> _resource;
    std::optional<ss::sstring> _name;
    std::optional<pattern_filter_type> _pattern;
};

std::ostream&
operator<<(std::ostream&, resource_pattern_filter::serialized_pattern_type);

/*
 * A filter for matching ACL entries.
 */
class acl_entry_filter
  : public serde::
      envelope<acl_entry_filter, serde::version<0>, serde::compat_version<0>> {
public:
    acl_entry_filter() = default;
    // NOLINTNEXTLINE(hicpp-explicit-conversions)
    acl_entry_filter(const acl_entry& entry)
      : acl_entry_filter(
        entry.principal(),
        entry.host(),
        entry.operation(),
        entry.permission()) {}

    acl_entry_filter(
      std::optional<acl_principal> principal,
      std::optional<acl_host> host,
      std::optional<acl_operation> operation,
      std::optional<acl_permission> permission)
      : _principal(std::move(principal))
      , _host(std::move(host))
      , _operation(operation)
      , _permission(permission) {}

    /*
     * A filter that matches any ACL entry.
     */
    static const acl_entry_filter& any() {
        static const acl_entry_filter filter(
          std::nullopt, std::nullopt, std::nullopt, std::nullopt);
        return filter;
    }

    bool matches(const acl_entry& other) const;

    const std::optional<acl_principal>& principal() const { return _principal; }
    std::optional<acl_host> host() const { return _host; }
    std::optional<acl_operation> operation() const { return _operation; }
    std::optional<acl_permission> permission() const { return _permission; }

    auto serde_fields() {
        return std::tie(_principal, _host, _operation, _permission);
    }

    friend bool operator==(const acl_entry_filter&, const acl_entry_filter&)
      = default;

    friend std::ostream& operator<<(std::ostream&, const acl_entry_filter&);

private:
    std::optional<acl_principal> _principal;
    std::optional<acl_host> _host;
    std::optional<acl_operation> _operation;
    std::optional<acl_permission> _permission;
};

/*
 * A filter for matching ACL bindings.
 */
class acl_binding_filter
  : public serde::envelope<
      acl_binding_filter,
      serde::version<0>,
      serde::compat_version<0>> {
public:
    acl_binding_filter() = default;
    acl_binding_filter(resource_pattern_filter pattern, acl_entry_filter acl)
      : _pattern(std::move(pattern))
      , _acl(std::move(acl)) {}

    /*
     * A filter that matches any ACL binding.
     */
    static const acl_binding_filter& any() {
        static const acl_binding_filter filter(
          resource_pattern_filter::any(), acl_entry_filter::any());
        return filter;
    }

    bool matches(const acl_binding& binding) const {
        return _pattern.matches(binding.pattern())
               && _acl.matches(binding.entry());
    }

    const resource_pattern_filter& pattern() const { return _pattern; }
    const acl_entry_filter& entry() const { return _acl; }

    friend bool operator==(const acl_binding_filter&, const acl_binding_filter&)
      = default;

    friend std::ostream& operator<<(std::ostream&, const acl_binding_filter&);

    auto serde_fields() { return std::tie(_pattern, _acl); }

private:
    resource_pattern_filter _pattern;
    acl_entry_filter _acl;
};

} // namespace security
