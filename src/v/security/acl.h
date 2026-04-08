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
#include "absl/container/btree_map.h"
#include "absl/container/flat_hash_set.h"
#include "base/seastarx.h"
#include "base/type_traits.h"
#include "kafka/protocol/types.h"
#include "model/fundamental.h"
#include "pandaproxy/schema_registry/types.h"
#include "security/audit/schemas/types.h"
#include "serde/envelope.h"
#include "serde/rw/enum.h"
#include "serde/rw/envelope.h"
#include "serde/rw/inet_address.h"
#include "serde/rw/named_type.h"
#include "serde/rw/optional.h"
#include "serde/rw/rw.h"
#include "serde/rw/variant.h"
#include "utils/named_type.h"

#include <seastar/core/sstring.hh>
#include <seastar/net/inet_address.hh>

#include <fmt/core.h>

#include <iosfwd>
#include <variant>

namespace security {

/*
 * Conversions throw acl_conversion_error and the exception message via (what())
 * is generally what should be returned as the error message in kafka responses.
 *
 * Using an exception here eliminates the need to write c/go-style error
 * handling for the large number of fields that need to be converted.
 */
struct acl_conversion_error : std::exception {
    explicit acl_conversion_error(ss::sstring msg)
      : msg{std::move(msg)} {}
    const char* what() const noexcept final { return msg.c_str(); }
    ss::sstring msg;
};

template<typename E>
std::enable_if_t<std::is_enum_v<E>, std::optional<E>>
  from_string_view(std::string_view);

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
    sr_subject = 4,
    sr_registry = 5,
};

constexpr std::string_view to_string_view(resource_type type) {
    switch (type) {
    case resource_type::topic:
        return "topic";
    case resource_type::group:
        return "group";
    case resource_type::cluster:
        return "cluster";
    case resource_type::transactional_id:
        return "transactional_id";
    case resource_type::sr_subject:
        return "subject";
    case resource_type::sr_registry:
        return "registry";
    }
    __builtin_unreachable();
}

template<>
std::optional<resource_type>
from_string_view<resource_type>(std::string_view str);

template<typename T>
consteval resource_type get_resource_type() {
    namespace ppsr = pandaproxy::schema_registry;
    if constexpr (std::is_same_v<T, model::topic>) {
        return resource_type::topic;
    } else if constexpr (std::is_same_v<T, kafka::group_id>) {
        return resource_type::group;
    } else if constexpr (std::is_same_v<T, security::acl_cluster_name>) {
        return resource_type::cluster;
    } else if constexpr (std::is_same_v<T, kafka::transactional_id>) {
        return resource_type::transactional_id;
    } else if constexpr (std::is_same_v<T, ppsr::context_subject>) {
        return resource_type::sr_subject;
    } else if constexpr (std::is_same_v<T, ppsr::registry_resource>) {
        return resource_type::sr_registry;
    } else {
        static_assert(base::unsupported_type<T>::value, "Unsupported type");
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

constexpr std::string_view to_string_view(pattern_type type) {
    switch (type) {
    case pattern_type::literal:
        return "literal";
    case pattern_type::prefixed:
        return "prefixed";
    }
    __builtin_unreachable();
}

template<>
std::optional<pattern_type>
from_string_view<pattern_type>(std::string_view str);

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

constexpr std::string_view to_string_view(acl_operation op) {
    switch (op) {
    case acl_operation::all:
        return "all";
    case acl_operation::read:
        return "read";
    case acl_operation::write:
        return "write";
    case acl_operation::create:
        return "create";
    case acl_operation::remove:
        return "delete";
    case acl_operation::alter:
        return "alter";
    case acl_operation::describe:
        return "describe";
    case acl_operation::cluster_action:
        return "cluster_action";
    case acl_operation::describe_configs:
        return "describe_configs";
    case acl_operation::alter_configs:
        return "alter_configs";
    case acl_operation::idempotent_write:
        return "idempotent_write";
    }
    __builtin_unreachable();
}

std::ostream& operator<<(std::ostream&, acl_operation);
template<>
std::optional<acl_operation>
from_string_view<acl_operation>(std::string_view str);

/*
 * Grant or deny access.
 *
 * IMPORTANT: on-write value
 */
enum class acl_permission : int8_t {
    deny = 0,
    allow = 1,
};

constexpr std::string_view to_string_view(acl_permission perm) {
    switch (perm) {
    case acl_permission::deny:
        return "deny";
    case acl_permission::allow:
        return "allow";
    }
    __builtin_unreachable();
}

std::ostream& operator<<(std::ostream&, acl_permission);
template<>
std::optional<acl_permission>
from_string_view<acl_permission>(std::string_view str);

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
    role = 2,
    group = 3,
};

constexpr std::string_view to_string_view(principal_type type) {
    switch (type) {
    case principal_type::user:
        return "user";
    case principal_type::ephemeral_user:
        return "ephemeral user";
    case principal_type::role:
        return "role";
    case principal_type::group:
        return "group";
    }
    __builtin_unreachable();
}

template<>
std::optional<principal_type>
from_string_view<principal_type>(std::string_view str);

std::ostream& operator<<(std::ostream&, resource_type);
std::ostream& operator<<(std::ostream&, pattern_type);
std::ostream& operator<<(std::ostream&, principal_type);

/**
 * Abstract interface for Kafka principals.
 *
 * A Kafka principal is (principal-type, principal).
 *
 * Note that no virtual destructor is provided here. This is intentional.
 * acl_principal_base is meant to support polymorphic references at various
 * auth APIs, **not** to support polymorphic construction/destruction of
 * principal instances.
 *
 */
class acl_principal_base {
public:
    /**
     * Get a view to the principal name.
     */
    virtual std::string_view name_view() const = 0;
    /**
     * Get the principal type
     */
    virtual principal_type type() const = 0;

private:
    template<typename H>
    friend H AbslHashValue(H h, const acl_principal_base& e) {
        return H::combine(std::move(h), e.type(), e.name_view());
    }

    friend bool
    operator==(const acl_principal_base& l, const acl_principal_base& r) {
        return l.type() == r.type() && l.name_view() == r.name_view();
    }

    friend std::ostream& operator<<(std::ostream&, const acl_principal_base&);
};

/**
 * Concrete instance of a Kafka principal.
 *
 * This implementation owns the memory for its name.
 */
class acl_principal final
  : public serde::
      envelope<acl_principal, serde::version<0>, serde::compat_version<0>>
  , public acl_principal_base {
public:
    acl_principal() = default;
    acl_principal(principal_type type, ss::sstring name)
      : _type(type)
      , _name(std::move(name)) {}

    static acl_principal from_string(std::string_view principal);

    /**
     * Get a view to the principal name.
     */
    std::string_view name_view() const override { return _name; }
    /**
     * Get the principal type
     */
    principal_type type() const override { return _type; }
    /**
     * Check whether this is a 'wildcard' principal.
     *
     * Note that this is type()-dependent. A principal of type 'role' is
     * always exempt from wildcard matching.
     */
    bool wildcard() const {
        switch (_type) {
        case principal_type::user:
        case principal_type::ephemeral_user:
            return _name == "*";
        case principal_type::role:
        case principal_type::group:
            return false;
        }
    }

    // Needed for ADL serialization
    const ss::sstring& name() const { return _name; }

    auto serde_fields() { return std::tie(_type, _name); }

private:
    principal_type _type;
    ss::sstring _name;
};

/// Convert ACL group principals to audit group objects for inclusion in audit
/// logs. Filters to only principals with type::group and converts them to
/// audit::group with type idp_group.
chunked_vector<::security::audit::group>
acl_principals_to_audit_groups(const chunked_vector<acl_principal>& principals);

} // namespace security

template<>
struct fmt::formatter<security::acl_principal_base> {
    constexpr auto parse(fmt::format_parse_context& ctx)
      -> decltype(ctx.begin()) {
        auto it = ctx.begin();
        auto end = ctx.end();

        // Parse format specifiers:
        // 'l' - logging and audit logging
        // 'a' - kafka and schema registry API
        if (it != end && (*it == 'l' || *it == 'a')) {
            presentation = *it++;
        }

        if (it != end && *it != '}') {
            throw fmt::format_error("invalid format specifier for principal");
        }

        return it;
    }

    template<typename FormatContext>
    auto format(const security::acl_principal_base& p, FormatContext& ctx) const
      -> decltype(ctx.out()) {
        switch (presentation) {
        case 'a': // User:Alice
            switch (p.type()) {
            case security::principal_type::user:
                return fmt::format_to(ctx.out(), "User:{}", p.name_view());
            case security::principal_type::ephemeral_user:
                return fmt::format_to(
                  ctx.out(), "Ephemeral user:{}", p.name_view());
            case security::principal_type::role:
                return fmt::format_to(
                  ctx.out(), "RedpandaRole:{}", p.name_view());
            case security::principal_type::group:
                return fmt::format_to(ctx.out(), "Group:{}", p.name_view());
            }
        case 'l': // type {user} name {Alice}
        default:
            return fmt::format_to(
              ctx.out(), "type {{{}}} name {{{}}}", p.type(), p.name_view());
        }
    }

    char presentation{'l'};
};

template<>
struct fmt::formatter<security::acl_principal>
  : fmt::formatter<security::acl_principal_base> {};

namespace security {

/**
 * Concrete instance of a Kafka principal.
 *
 * This implementation does _not_ own the memory for its name. Use
 * with care, similarly to a string_view, only when the lifetime of
 * the view is known not to exceed the referenced principal.
 *
 */
class acl_principal_view final : public acl_principal_base {
public:
    acl_principal_view() = delete;
    acl_principal_view(principal_type type, std::string_view name)
      : _type(type)
      , _name(name) {}
    explicit acl_principal_view(const acl_principal& p)
      : _type(p.type())
      , _name(p.name_view()) {}

    /**
     * Get a view to the principal name.
     */
    std::string_view name_view() const override { return _name; }
    /**
     * Get the principal type
     */
    principal_type type() const override { return _type; }

private:
    principal_type _type;
    std::string_view _name;
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

    friend bool
    operator==(const resource_pattern&, const resource_pattern&) = default;

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
    explicit acl_host(ss::sstring host)
      : _addr(std::move(host)) {}

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
 *
 * Note: See acl_binding_filter::serde_write or write_v0 for history on how
 * serde version 0 of this field was serialized
 */
class resource_pattern_filter
  : public serde::envelope<
      resource_pattern_filter,
      serde::version<1>,
      serde::compat_version<1>> {
public:
    enum class serialized_pattern_type {
        literal = 0,
        prefixed = 1,
        match = 2,
    };

    enum class resource_subsystem : uint8_t {
        kafka = 0,
        schema_registry = 1,
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

    struct pattern_match
      : public serde::
          envelope<pattern_match, serde::version<0>, serde::compat_version<0>> {
        friend bool
        operator==(const pattern_match&, const pattern_match&) = default;

        friend std::ostream& operator<<(std::ostream&, const pattern_match&);

        auto serde_fields() { return std::tie(); }
    };
    using pattern_filter_type = serde::variant<pattern_type, pattern_match>;

    resource_pattern_filter() = default;

    resource_pattern_filter(
      std::optional<resource_type> type,
      std::optional<ss::sstring> name,
      std::optional<pattern_filter_type> pattern,
      resource_subsystem subsystem = resource_subsystem::kafka)
      : _resource(type)
      , _name(std::move(name))
      , _pattern(pattern)
      , _subsystem(subsystem) {}

    // NOLINTNEXTLINE(hicpp-explicit-conversions)
    resource_pattern_filter(const resource_pattern& resource)
      : resource_pattern_filter(
          resource.resource(), resource.name(), resource.pattern()) {}

    /*
     * A filter that matches any resource.
     */
    static const resource_pattern_filter&
    any(resource_subsystem subsystem = resource_subsystem::kafka) {
        static const resource_pattern_filter k_filter(
          std::nullopt, std::nullopt, std::nullopt, resource_subsystem::kafka);
        static const resource_pattern_filter sr_filter(
          std::nullopt,
          std::nullopt,
          std::nullopt,
          resource_subsystem::schema_registry);
        return subsystem == resource_subsystem::kafka ? k_filter : sr_filter;
    }

    bool matches(const resource_pattern& pattern) const;
    std::vector<resource_pattern> to_resource_patterns() const;

    std::optional<resource_type> resource() const { return _resource; }
    const std::optional<ss::sstring>& name() const { return _name; }
    std::optional<pattern_filter_type> pattern() const { return _pattern; }
    resource_subsystem subsystem() const { return _subsystem; }

    template<typename H>
    friend H AbslHashValue(H h, const pattern_match&) {
        return H::combine(std::move(h), 0x1B3A5CD7); // random number
    }
    template<typename H>
    friend H AbslHashValue(H h, const resource_pattern_filter& f) {
        return H::combine(std::move(h), f._resource, f._name, f._pattern);
    }

    friend void read_nested_v0(
      iobuf_parser& in,
      resource_pattern_filter& filter,
      const size_t bytes_left_limit);

    friend void write_v0(iobuf& out, resource_pattern_filter filter);

    friend bool operator==(
      const resource_pattern_filter&, const resource_pattern_filter&) = default;

    friend std::ostream&
    operator<<(std::ostream&, const resource_pattern_filter&);

    auto serde_fields() {
        return std::tie(_resource, _name, _pattern, _subsystem);
    }

private:
    std::optional<resource_type> _resource;
    std::optional<ss::sstring> _name;
    std::optional<pattern_filter_type> _pattern;
    resource_subsystem _subsystem{resource_subsystem::kafka};
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
      , _host(host)
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

    template<typename H>
    friend H AbslHashValue(H h, const acl_entry_filter& f) {
        return H::combine(
          std::move(h), f._principal, f._host, f._operation, f._permission);
    }

    friend bool
    operator==(const acl_entry_filter&, const acl_entry_filter&) = default;

    friend std::ostream& operator<<(std::ostream&, const acl_entry_filter&);

private:
    std::optional<acl_principal> _principal;
    std::optional<acl_host> _host;
    std::optional<acl_operation> _operation;
    std::optional<acl_permission> _permission;
};

/*
 * A filter for matching ACL bindings.
 *
 * Note: see acl_binding_filter::serde_write for context on serde version
 * history
 */
class acl_binding_filter
  : public serde::envelope<
      acl_binding_filter,
      serde::version<1>,
      serde::compat_version<0>> {
public:
    acl_binding_filter() = default;
    acl_binding_filter(resource_pattern_filter pattern, acl_entry_filter acl)
      : _pattern(std::move(pattern))
      , _acl(std::move(acl)) {}

    template<typename H>
    friend H AbslHashValue(H h, const acl_binding_filter& f) {
        return H::combine(std::move(h), f._pattern, f._acl);
    }

    /*
     * A filter that matches any ACL binding for the given subsystem.
     */
    static const acl_binding_filter& any(
      resource_pattern_filter::resource_subsystem subsystem
      = resource_pattern_filter::resource_subsystem::kafka) {
        static const acl_binding_filter k_filter(
          resource_pattern_filter::any(
            resource_pattern_filter::resource_subsystem::kafka),
          acl_entry_filter::any());
        static const acl_binding_filter sr_filter(
          resource_pattern_filter::any(
            resource_pattern_filter::resource_subsystem::schema_registry),
          acl_entry_filter::any());

        return subsystem == resource_pattern_filter::resource_subsystem::kafka
                 ? k_filter
                 : sr_filter;
    }

    bool matches(const acl_binding& binding) const {
        return _pattern.matches(binding.pattern())
               && _acl.matches(binding.entry());
    }

    const resource_pattern_filter& pattern() const { return _pattern; }
    const acl_entry_filter& entry() const { return _acl; }

    friend bool
    operator==(const acl_binding_filter&, const acl_binding_filter&) = default;

    friend std::ostream& operator<<(std::ostream&, const acl_binding_filter&);

    void serde_write(iobuf&) const;
    void serde_read(iobuf_parser&, const serde::header&);

    // Helpers for serde compatibility testing
    // They read/write the full object, including the header
    void testing_serde_full_write_v0(iobuf&) const;
    void testing_serde_full_read_v0(iobuf_parser&, const std::size_t);
    void testing_serde_full_write_v2(iobuf&) const;
    void testing_serde_full_read_v2(iobuf_parser&, const std::size_t);

private:
    resource_pattern_filter _pattern;
    acl_entry_filter _acl;
};

/// Name of the principal the kafka client for auditing will be using
inline const acl_principal audit_principal{
  principal_type::ephemeral_user, "__auditing"};

inline const acl_principal schema_registry_principal{
  principal_type::ephemeral_user, "__schema_registry"};

namespace testing {

struct acl_binding_filter_v0 : public acl_binding_filter {
    static constexpr auto redpanda_serde_version = serde::version_t{0};
    static constexpr auto redpanda_serde_compat_version = serde::version_t{0};
};

struct acl_binding_filter_v2 : public acl_binding_filter {
    static constexpr auto redpanda_serde_version = serde::version_t{2};
    static constexpr auto redpanda_serde_compat_version = serde::version_t{0};
};

} // namespace testing

/**
 *  list of acl operations for specific resource
 */
template<typename T>
const std::vector<acl_operation>& get_allowed_operations();

} // namespace security
