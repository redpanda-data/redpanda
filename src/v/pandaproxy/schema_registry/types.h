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

#include "kafka/protocol/errors.h"
#include "model/metadata.h"
#include "outcome.h"
#include "seastarx.h"
#include "utils/named_type.h"
#include "utils/string_switch.h"

#include <seastar/core/sstring.hh>
#include <seastar/util/bool_class.hh>

#include <avro/ValidSchema.hh>

#include <iosfwd>
#include <type_traits>

namespace pandaproxy::schema_registry {

using permanent_delete = ss::bool_class<struct delete_tag>;
using include_deleted = ss::bool_class<struct include_deleted_tag>;
using is_deleted = ss::bool_class<struct is_deleted_tag>;
using default_to_global = ss::bool_class<struct default_to_global_tag>;

template<typename E>
std::enable_if_t<std::is_enum_v<E>, std::optional<E>>
  from_string_view(std::string_view);

enum class schema_type { avro = 0, json, protobuf };

constexpr std::string_view to_string_view(schema_type e) {
    switch (e) {
    case schema_type::avro:
        return "AVRO";
    case schema_type::json:
        return "JSON";
    case schema_type::protobuf:
        return "PROTOBUF";
    }
    return "{invalid}";
}
template<>
constexpr std::optional<schema_type>
from_string_view<schema_type>(std::string_view sv) {
    return string_switch<std::optional<schema_type>>(sv)
      .match(to_string_view(schema_type::avro), schema_type::avro)
      .match(to_string_view(schema_type::json), schema_type::json)
      .match(to_string_view(schema_type::protobuf), schema_type::protobuf)
      .default_match(std::nullopt);
}

std::ostream& operator<<(std::ostream& os, const schema_type& v);

///\brief A subject is the name under which a schema is registered.
///
/// Typically it will be "<topic>-key" or "<topic>-value".
using subject = named_type<ss::sstring, struct subject_tag>;
static const subject invalid_subject{};

///\brief The version of the schema registered with a subject.
///
/// A subject may evolve its schema over time. Each version is associated with a
/// schema_id.
using schema_version = named_type<int32_t, struct schema_version_tag>;
static constexpr schema_version invalid_schema_version{-1};

struct schema_reference {
    friend bool
    operator==(const schema_reference& lhs, const schema_reference& rhs)
      = default;

    friend std::ostream&
    operator<<(std::ostream& os, const schema_reference& ref);

    ss::sstring name;
    subject sub{invalid_subject};
    schema_version version{invalid_schema_version};
};

///\brief Definition of a schema and its type.
template<typename Tag>
class typed_schema_definition {
public:
    using tag = Tag;
    using raw_string = named_type<ss::sstring, tag>;
    using references = std::vector<schema_reference>;

    template<typename T>
    typed_schema_definition(T&& def, schema_type type)
      : _def{ss::sstring{std::forward<T>(def)}}
      , _type{type}
      , _refs{} {}

    template<typename T>
    typed_schema_definition(T&& def, schema_type type, references refs)
      : _def{ss::sstring{std::forward<T>(def)}}
      , _type{type}
      , _refs{std::move(refs)} {}

    friend bool operator==(
      const typed_schema_definition& lhs, const typed_schema_definition& rhs)
      = default;

    friend std::ostream&
    operator<<(std::ostream& os, const typed_schema_definition&);

    schema_type type() const { return _type; }

    const raw_string& raw() const& { return _def; }
    raw_string raw() && { return std::move(_def); }

    const references& refs() const& { return _refs; }
    references refs() && { return std::move(_refs); }

private:
    raw_string _def;
    schema_type _type{schema_type::avro};
    references _refs;
};

///\brief An unvalidated definition of the schema and its type.
///
/// This comes from the user and should be considered as potentially
/// ill-formed.
using unparsed_schema_definition
  = typed_schema_definition<struct unparsed_schema_defnition_tag>;

///\brief A canonical definition of the schema and its type.
///
/// This form is stored on the topic and returned to the user.
using canonical_schema_definition
  = typed_schema_definition<struct canonical_schema_definition_tag>;

static const unparsed_schema_definition invalid_schema_definition{
  "", schema_type::avro};

///\brief The definition of an avro schema.
class avro_schema_definition {
public:
    explicit avro_schema_definition(
      avro::ValidSchema vs, canonical_schema_definition::references refs);

    canonical_schema_definition::raw_string raw() const;
    canonical_schema_definition::references const& refs() const {
        return _refs;
    };

    const avro::ValidSchema& operator()() const;

    friend bool operator==(
      const avro_schema_definition& lhs, const avro_schema_definition& rhs);

    friend std::ostream&
    operator<<(std::ostream& os, const avro_schema_definition& rhs);

    constexpr schema_type type() const { return schema_type::avro; }

    explicit operator canonical_schema_definition() const {
        return {raw(), type()};
    }

    ss::sstring name() const;

private:
    avro::ValidSchema _impl;
    canonical_schema_definition::references _refs;
};

class protobuf_schema_definition {
public:
    struct impl;
    using pimpl = ss::shared_ptr<const impl>;

    explicit protobuf_schema_definition(
      pimpl p, canonical_schema_definition::references refs)
      : _impl{std::move(p)}
      , _refs(std::move(refs)) {}

    canonical_schema_definition::raw_string raw() const;
    canonical_schema_definition::references const& refs() const {
        return _refs;
    };

    const impl& operator()() const { return *_impl; }

    friend bool operator==(
      const protobuf_schema_definition& lhs,
      const protobuf_schema_definition& rhs);

    friend std::ostream&
    operator<<(std::ostream& os, const protobuf_schema_definition& rhs);

    constexpr schema_type type() const { return schema_type::protobuf; }

    explicit operator canonical_schema_definition() const {
        return {raw(), type(), refs()};
    }

    ::result<ss::sstring, kafka::error_code>
    name(std::vector<int> const& fields) const;

private:
    pimpl _impl;
    canonical_schema_definition::references _refs;
};

///\brief A schema that has been validated.
class valid_schema {
    using impl
      = std::variant<avro_schema_definition, protobuf_schema_definition>;

    template<typename T>
    using disable_if_valid_schema = std::
      enable_if_t<!std::is_same_v<std::remove_cvref_t<T>, valid_schema>, int>;

    template<typename T>
    using enable_if_can_construct_impl = std::
      enable_if_t<std::is_constructible_v<impl, std::remove_cvref_t<T>>, int>;

public:
    ///\brief Converting constructor from variant types
    template<
      typename T,
      disable_if_valid_schema<T> = 0,
      enable_if_can_construct_impl<T> = 0>
    valid_schema(T&& def)
      : _impl{std::forward<T>(def)} {}

    template<typename V, typename... Args>
    decltype(auto) visit(V&& v, Args... args) const& {
        return std::visit(
          std::forward<V>(v), _impl, std::forward<Args>(args)...);
    }

    template<typename V, typename... Args>
    decltype(auto) visit(V&& v, Args... args) && {
        return std::visit(
          std::forward<V>(v), std::move(_impl), std::forward<Args>(args)...);
    }

    schema_type type() const {
        return visit([](const auto& def) { return def.type(); });
    }

    unparsed_schema_definition::raw_string raw() const& {
        return visit([](auto&& def) {
            return unparsed_schema_definition::raw_string{def.raw()()};
        });
    }

    unparsed_schema_definition::raw_string raw() && {
        return visit([](auto def) {
            return unparsed_schema_definition::raw_string{
              std::move(def).raw()()};
        });
    }

    friend std::ostream& operator<<(std::ostream& os, const valid_schema& def) {
        def.visit([&os](const auto& def) { os << def; });
        return os;
    }

private:
    impl _impl;
};

///\brief Globally unique identifier for a schema.
using schema_id = named_type<int32_t, struct schema_id_tag>;
static constexpr schema_id invalid_schema_id{-1};

struct subject_version {
    subject_version(subject s, schema_version v)
      : sub{std::move(s)}
      , version{v} {}
    subject sub;
    schema_version version;
};

// Very similar to topic_key_type, separate to avoid intermingling storage code
enum class seq_marker_key_type { invalid = 0, schema, delete_subject, config };

constexpr std::string_view to_string_view(seq_marker_key_type v) {
    switch (v) {
    case seq_marker_key_type::schema:
        return "schema";
        break;
    case seq_marker_key_type::delete_subject:
        return "delete_subject";
        break;
    case seq_marker_key_type::config:
        return "config";
        break;
    default:
        return "invalid";
    }
}

// Record the sequence+node where updates were made to a subject,
// in order to later generate tombstone keys when doing a permanent
// deletion.
struct seq_marker {
    std::optional<model::offset> seq;
    std::optional<model::node_id> node;
    schema_version version;
    seq_marker_key_type key_type{seq_marker_key_type::invalid};

    friend std::ostream& operator<<(std::ostream& os, const seq_marker& v);
};

///\brief A schema with its subject
template<typename Tag>
class typed_schema {
public:
    using tag = Tag;
    using schema_definition = typed_schema_definition<tag>;

    typed_schema() = default;

    typed_schema(subject sub, schema_definition def)
      : _sub{std::move(sub)}
      , _def{std::move(def)} {}

    friend bool operator==(const typed_schema& lhs, const typed_schema& rhs)
      = default;

    friend std::ostream& operator<<(std::ostream& os, const typed_schema& ref);

    const subject& sub() const& { return _sub; }
    subject sub() && { return std::move(_sub); }

    schema_type type() const { return _def.type(); }

    const schema_definition& def() const& { return _def; }
    schema_definition def() && { return std::move(_def); }

private:
    subject _sub{invalid_subject};
    schema_definition _def{"", schema_type::avro};
};

using unparsed_schema = typed_schema<unparsed_schema_definition::tag>;
using canonical_schema = typed_schema<canonical_schema_definition::tag>;

///\brief Complete description of a subject and schema for a version.
struct subject_schema {
    canonical_schema schema;
    schema_version version{invalid_schema_version};
    schema_id id{invalid_schema_id};
    is_deleted deleted{false};
};

enum class compatibility_level {
    none = 0,
    backward,
    backward_transitive,
    forward,
    forward_transitive,
    full,
    full_transitive,
};

constexpr std::string_view to_string_view(compatibility_level v) {
    switch (v) {
    case compatibility_level::none:
        return "NONE";
    case compatibility_level::backward:
        return "BACKWARD";
    case compatibility_level::backward_transitive:
        return "BACKWARD_TRANSITIVE";
    case compatibility_level::forward:
        return "FORWARD";
    case compatibility_level::forward_transitive:
        return "FORWARD_TRANSITIVE";
    case compatibility_level::full:
        return "FULL";
    case compatibility_level::full_transitive:
        return "FULL_TRANSITIVE";
    }
    return "{invalid}";
}
template<>
constexpr std::optional<compatibility_level>
from_string_view<compatibility_level>(std::string_view sv) {
    return string_switch<std::optional<compatibility_level>>(sv)
      .match(
        to_string_view(compatibility_level::none), compatibility_level::none)
      .match(
        to_string_view(compatibility_level::backward),
        compatibility_level::backward)
      .match(
        to_string_view(compatibility_level::backward_transitive),
        compatibility_level::backward_transitive)
      .match(
        to_string_view(compatibility_level::forward),
        compatibility_level::forward)
      .match(
        to_string_view(compatibility_level::forward_transitive),
        compatibility_level::forward_transitive)
      .match(
        to_string_view(compatibility_level::full), compatibility_level::full)
      .match(
        to_string_view(compatibility_level::full_transitive),
        compatibility_level::full_transitive)
      .default_match(std::nullopt);
}

} // namespace pandaproxy::schema_registry
