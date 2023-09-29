/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "config/convert.h"
#include "config/from_string_view.h"
#include "config/property.h"
#include "json/_include_first.h"
#include "json/stringbuffer.h"
#include "json/writer.h"

#include <seastar/core/sstring.hh>
#include <seastar/util/noncopyable_function.hh>

#include <fmt/format.h>
#include <yaml-cpp/node/node.h>

#include <memory>
#include <string>

namespace cluster {
struct topic_configuration;
} // namespace cluster

namespace config {

struct constraint_validator_t {
    virtual bool validate(const cluster::topic_configuration&) const = 0;
    virtual void encode_yaml(YAML::Node&) const = 0;
    virtual void rjson_serialize(json::Writer<json::StringBuffer>&) const = 0;
    virtual void print(std::ostream&) const = 0;
    virtual ~constraint_validator_t() = default;
};

template<typename T>
struct constraint_validator_range : public constraint_validator_t {
    using handle_t =
      typename ss::noncopyable_function<T(const cluster::topic_configuration&)>;

    std::optional<T> min;
    std::optional<T> max;
    handle_t handler;

    constraint_validator_range(
      std::optional<T> min, std::optional<T> max, handle_t handle)
      : min{std::move(min)}
      , max{std::move(max)}
      , handler{std::move(handle)} {}

    virtual bool validate(const cluster::topic_configuration& topic_cfg) const {
        T topic_val = handler(topic_cfg);

        if (min && *min > topic_val) {
            return false;
        }

        if (max && *max < topic_val) {
            return false;
        }

        return true;
    }

    virtual void encode_yaml(YAML::Node& n) const {
        if (min) {
            n["min"] = *min;
        }

        if (max) {
            n["max"] = *max;
        }
    }

    virtual void rjson_serialize(json::Writer<json::StringBuffer>& w) const {
        w.Key("min");
        if (min) {
            w.String(fmt::format("{}", *min));
        } else {
            w.String("null");
        }

        w.Key("max");
        if (max) {
            w.String(fmt::format("{}", *max));
        } else {
            w.String("null");
        }
    }

    virtual void print(std::ostream& os) const {
        os << "min:";
        if (min) {
            os << *min;
        } else {
            os << "null";
        }

        os << ", max:";
        if (max) {
            os << *max;
        } else {
            os << "null";
        }
    }
};

using constraint_enabled_t = ss::bool_class<struct constraint_enabled_tag>;
template<typename T>
struct constraint_validator_enabled : public constraint_validator_t {
    using handle_t =
      typename ss::noncopyable_function<T(const cluster::topic_configuration&)>;

    constraint_enabled_t enabled;
    binding<T> original_property;
    handle_t handler;

    constraint_validator_enabled(
      constraint_enabled_t enabled, binding<T> original, handle_t handle)
      : enabled{std::move(enabled)}
      , original_property{std::move(original)}
      , handler{std::move(handle)} {}

    virtual bool validate(const cluster::topic_configuration& topic_cfg) const {
        T topic_val = handler(topic_cfg);
        return topic_val == original_property();
    }

    virtual void encode_yaml(YAML::Node& n) const {
        n["enabled"] = enabled == constraint_enabled_t::yes ? true : false;
    }

    virtual void rjson_serialize(json::Writer<json::StringBuffer>& w) const {
        w.Key("enabled");
        enabled == constraint_enabled_t::yes ? w.String("true")
                                             : w.String("false");
    }

    virtual void print(std::ostream& os) const {
        os << "enabled:"
           << (enabled == constraint_enabled_t::yes ? "true" : "false");
    }
};

std::shared_ptr<constraint_validator_t> constraint_validator_map(
  ss::sstring& name,
  std::optional<ss::sstring> min_str_opt,
  std::optional<ss::sstring> max_str_opt,
  constraint_enabled_t enabled);

enum class constraint_type {
    none = 0,
    _restrict = 1,
    clamp = 2,
};

std::string_view to_string_view(constraint_type type);

template<>
std::optional<constraint_type>
from_string_view<constraint_type>(std::string_view sv);

struct constraint_t {
    ss::sstring name;
    constraint_type type;
    std::shared_ptr<constraint_validator_t> validator;

    constraint_t()
      : name{"undefined"}
      , type{constraint_type::none}
      , validator{nullptr} {}

    constraint_t(
      ss::sstring name,
      constraint_type type,
      std::shared_ptr<constraint_validator_t> validator)
      : name{name}
      , type{type}
      , validator{validator} {}

    bool validate(const cluster::topic_configuration& topic_cfg) const {
        if (validator != nullptr) {
            return validator->validate(topic_cfg);
        }
        return true;
    }

    friend bool operator==(const constraint_t&, const constraint_t&) = default;

    friend std::ostream&
    operator<<(std::ostream& os, const constraint_t& constraint) {
        os << "name:" << constraint.name << ","
           << "type:" << to_string_view(constraint.type) << ",";
        if (constraint.validator != nullptr) {
            constraint.validator->print(os);
        }
        return os;
    }
};

namespace detail {

template<>
consteval std::string_view property_type_name<constraint_t>() {
    return "config::constraint_t";
}

} // namespace detail

} // namespace config

namespace YAML {
template<>
struct convert<config::constraint_t> {
    using type = config::constraint_t;
    static Node encode(const type& rhs);
    static bool decode(const Node& node, type& rhs);
};

} // namespace YAML

namespace json {

void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const config::constraint_t& constraint);

} // namespace json
