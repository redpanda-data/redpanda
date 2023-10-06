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
#include "json/json.h"

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/noncopyable_function.hh>

#include <fmt/format.h>
#include <yaml-cpp/node/node.h>

#include <string>

namespace cluster {
struct topic_configuration;
} // namespace cluster

namespace config {

struct constraint_validator_t {
    virtual bool validate(const cluster::topic_configuration&) const = 0;
    virtual void clamp(cluster::topic_configuration&) const = 0;
    virtual std::optional<ss::sstring> check(const ss::sstring&) const = 0;
    virtual void encode_yaml(YAML::Node&) const = 0;
    virtual void to_json(json::Writer<json::StringBuffer>&) const = 0;
    virtual void print(std::ostream&) const = 0;
    virtual ~constraint_validator_t() = default;
};

using constraint_enabled_t = ss::bool_class<struct constraint_enabled_tag>;

ss::shared_ptr<constraint_validator_t> constraint_validator_map(
  ss::sstring& name,
  std::optional<ss::sstring> min_str_opt,
  std::optional<ss::sstring> max_str_opt,
  constraint_enabled_t enabled);

template<typename T>
struct constraint_validator_range : public constraint_validator_t {
    using extractor_t =
      typename ss::noncopyable_function<T(const cluster::topic_configuration&)>;
    using clamper_t = typename ss::noncopyable_function<void(
      cluster::topic_configuration&, const T&)>;

    std::optional<T> min;
    std::optional<T> max;
    extractor_t value_extractor;
    clamper_t clamper;

    constraint_validator_range(
      std::optional<T> min,
      std::optional<T> max,
      extractor_t value_extractor,
      clamper_t clamper)
      : min{std::move(min)}
      , max{std::move(max)}
      , value_extractor{std::move(value_extractor)}
      , clamper{std::move(clamper)} {}

    bool
    validate(const cluster::topic_configuration& topic_cfg) const override {
        T topic_val = value_extractor(topic_cfg);

        if (min && *min > topic_val) {
            return false;
        }

        if (max && *max < topic_val) {
            return false;
        }

        return true;
    }

    void clamp(cluster::topic_configuration& topic_cfg) const override {
        T topic_val = value_extractor(topic_cfg);
        if (min) {
            topic_val = std::max(*min, topic_val);
        }
        if (max) {
            topic_val = std::min(*max, topic_val);
        }
        clamper(topic_cfg, topic_val);
    }

    std::optional<ss::sstring>
    check(const ss::sstring& property_name) const override {
        if ((min && max) && (*min > *max)) {
            return fmt::format(
              "Constraints failure: min > max: config {}", property_name);
        }
        return std::nullopt;
    }

    void encode_yaml(YAML::Node& n) const override {
        if (min) {
            n["min"] = *min;
        }

        if (max) {
            n["max"] = *max;
        }
    }

    void to_json(json::Writer<json::StringBuffer>& w) const override {
        w.Key("min");
        rjson_serialize<T>(w, min);

        w.Key("max");
        rjson_serialize<T>(w, max);
    }

    void print(std::ostream& os) const override {
        os << fmt::format("min: {}, max: {}", min, max);
    }
};

/*
 * When enabled=True, this validator will check topic-level properties against
 * the corresponding cluster-level property.
 */
template<typename T>
struct constraint_validator_enabled : public constraint_validator_t {
    using extractor_t =
      typename ss::noncopyable_function<T(const cluster::topic_configuration&)>;
    using clamper_t = typename ss::noncopyable_function<void(
      cluster::topic_configuration&, const T&)>;

    constraint_enabled_t enabled;
    binding<T> original_property;
    extractor_t value_extractor;
    clamper_t clamper;

    constraint_validator_enabled(
      constraint_enabled_t enabled,
      binding<T> original,
      extractor_t value_extractor,
      clamper_t clamper)
      : enabled{std::move(enabled)}
      , original_property{std::move(original)}
      , value_extractor{std::move(value_extractor)}
      , clamper{std::move(clamper)} {}

    bool
    validate(const cluster::topic_configuration& topic_cfg) const override {
        if (enabled == constraint_enabled_t::yes) {
            T topic_val = value_extractor(topic_cfg);
            return topic_val == original_property();
        } else {
            return true;
        }
    }

    void clamp(cluster::topic_configuration& topic_cfg) const override {
        if (enabled == constraint_enabled_t::yes) {
            T topic_val = value_extractor(topic_cfg);
            topic_val = original_property();
            clamper(topic_cfg, topic_val);
        }
    }

    std::optional<ss::sstring> check(const ss::sstring&) const override {
        return std::nullopt;
    }

    void encode_yaml(YAML::Node& n) const override {
        n["enabled"] = enabled == constraint_enabled_t::yes;
    }

    void to_json(json::Writer<json::StringBuffer>& w) const override {
        w.Key("enabled");
        w.Bool(enabled == constraint_enabled_t::yes);
    }

    void print(std::ostream& os) const override {
        os << "enabled: " << enabled;
    }
};

enum class constraint_type {
    none = 0,
    restrikt = 1,
    clamp = 2,
};

std::string_view to_string_view(constraint_type type);

template<>
std::optional<constraint_type>
from_string_view<constraint_type>(std::string_view sv);

struct constraint_t {
    ss::sstring name;
    constraint_type type;
    ss::shared_ptr<constraint_validator_t> validator;

    constraint_t() = default;
    constraint_t(
      ss::sstring name,
      constraint_type type,
      ss::shared_ptr<constraint_validator_t> validator);

    bool validate(const cluster::topic_configuration& topic_cfg) const;
    void clamp(cluster::topic_configuration& topic_cfg) const;
    std::optional<ss::sstring> check() const;

    friend bool operator==(const constraint_t&, const constraint_t&) = default;

    friend std::ostream&
    operator<<(std::ostream& os, const constraint_t& constraint);
};

std::vector<constraint_t> predefined_constraints();

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
