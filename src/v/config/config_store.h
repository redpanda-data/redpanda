/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/seastarx.h"
#include "config/logger.h"
#include "config/property.h"
#include "json/rjson_writer_fwd.h"
#include "utils/to_string.h"

#include <fmt/format.h>

#include <map>
#include <set>
#include <unordered_map>

namespace YAML {
class Node;
}

namespace config {
class config_store {
public:
    bool contains(std::string_view name) {
        return _properties.contains(name) || _aliases.contains(name);
    }

    base_property& get(const std::string_view& name) {
        if (auto found = _properties.find(name); found != _properties.end()) {
            return *(found->second);
        } else if (auto found = _aliases.find(name); found != _aliases.end()) {
            return *(found->second);
        } else {
            throw std::out_of_range(fmt::format("Property {} not found", name));
        }
    }

    using error_map_t = std::map<ss::sstring, ss::sstring>;

    virtual error_map_t read_yaml(const YAML::Node& root_node);

    template<typename Func>
    void for_each(Func&& f) const {
        for (const auto& [_, property] : _properties) {
            f(*property);
        }
    }

    void to_json(
      json::rjson_writer& w,
      redact_secrets redact,
      std::optional<std::function<bool(base_property&)>> filter
      = std::nullopt) const;

    void to_json_single_key(
      json::rjson_writer& w, redact_secrets redact, std::string_view key);

    void to_json_for_metrics(json::rjson_writer& w);

    std::set<std::string_view> property_names() const {
        std::set<std::string_view> result;
        for (const auto& i : _properties) {
            result.insert(i.first);
        }

        return result;
    }

    std::set<std::string_view> property_aliases() const {
        std::set<std::string_view> result;
        for (const auto& i : _aliases) {
            result.insert(i.first);
        }

        return result;
    }

    std::set<std::string_view> property_names_and_aliases() const {
        auto all = property_names();
        all.merge(property_aliases());
        return all;
    }

    friend std::ostream&
    operator<<(std::ostream& o, const config::config_store& c) {
        o << "{ ";
        c.for_each([&o](const auto& property) { o << property << " "; });
        o << "}";
        return o;
    }

    void notify_original_version(legacy_version ov) {
        for (const auto& [name, property] : _properties) {
            property->notify_original_version(ov);
        }
    }

    virtual ~config_store() noexcept = default;

    virtual ss::sstring store_name() const { return "config_store"; }

private:
    friend class base_property;
    std::unordered_map<std::string_view, base_property*> _properties;

    // If a property has some aliases for backward compat, they are tracked
    // here: a property must appear at least in _properties, and may appear
    // 0..n times in _aliases
    std::unordered_map<std::string_view, base_property*> _aliases;
};

YAML::Node to_yaml(const config_store& cfg, redact_secrets redact);

}; // namespace config
