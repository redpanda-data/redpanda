/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "config/property.h"
#include "seastarx.h"
#include "utils/to_string.h"

#include <fmt/format.h>

#include <unordered_map>

namespace config {
class config_store {
public:
    bool contains(std::string_view name) { return _properties.contains(name); }

    base_property& get(const std::string_view& name) {
        return *_properties.at(name);
    }

    virtual void read_yaml(
      const YAML::Node& root_node,
      const std::set<std::string_view> ignore_missing = {}) {
        for (auto const& [name, property] : _properties) {
            if (property->is_required() == required::no) {
                continue;
            }
            ss::sstring name_str(name.data());
            if (!root_node[name_str]) {
                throw std::invalid_argument(
                  fmt::format("Property {} is required", name));
            }
        }

        for (auto const& node : root_node) {
            auto name = node.first.as<ss::sstring>();
            auto found = _properties.find(name);
            if (found == _properties.end()) {
                if (!ignore_missing.contains(name)) {
                    throw std::invalid_argument(
                      fmt::format("Unknown property {}", name));
                }
            } else {
                found->second->set_value(node.second);
            }
        }
    }

    template<typename Func>
    void for_each(Func&& f) const {
        for (auto const& [_, property] : _properties) {
            f(*property);
        }
    }

    const std::vector<validation_error> validate() {
        std::vector<validation_error> errors;
        for_each([&errors](const base_property& p) {
            if (auto err = p.validate()) {
                errors.push_back(std::move(*err));
            }
        });
        return errors;
    }

    void to_json(rapidjson::Writer<rapidjson::StringBuffer>& w, redact_secrets redact) const {
        w.StartObject();

        for (const auto& [name, property] : _properties) {
            w.Key(name.data(), name.size());
            property->to_json(w, redact);
        }

        w.EndObject();
    }

    std::set<std::string_view> property_names() const {
        std::set<std::string_view> result;
        for (const auto& i : _properties) {
            result.insert(i.first);
        }

        return result;
    }

    virtual ~config_store() noexcept = default;

private:
    friend class base_property;
    std::unordered_map<std::string_view, base_property*> _properties;
};

inline YAML::Node to_yaml(const config_store& cfg, redact_secrets redact) {
    rapidjson::StringBuffer buf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
    cfg.to_json(writer, redact);
    return YAML::Load(buf.GetString());
}
}; // namespace config

namespace std {
template<typename... Types>
static inline ostream& operator<<(ostream& o, const config::config_store& c) {
    o << "{ ";
    c.for_each([&o](const auto& property) { o << property << " "; });
    o << "}";
    return o;
}
} // namespace std
