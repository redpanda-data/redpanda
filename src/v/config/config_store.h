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

#include "config/property.h"
#include "seastarx.h"
#include "utils/to_string.h"

#include <fmt/format.h>

#include <unordered_map>

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

    /**
     * Missing or invalid properties whose metadata specifies `required=true`
     * are fatal errors, raised as std::invalid_argument.
     *
     * Other validation errors on property values are returned in a map
     * of property name to error message.  This includes malformed YAML,
     * bad YAML type, or an error flagged by the property's validator hook.
     *
     * @param root_node
     * @param ignore_missing Tolerate extra values in the config if they are
     *        contained in this set -- this is for reading old configs that
     *        mix node & cluster config properties.
     * @return map of property name to error.  Empty on clean load.
     */
    error_map_t read_yaml(
      const YAML::Node& root_node,
      const std::set<std::string_view> ignore_missing = {}) {
        error_map_t errors;

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
            auto* prop = [&]() -> base_property* {
                auto primary = _properties.find(name);
                if (primary != _properties.end()) {
                    return primary->second;
                }

                auto secondary = _aliases.find(name);
                if (secondary != _aliases.end()) {
                    return secondary->second;
                }

                if (ignore_missing.contains(name)) {
                    return nullptr;
                }

                throw std::invalid_argument(
                  fmt::format("Unknown property {}", name));
            }();

            if (prop == nullptr) {
                continue;
            }
            bool ok = false;
            try {
                auto validation_err = prop->validate(node.second);
                if (validation_err.has_value()) {
                    errors[name] = fmt::format(
                      "Validation error: {}",
                      validation_err.value().error_message());
                }

                prop->set_value(node.second);
                ok = true;
            } catch (YAML::InvalidNode const& e) {
                errors[name] = fmt::format("Invalid syntax: {}", e);
            } catch (YAML::ParserException const& e) {
                errors[name] = fmt::format("Invalid syntax: {}", e);
            } catch (YAML::BadConversion const& e) {
                errors[name] = fmt::format("Invalid value: {}", e);
            }

            // A validation error is fatal if the property was required,
            // e.g. if someone entered a non-integer node_id, or an invalid
            // internal RPC address.
            if (!ok && prop->is_required()) {
                throw std::invalid_argument(fmt::format(
                  "Property {} is required and has invalid value, e: {}",
                  name,
                  errors[name]));
            }
        }

        return errors;
    }

    template<typename Func>
    void for_each(Func&& f) const {
        for (auto const& [_, property] : _properties) {
            f(*property);
        }
    }

    /**
     *
     * @param filter optional callback for filtering out config properties.
     *               callback should return false to exclude a property.
     */
    void to_json(
      json::Writer<json::StringBuffer>& w,
      redact_secrets redact,
      std::optional<std::function<bool(base_property&)>> filter
      = std::nullopt) const {
        w.StartObject();

        for (const auto& [name, property] : _properties) {
            if (property->get_visibility() == visibility::deprecated) {
                continue;
            }

            if (filter && !filter.value()(*property)) {
                continue;
            }

            w.Key(name.data(), name.size());
            property->to_json(w, redact);
        }

        w.EndObject();
    }

    void to_json_for_metrics(json::Writer<json::StringBuffer>& w) {
        w.StartObject();

        for (const auto& [name, property] : _properties) {
            if (property->get_visibility() == visibility::deprecated) {
                continue;
            }

            if (property->type_name() == "boolean") {
                w.Key(name.data(), name.size());
                property->to_json(w, redact_secrets::yes);
                continue;
            }

            if (property->is_nullable()) {
                w.Key(name.data(), name.size());
                w.String(property->is_default() ? "default" : "[value]");
                continue;
            }

            if (!property->enum_values().empty()) {
                w.Key(name.data(), name.size());
                property->to_json(w, redact_secrets::yes);
                continue;
            }
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

    std::set<std::string_view> property_aliases() const {
        std::set<std::string_view> result;
        for (const auto& i : _aliases) {
            result.insert(i.first);
        }

        return result;
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

private:
    friend class base_property;
    std::unordered_map<std::string_view, base_property*> _properties;

    // If a property has some aliases for backward compat, they are tracked
    // here: a property must appear at least in _properties, and may appear
    // 0..n times in _aliases
    std::unordered_map<std::string_view, base_property*> _aliases;
};

inline YAML::Node to_yaml(const config_store& cfg, redact_secrets redact) {
    json::StringBuffer buf;
    json::Writer<json::StringBuffer> writer(buf);
    cfg.to_json(writer, redact);
    return YAML::Load(buf.GetString());
}
}; // namespace config
