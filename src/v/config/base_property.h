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
#include "config/validation_error.h"
#include "json/stringbuffer.h"
#include "json/writer.h"
#include "utils/named_type.h"

#include <seastar/util/bool_class.hh>

#include <yaml-cpp/yaml.h>

#include <any>
#include <iosfwd>
#include <string>
#include <string_view>

namespace config {

// String to use when logging the value of a secret property
static constexpr std::string_view secret_placeholder = "[secret]";

class config_store;
// using required = ss::bool_class<struct required_tag>;
// using needs_restart = ss::bool_class<struct needs_restart_tag>;
// using is_secret = ss::bool_class<struct is_secret_tag>;

enum class required : char {
    yes,
    no,
};

enum class needs_restart : char {
    yes,
    no,
};
enum class is_secret : char {
    yes,
    no,
};

// Whether to redact secrets. If true, `secret_placeholder` should be used
// instead of the config value.
using redact_secrets = ss::bool_class<struct redact_secrets_tag>;

enum class visibility : char {
    // Tunables can be set by the user, but they control implementation
    // details like (e.g. buffer sizes, queue lengths)
    tunable,
    // User properties are normal, end-user visible settings that control
    // functional redpanda behaviours (e.g. enable a feature)
    user,
    // Deprecated properties are kept around to avoid complaining
    // about invalid config after upgrades, but they do nothing and
    // should never be presented to the user for editing.
    deprecated,
};

// Whether to force an even or an odd value for a given property.
enum class odd_even_constraint {
    even,
    odd,
};

// This is equivalent to cluster::cluster_version, but defined here to
// avoid a dependency between config/ and cluster/
using legacy_version = named_type<int64_t, struct legacy_version_tag>;

std::string_view to_string_view(visibility v);

class base_property {
public:
    struct meta {
        required required{required::no};
        needs_restart needs_restart{needs_restart::yes};
        std::string_view example;
        visibility visibility{visibility::user};
        is_secret secret{is_secret::no};

        // Aliases are used exclusively for input: all output (e.g. listing
        // configuration) uses the primary name of the property.
        std::string_view aliases;
    };

    struct metadata {
        consteval metadata() {}
        consteval metadata(meta md) {
            if (md.needs_restart == needs_restart::yes) {
                flags |= 1;
            }
            if (md.required == required::yes) {
                flags |= 2;
            }
            if (md.secret == is_secret::yes) {
                flags |= 4;
            }
            if (md.visibility == visibility::deprecated) {
                flags |= 8;
            } else if (md.visibility == visibility::tunable) {
                flags |= 16;
            } else {
                // user
            }

            aliases = md.aliases.data();
            example = md.example.data();
        }

        std::uint8_t flags = 0;
        const char* aliases = nullptr;
        const char* example = nullptr;
    };

    base_property(
      config_store& conf,
      std::string_view name,
      std::string_view desc,
      metadata meta);

    const std::string_view& name() const { return _name; }
    const std::string_view& desc() const { return _desc; }

    required is_required() const {
        return _meta.flags & 2 ? required::yes : required::no;
    }
    bool needs_restart() const { return _meta.flags & 1; }
    visibility get_visibility() const {
        if (_meta.flags & 8) {
            return visibility::deprecated;
        } else if (_meta.flags & 16) {
            return visibility::tunable;
        }
        return visibility::user;
    }
    bool is_secret() const { return _meta.flags & 4; }
    std::vector<std::string_view> aliases() const {
        if (_meta.aliases != nullptr) {
            return {std::string_view(_meta.aliases)};
        }
        return {};
    }

    // this serializes the property value. a full configuration serialization is
    // performed in config_store::to_json where the json object key is taken
    // from the property name.
    virtual void
    to_json(json::Writer<json::StringBuffer>& w, redact_secrets redact) const
      = 0;

    virtual void print(std::ostream&) const = 0;
    virtual bool set_value(YAML::Node) = 0;
    virtual void set_value(std::any) = 0;
    virtual void reset() = 0;
    virtual bool is_default() const = 0;
    virtual bool is_hidden() const = 0;

    /**
     * Helper for logging string-ized values of a property, e.g.
     * while processing an API request or loading from file, before
     * the property itself is initialized.
     *
     * Use this to ensure that any logged values are properly
     * redacted if secret.
     */
    template<typename U>
    std::string_view format_raw(const U& in) {
        if (is_secret() && !in.empty()) {
            return secret_placeholder;
        } else {
            return in;
        }
    }

    virtual std::string_view type_name() const = 0;
    virtual std::optional<std::string_view> units_name() const = 0;
    virtual bool is_nullable() const = 0;
    virtual bool is_array() const = 0;
    virtual std::optional<std::string_view> example() const = 0;
    virtual std::vector<ss::sstring> enum_values() const { return {}; };

    /**
     * Validation of a proposed new value before it has been assigned
     * to this property.
     */
    virtual std::optional<validation_error> validate(YAML::Node) const = 0;
    virtual base_property& operator=(const base_property&) = 0;
    virtual ~base_property() noexcept = default;

    /**
     * Notify the property of the cluster's original logical version, in case
     * it has alternative defaults for old clusters.
     */
    virtual void notify_original_version(legacy_version) = 0;

private:
    friend std::ostream& operator<<(std::ostream&, const base_property&);
    std::string_view _name;
    std::string_view _desc;

protected:
    metadata _meta;
    void assert_live_settable() const;
};
}; // namespace config
