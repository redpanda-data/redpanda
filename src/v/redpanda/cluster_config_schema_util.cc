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
#include "cluster_config_schema_util.h"

#include "redpanda/admin/api-doc/cluster_config.json.h"

// This is factored out to make it a separate binary that can generate schema
// without bringing up a redpanda cluster. Down stream tools can make use of
// this for config generation.
ss::json::json_return_type
util::generate_json_schema(const config::configuration& conf) {
    using property_map = std::map<
      ss::sstring,
      ss::httpd::cluster_config_json::cluster_config_property_metadata>;

    property_map properties;

    conf.for_each([&properties](const config::base_property& p) {
        if (p.get_visibility() == config::visibility::deprecated) {
            // Do not mention deprecated settings in schema: they
            // only exist internally to avoid making existing stored
            // configs invalid.
            return;
        }

        auto [pm_i, inserted] = properties.emplace(
          ss::sstring(p.name()),
          ss::httpd::cluster_config_json::cluster_config_property_metadata());
        vassert(inserted, "Emplace failed, duplicate property name?");

        auto& pm = pm_i->second;
        pm.description = ss::sstring(p.desc());
        pm.needs_restart = p.needs_restart();
        pm.visibility = ss::sstring(config::to_string_view(p.get_visibility()));
        pm.nullable = p.is_nullable();
        pm.is_secret = p.is_secret();

        if (p.is_array()) {
            pm.type = "array";

            auto items = ss::httpd::cluster_config_json::
              cluster_config_property_metadata_items();
            items.type = ss::sstring(p.type_name());
            pm.items = items;
        } else {
            pm.type = ss::sstring(p.type_name());
        }

        auto enum_values = p.enum_values();
        if (!enum_values.empty()) {
            // In swagger, this field would just be called 'enum', but
            // because we use C++ code generation for our structures,
            // we cannot use that reserved word
            pm.enum_values = enum_values;
        }

        const auto& example = p.example();
        if (example.has_value()) {
            pm.example = ss::sstring(example.value());
        }

        const auto& units = p.units_name();
        if (units.has_value()) {
            pm.units = ss::sstring(units.value());
        }
    });

    std::map<ss::sstring, property_map> response = {
      {ss::sstring("properties"), std::move(properties)}};
    return ss::json::json_return_type(std::move(response));
}
