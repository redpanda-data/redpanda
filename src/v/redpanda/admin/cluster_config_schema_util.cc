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

#include "redpanda/admin/api-doc/cluster_config.json.hh"

#include <seastar/json/json_elements.hh>

namespace {

using property_map = std::map<
  ss::sstring,
  ss::httpd::cluster_config_json::cluster_config_property_metadata>;

void populate_property_metadata(
  ss::httpd::cluster_config_json::cluster_config_property_metadata& pm,
  const config::base_property& p) {
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

    std::vector<ss::sstring> aliases;
    for (const auto& alias : p.aliases()) {
        aliases.emplace_back(alias);
    }
    pm.aliases = aliases;

    {
        json::StringBuffer default_buf;
        json::Writer<json::StringBuffer> default_writer(default_buf);
        p.to_json_default(default_writer, config::redact_secrets::no);
        pm.default_value = ss::sstring(default_buf.GetString());
    }
    if (auto min = p.minimum_as_string(); min.has_value()) {
        pm.minimum = min.value();
    }
    if (auto max = p.maximum_as_string(); max.has_value()) {
        pm.maximum = max.value();
    }

    pm.is_enterprise = p.is_enterprise();
    if (p.is_enterprise()) {
        {
            json::StringBuffer sanctioned_buf;
            json::Writer<json::StringBuffer> sanctioned_writer(
              sanctioned_buf);
            p.to_json_enterprise_sanctioned(sanctioned_writer);
            pm.enterprise_sanctioned_value = ss::sstring(
              sanctioned_buf.GetString());
        }
        pm.enterprise_restriction_is_dynamic
          = p.enterprise_restriction_is_dynamic();
        {
            json::StringBuffer restricted_buf;
            json::Writer<json::StringBuffer> restricted_writer(
              restricted_buf);
            p.to_json_enterprise_restricted(restricted_writer);
            pm.enterprise_restricted_value = ss::sstring(
              restricted_buf.GetString());
        }
    }
}

} // namespace

// This is factored out to make it a separate binary that can generate schema
// without bringing up a redpanda cluster. Down stream tools can make use of
// this for config generation.
ss::json::json_return_type
util::generate_json_schema(const config::configuration& conf) {
    property_map properties;

    conf.for_each([&properties](const config::base_property& p) {
        if (p.is_hidden()) {
            // Do not mention deprecated settings in schema: they
            // only exist internally to avoid making existing stored
            // configs invalid.
            return;
        }

        auto [pm_i, inserted] = properties.emplace(
          ss::sstring(p.name()),
          ss::httpd::cluster_config_json::cluster_config_property_metadata());
        vassert(inserted, "Emplace failed, duplicate property name?");
        populate_property_metadata(pm_i->second, p);
    });

    std::map<ss::sstring, property_map> response = {
      {ss::sstring("properties"), std::move(properties)}};
    return ss::json::stream_object(std::move(response));
}
