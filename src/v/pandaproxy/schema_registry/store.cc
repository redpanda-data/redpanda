/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "pandaproxy/schema_registry/store.h"

#include "pandaproxy/schema_registry/avro.h"

namespace pandaproxy::schema_registry {

result<bool> store::is_compatible(
  const subject& sub,
  schema_version version,
  const schema_definition& new_schema,
  schema_type new_schema_type) {
    // Lookup the subject
    auto sub_it = _subjects.find(sub);
    if (sub_it == _subjects.end()) {
        return error_code::subject_not_found;
    }

    if (sub_it->second.deleted) {
        return error_code::subject_not_found;
    }

    // Lookup the version
    const auto& versions = sub_it->second.versions;
    auto ver_it = std::lower_bound(
      versions.begin(),
      versions.end(),
      version,
      [](const subject_version_id& lhs, schema_version rhs) {
          return lhs.version < rhs;
      });
    if (ver_it == versions.end() || ver_it->version != version) {
        return error_code::subject_version_not_found;
    }

    // Lookup the schema at the version
    auto sch_it = _schemas.find(ver_it->id);
    if (sch_it == _schemas.end()) {
        return error_code::schema_id_not_found;
    }

    // Types must always match
    const auto& old_schema = sch_it->second;
    if (old_schema.type != new_schema_type) {
        return false;
    }

    // Lookup the compatibility level
    auto compat_res = get_compatibility(sub);
    if (compat_res.has_error()) {
        return compat_res.error();
    }
    auto compat = compat_res.assume_value();

    if (compat == compatibility_level::none) {
        return true;
    }

    // Currently only support AVRO
    if (new_schema_type != schema_type::avro) {
        // TODO: better error_code
        return error_code::schema_invalid;
    }

    // if transitive, search all, otherwise seach forwards from version
    if (
      compat == compatibility_level::backward_transitive
      || compat == compatibility_level::forward_transitive
      || compat == compatibility_level::full_transitive) {
        ver_it = versions.begin();
    }

    auto new_avro_res = make_avro_schema_definition(new_schema());
    if (new_avro_res.has_error()) {
        return new_avro_res.error();
    }
    auto new_avro = std::move(new_avro_res).assume_value();

    auto is_compat = true;
    for (; is_compat && ver_it != versions.end(); ++ver_it) {
        auto sch_it = _schemas.find(ver_it->id);
        if (sch_it == _schemas.end()) {
            return error_code::schema_id_not_found;
        }

        auto old_avro_res = make_avro_schema_definition(
          sch_it->second.definition());
        if (old_avro_res.has_error()) {
            return old_avro_res.error();
        }
        auto old_avro = std::move(old_avro_res).assume_value();

        if (
          compat == compatibility_level::backward
          || compat == compatibility_level::backward_transitive
          || compat == compatibility_level::full) {
            is_compat = is_compat && check_compatible(new_avro, old_avro);
        }
        if (
          compat == compatibility_level::forward
          || compat == compatibility_level::forward_transitive
          || compat == compatibility_level::full) {
            is_compat = is_compat && check_compatible(old_avro, new_avro);
        }
    }
    return is_compat;
}

} // namespace pandaproxy::schema_registry
