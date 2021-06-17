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

#pragma once

#include "outcome.h"
#include "pandaproxy/schema_registry/error.h"
#include "pandaproxy/schema_registry/types.h"

#include <absl/container/btree_map.h>
#include <absl/container/node_hash_map.h>

namespace pandaproxy::schema_registry {

class store {
public:
    struct insert_result {
        schema_version version;
        schema_id id;
        bool inserted;
    };
    ///\brief Insert a schema for a given subject.
    ///
    /// If the schema is not registered, register it.
    /// If the subject does not have this schema at any version, register a new
    /// version.
    ///
    /// return the schema_version and schema_id, and whether it's new.
    insert_result insert(subject sub, schema_definition def, schema_type type) {
        auto id = insert_schema(std::move(def), type).id;
        auto [version, inserted] = insert_subject(std::move(sub), id);
        return {version, id, inserted};
    }

    ///\brief Return a schema by id.
    result<schema> get_schema(const schema_id& id) const {
        auto it = _schemas.find(id);
        if (it == _schemas.end()) {
            return error_code::schema_id_not_found;
        }
        return {it->first, it->second.type, it->second.definition};
    }

    ///\brief Return a schema by subject and version.
    result<subject_schema>
    get_subject_schema(const subject& sub, schema_version version) const {
        auto sub_it = _subjects.find(sub);
        if (sub_it == _subjects.end()) {
            return error_code::subject_not_found;
        }

        const auto& versions = sub_it->second.versions;
        auto v_it = std::lower_bound(
          versions.begin(),
          versions.end(),
          version,
          [](const subject_version_id& lhs, schema_version rhs) {
              return lhs.version < rhs;
          });
        if (v_it == versions.end() || v_it->version != version) {
            return error_code::subject_version_not_found;
        }

        auto s = get_schema(v_it->id);
        if (!s) {
            return s.as_failure();
        }

        return subject_schema{
          .sub = sub,
          .version = v_it->version,
          .id = v_it->id,
          .type = s.value().type,
          .definition = std::move(s).value().definition,
          .deleted = v_it->deleted};
    }

    ///\brief Return a list of subjects.
    std::vector<subject> get_subjects() const {
        std::vector<subject> res;
        res.reserve(_subjects.size());
        std::transform(
          _subjects.begin(),
          _subjects.end(),
          std::back_inserter(res),
          [](const auto& v) { return v.first; });
        return res;
    }

    ///\brief Return a list of versions and associated schema_id.
    result<std::vector<schema_version>> get_versions(const subject& sub) const {
        auto sub_it = _subjects.find(sub);
        if (sub_it == _subjects.end()) {
            return error_code::subject_not_found;
        }
        const auto& versions = sub_it->second.versions;
        std::vector<schema_version> res;
        res.reserve(versions.size());
        std::transform(
          versions.begin(),
          versions.end(),
          std::back_inserter(res),
          [](const auto& v) { return v.version; });
        return res;
    }

private:
    struct insert_schema_result {
        schema_id id;
        bool inserted;
    };
    insert_schema_result
    insert_schema(schema_definition def, schema_type type) {
        const auto s_it = std::find_if(
          _schemas.begin(), _schemas.end(), [&](const auto& s) {
              const auto& entry = s.second;
              return type == entry.type && def == entry.definition;
          });
        if (s_it != _schemas.end()) {
            return {s_it->first, false};
        }

        const auto id = _schemas.empty() ? schema_id{1}
                                         : std::prev(_schemas.end())->first + 1;
        auto [_, inserted] = _schemas.try_emplace(id, type, std::move(def));
        return {id, inserted};
    }

    struct insert_subject_result {
        schema_version version;
        bool inserted;
    };
    insert_subject_result insert_subject(subject sub, schema_id id) {
        auto& versions = _subjects[std::move(sub)].versions;
        const auto v_it = std::find_if(
          versions.cbegin(), versions.cend(), [id](auto v) {
              return v.id == id;
          });
        if (v_it != versions.cend()) {
            return {v_it->version, false};
        }

        const auto version = versions.empty() ? schema_version{1}
                                              : versions.back().version + 1;
        versions.emplace_back(version, id);
        return {version, true};
    }

    struct schema_entry {
        schema_entry(schema_type type, schema_definition definition)
          : type{type}
          , definition{std::move(definition)} {}

        schema_type type;
        schema_definition definition;
    };

    struct subject_entry {
        std::vector<subject_version_id> versions;
    };

    absl::btree_map<schema_id, schema_entry> _schemas;
    absl::node_hash_map<subject, subject_entry> _subjects;
};

} // namespace pandaproxy::schema_registry
