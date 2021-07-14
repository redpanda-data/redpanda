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

#include "pandaproxy/schema_registry/errors.h"
#include "pandaproxy/schema_registry/types.h"

#include <absl/container/btree_map.h>
#include <absl/container/node_hash_map.h>

namespace pandaproxy::schema_registry {

namespace detail {

template<typename T>
typename T::iterator
make_non_const_iterator(T& container, typename T::const_iterator it) {
    return container.erase(it, it);
}

template<typename T>
result<typename T::iterator>
make_non_const_iterator(T& container, result<typename T::const_iterator> it) {
    auto res = BOOST_OUTCOME_TRYX(it);
    return detail::make_non_const_iterator(container, res);
}

} // namespace detail

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

    ///\brief Update or insert a schema with the given id, and register it with
    /// the subject for the given version.
    ///
    /// return true if a new version was inserted, false if updated.
    bool upsert(
      subject sub,
      schema_definition def,
      schema_type type,
      schema_id id,
      schema_version version,
      is_deleted deleted) {
        upsert_schema(id, std::move(def), type);
        return upsert_subject(std::move(sub), version, id, deleted);
    }

    ///\brief Return a schema by id.
    result<schema> get_schema(const schema_id& id) const {
        auto it = _schemas.find(id);
        if (it == _schemas.end()) {
            return not_found(id);
        }
        return {it->first, it->second.type, it->second.definition};
    }

    ///\brief Return the id of the schema, if it already exists.
    std::optional<schema_id>
    get_schema_id(const schema_definition& def, schema_type type) const {
        const auto s_it = std::find_if(
          _schemas.begin(), _schemas.end(), [&](const auto& s) {
              const auto& entry = s.second;
              return type == entry.type && def == entry.definition;
          });
        return s_it == _schemas.end() ? std::optional<schema_id>{}
                                      : s_it->first;
    }

    ///\brief Return subject_version_id for a subject and version
    result<subject_version_id> get_subject_version_id(
      const subject& sub,
      schema_version version,
      include_deleted inc_del) const {
        auto sub_it = BOOST_OUTCOME_TRYX(get_subject_iter(sub, inc_del));
        auto v_it = BOOST_OUTCOME_TRYX(
          get_version_iter(*sub_it, version, inc_del));
        return *v_it;
    }

    ///\brief Return a schema by subject and version.
    result<subject_schema> get_subject_schema(
      const subject& sub,
      schema_version version,
      include_deleted inc_del) const {
        auto v_id = BOOST_OUTCOME_TRYX(
          get_subject_version_id(sub, version, inc_del));

        auto s = BOOST_OUTCOME_TRYX(get_schema(v_id.id));

        return subject_schema{
          .sub = sub,
          .version = v_id.version,
          .id = v_id.id,
          .type = s.type,
          .definition = std::move(s).definition,
          .deleted = v_id.deleted};
    }

    ///\brief Return a list of subjects.
    std::vector<subject> get_subjects(include_deleted inc_del) const {
        std::vector<subject> res;
        res.reserve(_subjects.size());
        for (const auto& sub : _subjects) {
            if (inc_del || !sub.second.deleted) {
                res.push_back(sub.first);
            }
        }
        return res;
    }

    ///\brief Return a list of versions and associated schema_id.
    result<std::vector<schema_version>>
    get_versions(const subject& sub, include_deleted inc_del) const {
        auto sub_it = BOOST_OUTCOME_TRYX(get_subject_iter(sub, inc_del));
        const auto& versions = sub_it->second.versions;
        std::vector<schema_version> res;
        res.reserve(versions.size());
        for (const auto& ver : versions) {
            if (inc_del || !ver.deleted) {
                res.push_back(ver.version);
            }
        }
        return res;
    }

    ///\brief Return a list of versions and associated schema_id.
    result<std::vector<subject_version_id>>
    get_version_ids(const subject& sub, include_deleted inc_del) const {
        auto sub_it = BOOST_OUTCOME_TRYX(get_subject_iter(sub, inc_del));
        return sub_it->second.versions;
    }

    ///\brief Delete a subject.
    result<std::vector<schema_version>>
    delete_subject(const subject& sub, permanent_delete permanent) {
        auto sub_it = BOOST_OUTCOME_TRYX(
          get_subject_iter(sub, include_deleted::yes));

        if (permanent && !sub_it->second.deleted) {
            return not_deleted(sub);
        }

        if (!permanent && sub_it->second.deleted) {
            return soft_deleted(sub);
        }

        sub_it->second.deleted = is_deleted::yes;

        const auto& versions = sub_it->second.versions;
        std::vector<schema_version> res;
        res.reserve(versions.size());
        std::transform(
          versions.begin(),
          versions.end(),
          std::back_inserter(res),
          [](const auto& v) { return v.version; });

        if (permanent) {
            _subjects.erase(sub_it);
        }

        return res;
    }

    ///\brief Delete a subject version
    result<bool> delete_subject_version(
      const subject& sub,
      schema_version version,
      permanent_delete permanent,
      include_deleted inc_del) {
        auto sub_it = BOOST_OUTCOME_TRYX(get_subject_iter(sub, inc_del));
        auto& versions = sub_it->second.versions;
        auto v_it = BOOST_OUTCOME_TRYX(
          get_version_iter(*sub_it, version, include_deleted::yes));

        if (!v_it->deleted && permanent && !inc_del) {
            return not_deleted(sub, version);
        }

        if (v_it->deleted && !permanent && !inc_del) {
            return soft_deleted(sub, version);
        }

        if (permanent) {
            versions.erase(v_it);
            return true;
        }
        return std::exchange(v_it->deleted, is_deleted::yes) != is_deleted::yes;
    }

    ///\brief Get the global compatibility level.
    result<compatibility_level> get_compatibility() const {
        return _compatibility;
    }

    ///\brief Get the compatibility level for a subject, or fallback to global.
    result<compatibility_level> get_compatibility(const subject& sub) const {
        auto sub_it = BOOST_OUTCOME_TRYX(
          get_subject_iter(sub, include_deleted::no));
        return sub_it->second.compatibility.value_or(_compatibility);
    }

    ///\brief Set the global compatibility level.
    result<bool> set_compatibility(compatibility_level compatibility) {
        return std::exchange(_compatibility, compatibility) != compatibility;
    }

    ///\brief Set the compatibility level for a subject.
    result<bool>
    set_compatibility(const subject& sub, compatibility_level compatibility) {
        auto sub_it = BOOST_OUTCOME_TRYX(
          get_subject_iter(sub, include_deleted::no));

        // TODO(Ben): Check needs to be made here?
        return std::exchange(sub_it->second.compatibility, compatibility)
               != compatibility;
    }

    ///\brief Clear the compatibility level for a subject.
    result<bool> clear_compatibility(const subject& sub) {
        auto sub_it = BOOST_OUTCOME_TRYX(
          get_subject_iter(sub, include_deleted::yes));
        return std::exchange(sub_it->second.compatibility, std::nullopt)
               != std::nullopt;
    }

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

    bool upsert_schema(schema_id id, schema_definition def, schema_type type) {
        return _schemas.insert_or_assign(id, schema_entry(type, std::move(def)))
          .second;
    }

    struct insert_subject_result {
        schema_version version;
        bool inserted;
    };
    insert_subject_result insert_subject(subject sub, schema_id id) {
        auto& subject_entry = _subjects[std::move(sub)];
        subject_entry.deleted = is_deleted::no;
        auto& versions = subject_entry.versions;
        const auto v_it = std::find_if(
          versions.begin(), versions.end(), [id](auto v) {
              return v.id == id;
          });
        if (v_it != versions.cend()) {
            auto inserted = std::exchange(v_it->deleted, is_deleted::no);
            return {v_it->version, bool(inserted)};
        }

        const auto version = versions.empty() ? schema_version{1}
                                              : versions.back().version + 1;
        versions.emplace_back(version, id, is_deleted::no);
        return {version, true};
    }

    bool upsert_subject(
      subject sub, schema_version version, schema_id id, is_deleted deleted) {
        auto& subject_entry = _subjects[std::move(sub)];
        // Inserting a version undeletes the subject
        subject_entry.deleted = is_deleted::no;
        auto& versions = subject_entry.versions;
        const auto v_it = std::lower_bound(
          versions.begin(),
          versions.end(),
          version,
          [](const subject_version_id& lhs, schema_version rhs) {
              return lhs.version < rhs;
          });
        if (v_it != versions.end() && v_it->version == version) {
            *v_it = subject_version_id(version, id, deleted);
            return false;
        }
        versions.insert(v_it, subject_version_id(version, id, deleted));
        return true;
    }

private:
    struct schema_entry {
        schema_entry(schema_type type, schema_definition definition)
          : type{type}
          , definition{std::move(definition)} {}

        schema_type type;
        schema_definition definition;
    };

    struct subject_entry {
        std::optional<compatibility_level> compatibility;
        std::vector<subject_version_id> versions;
        is_deleted deleted{false};
    };
    using schema_map = absl::btree_map<schema_id, schema_entry>;
    using subject_map = absl::node_hash_map<subject, subject_entry>;

    result<subject_map::iterator>
    get_subject_iter(const subject& sub, include_deleted inc_del) {
        const store* const_this = this;
        auto res = const_this->get_subject_iter(sub, inc_del);
        return detail::make_non_const_iterator(_subjects, res);
    }

    result<subject_map::const_iterator>
    get_subject_iter(const subject& sub, include_deleted inc_del) const {
        auto sub_it = _subjects.find(sub);
        if (sub_it == _subjects.end()) {
            return not_found(sub);
        }

        if (sub_it->second.deleted && !inc_del) {
            return not_found(sub);
        }
        return sub_it;
    }

    static result<std::vector<subject_version_id>::iterator> get_version_iter(
      subject_map::value_type& sub_entry,
      schema_version version,
      include_deleted inc_del) {
        const subject_map::value_type& const_entry = sub_entry;
        return detail::make_non_const_iterator(
          sub_entry.second.versions,
          get_version_iter(const_entry, version, inc_del));
    }

    static result<std::vector<subject_version_id>::const_iterator>
    get_version_iter(
      const subject_map::value_type& sub_entry,
      schema_version version,
      include_deleted inc_del) {
        auto& versions = sub_entry.second.versions;
        auto v_it = std::lower_bound(
          versions.begin(),
          versions.end(),
          version,
          [](const subject_version_id& lhs, schema_version rhs) {
              return lhs.version < rhs;
          });
        if (v_it == versions.end() || v_it->version != version) {
            return not_found(sub_entry.first, version);
        }
        if (!inc_del && v_it->deleted) {
            return not_found(sub_entry.first, version);
        }
        return v_it;
    }

    schema_map _schemas;
    subject_map _subjects;
    compatibility_level _compatibility{compatibility_level::none};
};

} // namespace pandaproxy::schema_registry
