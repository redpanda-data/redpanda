/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "container/fragmented_vector.h"
#include "pandaproxy/schema_registry/errors.h"
#include "pandaproxy/schema_registry/types.h"

#include <absl/algorithm/container.h>
#include <absl/container/btree_map.h>
#include <absl/container/btree_set.h>
#include <absl/container/node_hash_map.h>

namespace pandaproxy::schema_registry {

///\brief A mapping of version and schema id for a subject.
struct subject_version_entry {
    subject_version_entry(
      schema_version version, schema_id id, is_deleted deleted)
      : version{version}
      , id{id}
      , deleted(deleted) {}

    schema_version version;
    schema_id id;
    is_deleted deleted{is_deleted::no};

    std::vector<seq_marker> written_at;
};

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
    using schema_id_set = absl::btree_set<schema_id>;

    explicit store() = default;

    explicit store(is_mutable mut)
      : _mutable(mut) {}

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
    insert_result insert(canonical_schema schema) {
        auto [sub, def] = std::move(schema).destructure();
        auto id = insert_schema(std::move(def)).id;
        auto [version, inserted] = insert_subject(std::move(sub), id);
        return {version, id, inserted};
    }

    ///\brief Return a schema definition by id.
    result<canonical_schema_definition>
    get_schema_definition(const schema_id& id) const {
        auto it = _schemas.find(id);
        if (it == _schemas.end()) {
            return not_found(id);
        }
        return {it->second.definition.copy()};
    }

    ///\brief Return the id of the schema, if it already exists.
    std::optional<schema_id>
    get_schema_id(const canonical_schema_definition& def) const {
        const auto s_it = std::find_if(
          _schemas.begin(), _schemas.end(), [&](const auto& s) {
              const auto& entry = s.second;
              return def == entry.definition;
          });
        return s_it == _schemas.end() ? std::optional<schema_id>{}
                                      : s_it->first;
    }

    ///\brief Return a list of subject-versions for the shema id.
    chunked_vector<subject_version> get_schema_subject_versions(schema_id id) {
        chunked_vector<subject_version> svs;
        for (const auto& s : _subjects) {
            for (const auto& vs : s.second.versions) {
                if (vs.id == id && !vs.deleted) {
                    svs.emplace_back(s.first, vs.version);
                }
            }
        }
        return svs;
    }

    ///\brief Return a list of subjects for the schema id.
    chunked_vector<subject>
    get_schema_subjects(schema_id id, include_deleted inc_del) {
        chunked_vector<subject> subs;
        for (const auto& s : _subjects) {
            if (absl::c_any_of(
                  s.second.versions, [id, inc_del](const auto& vs) {
                      return vs.id == id && (inc_del || !vs.deleted);
                  })) {
                subs.emplace_back(s.first);
            }
        }
        return subs;
    }

    ///\brief Return subject_version_id for a subject and version
    result<subject_version_entry> get_subject_version_id(
      const subject& sub,
      std::optional<schema_version> version,
      include_deleted inc_del) const {
        auto sub_it = BOOST_OUTCOME_TRYX(get_subject_iter(sub, inc_del));

        if (!version.has_value()) {
            const auto& versions = sub_it->second.versions;
            auto it = std::find_if(
              versions.rbegin(), versions.rend(), [inc_del](const auto& ver) {
                  return inc_del || !ver.deleted;
              });
            if (it == versions.rend()) {
                return not_found(sub);
            }
            return *it;
        }

        auto v_it = BOOST_OUTCOME_TRYX(
          get_version_iter(*sub_it, *version, inc_del));
        return *v_it;
    }

    ///\brief Return a schema by subject and version.
    result<subject_schema> get_subject_schema(
      const subject& sub,
      std::optional<schema_version> version,
      include_deleted inc_del) const {
        auto v_id = BOOST_OUTCOME_TRYX(
          get_subject_version_id(sub, version, inc_del));

        auto def = BOOST_OUTCOME_TRYX(get_schema_definition(v_id.id));

        return subject_schema{
          .schema = {sub, std::move(def)},
          .version = v_id.version,
          .id = v_id.id,
          .deleted = v_id.deleted};
    }

    ///\brief Return a list of subjects.
    chunked_vector<subject> get_subjects(
      include_deleted inc_del,
      const std::optional<ss::sstring>& subject_prefix = std::nullopt) const {
        chunked_vector<subject> res;
        res.reserve(_subjects.size());
        for (const auto& sub : _subjects) {
            if (inc_del || !sub.second.deleted) {
                auto has_version = absl::c_any_of(
                  sub.second.versions,
                  [inc_del](const auto& v) { return inc_del || !v.deleted; });
                if (
                  has_version
                  && sub.first().starts_with(subject_prefix.value_or(""))) {
                    res.push_back(sub.first);
                }
            }
        }
        return res;
    }

    ///\brief Return if there are subjects.
    bool has_subjects(include_deleted inc_del) const {
        return absl::c_any_of(_subjects, [inc_del](const auto& sub) {
            return absl::c_any_of(
              sub.second.versions,
              [inc_del](const auto& v) { return inc_del || !v.deleted; });
        });
    }

    ///\brief Return a list of versions and associated schema_id.
    result<std::vector<schema_version>>
    get_versions(const subject& sub, include_deleted inc_del) const {
        auto sub_it = BOOST_OUTCOME_TRYX(get_subject_iter(sub, inc_del));
        const auto& versions = sub_it->second.versions;
        if (versions.empty()) {
            return not_found(sub);
        }
        std::vector<schema_version> res;
        res.reserve(versions.size());
        for (const auto& ver : versions) {
            if (inc_del || !ver.deleted) {
                res.push_back(ver.version);
            }
        }
        return res;
    }

    ///\brief Return the value of the 'deleted' field on a subject
    result<is_deleted> is_subject_deleted(const subject& sub) const {
        auto sub_it = BOOST_OUTCOME_TRYX(
          get_subject_iter(sub, include_deleted::yes));
        return sub_it->second.deleted;
    }

    ///\brief Return the value of the 'deleted' field on a subject
    result<is_deleted> is_subject_version_deleted(
      const subject& sub, const schema_version version) const {
        auto sub_it = BOOST_OUTCOME_TRYX(
          get_subject_iter(sub, include_deleted::yes));
        auto v_it = BOOST_OUTCOME_TRYX(
          get_version_iter(*sub_it, version, include_deleted::yes));
        return v_it->deleted;
    }

    /// \brief Return the seq_marker write history of a subject
    ///
    /// \return A vector with at least one element
    result<std::vector<seq_marker>>
    get_subject_written_at(const subject& sub) const {
        auto sub_it = BOOST_OUTCOME_TRYX(
          get_subject_iter(sub, include_deleted::yes));

        if (!sub_it->second.deleted) {
            // Refuse to yield sequence history for anything that
            // hasn't been soft-deleted, to prevent a hard-delete
            // from generating tombstones without a preceding soft-delete
            return not_deleted(sub);
        } else {
            if (sub_it->second.written_at.empty()) {
                // This should never happen (how can a record get into the
                // store without an originating sequenced record?), but return
                // an error instead of vasserting out.
                return not_found(sub);
            }

            return sub_it->second.written_at;
        }
    }

    /// \brief Return the seq_marker write history of a subject, but only
    /// config_keys
    ///
    /// \return A vector (possibly empty)
    result<std::vector<seq_marker>>
    get_subject_config_written_at(const subject& sub) const {
        auto sub_it = BOOST_OUTCOME_TRYX(
          get_subject_iter(sub, include_deleted::yes));

        // This should never happen (how can a record get into the
        // store without an originating sequenced record?), but return
        // an error instead of vasserting out.
        if (sub_it->second.written_at.empty()) {
            return not_found(sub);
        }

        std::vector<seq_marker> result;
        std::copy_if(
          sub_it->second.written_at.begin(),
          sub_it->second.written_at.end(),
          std::back_inserter(result),
          [](const auto& sm) {
              return sm.key_type == seq_marker_key_type::config;
          });

        return result;
    }

    /// \brief Return the seq_marker write history of a subject, but only
    /// mode_keys
    ///
    /// \return A vector (possibly empty)
    result<std::vector<seq_marker>>
    get_subject_mode_written_at(const subject& sub) const {
        auto sub_it = BOOST_OUTCOME_TRYX(
          get_subject_iter(sub, include_deleted::yes));

        // This should never happen (how can a record get into the
        // store without an originating sequenced record?), but return
        // an error instead of vasserting out.
        if (sub_it->second.written_at.empty()) {
            return not_found(sub);
        }

        std::vector<seq_marker> result;
        std::copy_if(
          sub_it->second.written_at.begin(),
          sub_it->second.written_at.end(),
          std::back_inserter(result),
          [](const auto& sm) {
              return sm.key_type == seq_marker_key_type::mode;
          });

        return result;
    }

    /// \brief Return the seq_marker write history of a version.
    ///
    /// \return A vector with at least one element
    result<std::vector<seq_marker>> get_subject_version_written_at(
      const subject& sub, schema_version version) const {
        auto sub_it = BOOST_OUTCOME_TRYX(
          get_subject_iter(sub, include_deleted::yes));

        auto v_it = BOOST_OUTCOME_TRYX(
          get_version_iter(*sub_it, version, include_deleted::yes));

        if (!v_it->deleted) {
            // Refuse to yield sequence history for anything that
            // hasn't been soft-deleted, to prevent a hard-delete
            // from generating tombstones without a preceding soft-delete
            return not_deleted(sub, version);
        }

        std::vector<seq_marker> result;
        for (auto s : sub_it->second.written_at) {
            if (s.version == version) {
                result.push_back(s);
            }
        }

        if (result.empty()) {
            // This should never happen (how can a record get into the
            // store without an originating sequenced record?), but return
            // an error instead of vasserting out.
            return not_found(sub, version);
        }

        return result;
    }

    ///\brief If this schema ID isn't already in the version list, return
    ///       what the version number will be if it is inserted.
    std::optional<schema_version>
    project_version(const subject& sub, schema_id sid) const {
        auto subject_iter = _subjects.find(sub);
        if (subject_iter == _subjects.end()) {
            // Subject doesn't exist yet.  First version will be 1.
            return schema_version{1};
        }

        const auto& versions = subject_iter->second.versions;

        schema_version maxver{0};
        for (const auto& v : versions) {
            if (v.id == sid && !(v.deleted || subject_iter->second.deleted)) {
                // No version to project, the schema is already
                // present (and not deleted) in this subject.
                // For a present-but-deleted case, we proceed
                // to allocate a new version number.
                return std::nullopt;
            } else {
                maxver = std::max(maxver, v.version);
            }
        }

        return maxver + 1;
    }

    ///\brief Return a list of versions and associated schema_id.
    result<std::vector<subject_version_entry>>
    get_version_ids(const subject& sub, include_deleted inc_del) const {
        auto sub_it = BOOST_OUTCOME_TRYX(get_subject_iter(sub, inc_del));
        std::vector<subject_version_entry> res;
        absl::c_copy_if(
          sub_it->second.versions,
          std::back_inserter(res),
          [inc_del](const subject_version_entry& e) {
              return inc_del || !e.deleted;
          });
        return {std::move(res)};
    }

    ///\brief Return whether this subject has a version that references the
    /// schema_id.
    result<bool> has_version(
      const subject& sub, schema_id id, include_deleted inc_del) const {
        auto sub_it = BOOST_OUTCOME_TRYX(get_subject_iter(sub, inc_del));
        const auto& vs = sub_it->second.versions;
        return absl::c_any_of(vs, [id, inc_del](const auto& entry) {
            return entry.id == id && (inc_del || !entry.deleted);
        });
    }

    schema_id_set referenced_by(const subject& sub, schema_version ver) {
        schema_id_set references;
        for (const auto& s : _schemas) {
            for (const auto& r : s.second.definition.refs()) {
                if (r.sub == sub && r.version == ver) {
                    references.insert(s.first);
                }
            }
        }
        return references;
    }

    schema_id_set subject_versions_with_any_of(const schema_id_set& ids) {
        schema_id_set has_ids;
        for (const auto& s : _subjects) {
            for (const auto& r : s.second.versions) {
                if (!r.deleted && ids.contains(r.id)) {
                    has_ids.insert(r.id);
                }
            }
        }
        return has_ids;
    }

    bool subject_versions_has_any_of(
      const schema_id_set& ids, include_deleted inc_del) {
        return absl::c_any_of(_subjects, [&ids, inc_del](const auto& s) {
            return absl::c_any_of(
              s.second.versions, [&ids, &s, inc_del](const auto& v) {
                  return (inc_del || !s.second.deleted) && ids.contains(v.id);
              });
        });
    }

    ///\brief Delete a subject.
    result<std::vector<schema_version>> delete_subject(
      seq_marker marker, const subject& sub, permanent_delete permanent) {
        auto sub_it = BOOST_OUTCOME_TRYX(
          get_subject_iter(sub, include_deleted::yes));

        if (permanent && !sub_it->second.deleted) {
            return not_deleted(sub);
        }

        if (!permanent && sub_it->second.deleted) {
            return soft_deleted(sub);
        }

        sub_it->second.written_at.push_back(marker);
        sub_it->second.deleted = is_deleted::yes;

        auto& versions = sub_it->second.versions;
        std::vector<schema_version> res;
        res.reserve(versions.size());
        for (const auto& ver : versions) {
            if (permanent || !ver.deleted) {
                res.push_back(ver.version);
            }
        }

        if (permanent) {
            _subjects.erase(sub_it);
        } else {
            // Mark all versions within the store deleted too: this matters
            // if someone revives the subject with new versions later, as
            // these older versions should remain deleted.
            for (auto& v : versions) {
                v.deleted = is_deleted::yes;
            }
        }

        return res;
    }

    ///\brief Delete a subject version.
    result<bool> delete_subject_version(
      const subject& sub, schema_version version, force force = force::no) {
        auto sub_it = BOOST_OUTCOME_TRYX(
          get_subject_iter(sub, include_deleted::yes));
        auto& versions = sub_it->second.versions;
        auto v_it = BOOST_OUTCOME_TRYX(
          get_version_iter(*sub_it, version, include_deleted::yes));

        // A hard delete should always be preceded by a soft delete,
        // however, due to compaction, it's possible that a soft-delete does not
        // appear on the topic. The topic is still correct, so override the
        // check if force::yes.
        if (!force && !(v_it->deleted || sub_it->second.deleted)) {
            return not_deleted(sub, version);
        }

        versions.erase(v_it);

        // Trim any seq_markers referring to this version, so
        // that when we later hard-delete the subject, we do not
        // emit more tombstones for versions already tombstoned
        auto& markers = sub_it->second.written_at;
        markers.erase(
          std::remove_if(
            markers.begin(),
            markers.end(),
            [&version](auto sm) { return sm.version == version; }),
          markers.end());

        if (versions.empty()) {
            _subjects.erase(sub_it);
        }

        return true;
    }

    ///\brief Get the global mode.
    result<mode> get_mode() const { return _mode; }

    ///\brief Get the mode for a subject, or fallback to global.
    result<mode>
    get_mode(const subject& sub, default_to_global fallback) const {
        auto sub_it = get_subject_iter(sub, include_deleted::yes);
        if (sub_it && (sub_it.assume_value())->second.mode.has_value()) {
            return (sub_it.assume_value())->second.mode.value();
        } else if (fallback) {
            return _mode;
        }
        return mode_not_found(sub);
    }

    ///\brief Set the global mode.
    result<bool> set_mode(mode m, force f) {
        BOOST_OUTCOME_TRYX(check_mode_mutability(f));
        return std::exchange(_mode, m) != m;
    }

    ///\brief Set the mode for a subject.
    result<bool>
    set_mode(seq_marker marker, const subject& sub, mode m, force f) {
        BOOST_OUTCOME_TRYX(check_mode_mutability(f));
        auto& sub_entry = _subjects[sub];
        sub_entry.written_at.push_back(marker);
        return std::exchange(sub_entry.mode, m) != m;
    }

    ///\brief Clear the mode for a subject.
    result<bool>
    clear_mode(const seq_marker& marker, const subject& sub, force f) {
        BOOST_OUTCOME_TRYX(check_mode_mutability(f));
        auto sub_it = BOOST_OUTCOME_TRYX(
          get_subject_iter(sub, include_deleted::yes));
        std::erase(sub_it->second.written_at, marker);
        return std::exchange(sub_it->second.mode, std::nullopt) != std::nullopt;
    }

    ///\brief Get the global compatibility level.
    result<compatibility_level> get_compatibility() const {
        return _compatibility;
    }

    ///\brief Get the compatibility level for a subject, or fallback to global.
    result<compatibility_level>
    get_compatibility(const subject& sub, default_to_global fallback) const {
        auto sub_it_res = get_subject_iter(sub, include_deleted::no);
        if (sub_it_res.has_error()) {
            return compatibility_not_found(sub);
        }
        auto sub_it = std::move(sub_it_res).assume_value();
        if (fallback) {
            return sub_it->second.compatibility.value_or(_compatibility);
        } else if (sub_it->second.compatibility) {
            return sub_it->second.compatibility.value();
        }
        return compatibility_not_found(sub);
    }

    ///\brief Set the global compatibility level.
    result<bool> set_compatibility(compatibility_level compatibility) {
        return std::exchange(_compatibility, compatibility) != compatibility;
    }

    ///\brief Set the compatibility level for a subject.
    result<bool> set_compatibility(
      seq_marker marker,
      const subject& sub,
      compatibility_level compatibility) {
        auto& sub_entry = _subjects[sub];
        sub_entry.written_at.push_back(marker);
        return std::exchange(sub_entry.compatibility, compatibility)
               != compatibility;
    }

    ///\brief Clear the compatibility level for a subject.
    result<bool>
    clear_compatibility(const seq_marker& marker, const subject& sub) {
        auto sub_it = BOOST_OUTCOME_TRYX(
          get_subject_iter(sub, include_deleted::yes));
        std::erase(sub_it->second.written_at, marker);
        return std::exchange(sub_it->second.compatibility, std::nullopt)
               != std::nullopt;
    }

    struct insert_schema_result {
        schema_id id;
        bool inserted;
    };
    insert_schema_result insert_schema(canonical_schema_definition def) {
        const auto s_it = std::find_if(
          _schemas.begin(), _schemas.end(), [&](const auto& s) {
              const auto& entry = s.second;
              return def == entry.definition;
          });
        if (s_it != _schemas.end()) {
            return {s_it->first, false};
        }

        const auto id = _schemas.empty() ? schema_id{1}
                                         : std::prev(_schemas.end())->first + 1;
        auto [_, inserted] = _schemas.try_emplace(id, std::move(def));
        return {id, inserted};
    }

    bool upsert_schema(schema_id id, canonical_schema_definition def) {
        return _schemas.insert_or_assign(id, schema_entry(std::move(def)))
          .second;
    }

    void delete_schema(schema_id id) { _schemas.erase(id); }

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
      seq_marker marker,
      subject sub,
      schema_version version,
      schema_id id,
      is_deleted deleted) {
        auto& subject_entry = _subjects[std::move(sub)];
        auto& versions = subject_entry.versions;
        subject_entry.written_at.push_back(marker);

        const auto v_it = std::lower_bound(
          versions.begin(),
          versions.end(),
          version,
          [](const subject_version_entry& lhs, schema_version rhs) {
              return lhs.version < rhs;
          });

        const bool found = v_it != versions.end() && v_it->version == version;
        if (found) {
            *v_it = subject_version_entry(version, id, deleted);
        } else {
            versions.emplace(v_it, version, id, deleted);
        }

        const auto all_deleted = is_deleted(
          std::all_of(versions.begin(), versions.end(), [](const auto& v) {
              return v.deleted;
          }));

        if (deleted == all_deleted) {
            // - If we're deleting and all are deleted, subject is deleted
            // - If we're not deleting and some are not deleted, the subject
            //   is not deleted.
            subject_entry.deleted = deleted;
        }

        return !found;
    }

    //// \brief Return error if the store is not mutable
    result<void> check_mode_mutability(force f) const {
        if (!_mutable && !f) {
            return error_info{
              error_code::subject_version_operation_not_permitted,
              "Mode changes are not allowed"};
        }
        return outcome::success();
    }

private:
    struct schema_entry {
        explicit schema_entry(canonical_schema_definition definition)
          : definition{std::move(definition)} {}

        canonical_schema_definition definition;
    };

    struct subject_entry {
        std::optional<compatibility_level> compatibility;
        std::optional<mode> mode;
        std::vector<subject_version_entry> versions;
        is_deleted deleted{false};

        std::vector<seq_marker> written_at;
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

    static result<std::vector<subject_version_entry>::iterator>
    get_version_iter(
      subject_map::value_type& sub_entry,
      schema_version version,
      include_deleted inc_del) {
        const subject_map::value_type& const_entry = sub_entry;
        return detail::make_non_const_iterator(
          sub_entry.second.versions,
          get_version_iter(const_entry, version, inc_del));
    }

    static result<std::vector<subject_version_entry>::const_iterator>
    get_version_iter(
      const subject_map::value_type& sub_entry,
      schema_version version,
      include_deleted inc_del) {
        auto& versions = sub_entry.second.versions;
        auto v_it = std::lower_bound(
          versions.begin(),
          versions.end(),
          version,
          [](const subject_version_entry& lhs, schema_version rhs) {
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
    compatibility_level _compatibility{compatibility_level::backward};
    mode _mode{mode::read_write};
    is_mutable _mutable{is_mutable::no};
};

} // namespace pandaproxy::schema_registry
