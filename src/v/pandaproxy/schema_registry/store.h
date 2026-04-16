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

#include "absl/algorithm/container.h"
#include "absl/container/btree_map.h"
#include "absl/container/btree_set.h"
#include "absl/container/node_hash_map.h"
#include "config/configuration.h"
#include "container/chunked_vector.h"
#include "metrics/metrics.h"
#include "metrics/prometheus_sanitize.h"
#include "pandaproxy/logger.h"
#include "pandaproxy/schema_registry/errors.h"
#include "pandaproxy/schema_registry/types.h"

#include <seastar/core/metrics.hh>

#include <algorithm>
#include <optional>
#include <ranges>
#include <utility>

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
    using schema_id_set = absl::btree_set<context_schema_id>;

    explicit store()
      : store(is_mutable::no) {}

    explicit store(is_mutable mut)
      : _mutable(mut) {
        setup_metrics();
    }

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
    insert_result insert(subject_schema schema) {
        auto [sub, def] = std::move(schema).destructure();
        auto id = insert_schema(sub.ctx, std::move(def)).id;
        auto [version, inserted] = insert_subject(std::move(sub), id);
        return {version, id, inserted};
    }

    ///\brief Return a schema definition by id.
    result<schema_definition>
    get_schema_definition(const context_schema_id& id) const {
        auto it = _schemas.find(id);
        if (it == _schemas.end()) {
            return not_found(id.id);
        }
        return {it->second.definition.share()};
    }

    ///\brief Return the id of the schema, if it already exists.
    std::optional<schema_id>
    get_schema_id(const context& ctx, const schema_definition& def) const {
        // Iterate in decreasing order to return the maximal matching id
        auto rev = std::views::reverse(_schemas);
        const auto s_it = std::ranges::find_if(rev, [&](const auto& s) {
            return ctx == s.first.ctx && def == s.second.definition;
        });
        return s_it == rev.end() ? std::optional<schema_id>{} : s_it->first.id;
    }

    ///\brief Return a list of subject-versions for the shema id.
    chunked_vector<subject_version>
    get_schema_subject_versions(const context_schema_id& id) {
        chunked_vector<subject_version> svs;
        for (const auto& s : _subjects) {
            if (s.first.ctx != id.ctx) {
                continue;
            }
            for (const auto& vs : s.second.versions) {
                if (vs.id == id.id && !vs.deleted) {
                    svs.emplace_back(s.first, vs.version);
                }
            }
        }
        return svs;
    }

    ///\brief Return a list of subjects for the schema id.
    chunked_vector<context_subject>
    get_schema_subjects(const context_schema_id& id, include_deleted inc_del) {
        chunked_vector<context_subject> subs;
        for (const auto& s : _subjects) {
            if (s.first.ctx != id.ctx) {
                continue;
            }
            if (
              std::ranges::any_of(
                s.second.versions, [&id, inc_del](const auto& vs) {
                    return vs.id == id.id && (inc_del || !vs.deleted);
                })) {
                subs.emplace_back(s.first);
            }
        }
        return subs;
    }

    ///\brief Return subject_version_id for a subject and version
    result<subject_version_entry> get_subject_version_id(
      const context_subject& sub,
      std::optional<schema_version> version,
      include_deleted inc_del) const {
        auto sub_it = BOOST_OUTCOME_TRYX(get_subject_iter(sub, inc_del));

        if (!version.has_value()) {
            const auto& versions = sub_it->second.versions;
            auto reversed = versions | std::views::reverse;
            auto it = std::ranges::find_if(
              reversed,
              [inc_del](const auto& ver) { return inc_del || !ver.deleted; });
            if (it == std::ranges::end(reversed)) {
                return not_found(sub);
            }
            return *it;
        }

        auto v_it = BOOST_OUTCOME_TRYX(
          get_version_iter(*sub_it, *version, inc_del));
        return *v_it;
    }

    ///\brief Return a schema by subject and version.
    result<stored_schema> get_subject_schema(
      const context_subject& sub,
      std::optional<schema_version> version,
      include_deleted inc_del) const {
        auto v_id = BOOST_OUTCOME_TRYX(
          get_subject_version_id(sub, version, inc_del));

        auto def = BOOST_OUTCOME_TRYX(
          get_schema_definition({sub.ctx, v_id.id}));

        return stored_schema{
          .schema = {sub, std::move(def)},
          .version = v_id.version,
          .id = v_id.id,
          .deleted = v_id.deleted};
    }

    ///\brief Return a list of subjects.
    chunked_vector<context_subject> get_subjects(
      include_deleted inc_del,
      std::optional<std::string_view> subject_prefix = std::nullopt) const {
        const auto original_prefix = subject_prefix;
        chunked_vector<context_subject> res;
        res.reserve(_subjects.size());

        constexpr std::string_view WILDCARD_CTX{":*:"};
        constexpr std::string_view DEFAULT_CTX{":.:"};
        const auto prefix_has_wildcard_ctx = subject_prefix.has_value()
                                             && subject_prefix->starts_with(
                                               WILDCARD_CTX);
        const auto prefix_has_default_ctx = subject_prefix.has_value()
                                            && subject_prefix->starts_with(
                                              DEFAULT_CTX);

        // If the prefix has a wildcard or qualified default context, strip it
        // for matching purposes
        if (prefix_has_wildcard_ctx) {
            subject_prefix = subject_prefix->substr(WILDCARD_CTX.size());
        } else if (prefix_has_default_ctx) {
            subject_prefix = subject_prefix->substr(DEFAULT_CTX.size());
        }

        auto matching_subject_names
          = _subjects | std::views::filter([inc_del](const auto& s) {
                return inc_del || !s.second.deleted;
            })
            | std::views::filter([inc_del](const auto& s) {
                  return std::ranges::any_of(
                    s.second.versions,
                    [inc_del](const auto& v) { return inc_del || !v.deleted; });
              })
            | std::views::filter([&](const auto& s) {
                  if (!subject_prefix.has_value()) {
                      return true;
                  }

                  if (prefix_has_wildcard_ctx) {
                      // Wildcard context prefix: match subjects across all
                      // contexts by subject name prefix
                      return s.first.sub().starts_with(subject_prefix.value());
                  } else if (prefix_has_default_ctx) {
                      // Qualified default context prefix: match only against
                      // subjects in the default context, which have no context
                      // prefix in their name
                      return s.first.ctx == default_context
                             && s.first.sub().starts_with(
                               subject_prefix.value());
                  } else {
                      return s.first.starts_with(subject_prefix.value());
                  }
              })
            | std::views::keys;

        std::ranges::copy(matching_subject_names, std::back_inserter(res));

        vlog(
          srlog.trace,
          "Listing subjects with prefix=\"{}\", mode={}, matched={} subjects",
          original_prefix.value_or("(none)"),
          (prefix_has_wildcard_ctx  ? "wildcard"
           : prefix_has_default_ctx ? "default_ctx"
                                    : "normal"),
          res.size());

        return res;
    }

    ///\brief Return if there are subjects.
    bool has_subjects(const context& ctx, include_deleted inc_del) const {
        return std::ranges::any_of(_subjects, [inc_del, &ctx](const auto& sub) {
            if (sub.first.ctx != ctx) {
                return false;
            }
            return std::ranges::any_of(
              sub.second.versions,
              [inc_del](const auto& v) { return inc_del || !v.deleted; });
        });
    }

    ///\brief Return a list of versions and associated schema_id.
    result<chunked_vector<schema_version>>
    get_versions(const context_subject& sub, include_deleted inc_del) const {
        auto sub_it = BOOST_OUTCOME_TRYX(get_subject_iter(sub, inc_del));
        const auto& versions = sub_it->second.versions;
        if (versions.empty()) {
            return not_found(sub);
        }
        chunked_vector<schema_version> res;
        res.reserve(versions.size());
        for (const auto& ver : versions) {
            if (inc_del || !ver.deleted) {
                res.push_back(ver.version);
            }
        }
        return res;
    }

    ///\brief Return the value of the 'deleted' field on a subject
    result<is_deleted> is_subject_deleted(const context_subject& sub) const {
        auto sub_it = BOOST_OUTCOME_TRYX(
          get_subject_iter(sub, include_deleted::yes));
        return sub_it->second.deleted;
    }

    ///\brief Return the value of the 'deleted' field on a subject
    result<is_deleted> is_subject_version_deleted(
      const context_subject& sub, const schema_version version) const {
        auto sub_it = BOOST_OUTCOME_TRYX(
          get_subject_iter(sub, include_deleted::yes));
        auto v_it = BOOST_OUTCOME_TRYX(
          get_version_iter(*sub_it, version, include_deleted::yes));
        return v_it->deleted;
    }

    /// \brief Return the seq_marker write history of a subject
    ///
    /// \return A vector with at least one element
    result<chunked_vector<seq_marker>>
    get_subject_written_at(const context_subject& sub) const {
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

            return sub_it->second.written_at.copy();
        }
    }

    /// \brief Return the seq_marker write history of a subject, but only
    /// config_keys
    ///
    /// \return A vector (possibly empty)
    result<chunked_vector<seq_marker>>
    get_subject_config_written_at(const context_subject& sub) const {
        auto sub_it = BOOST_OUTCOME_TRYX(
          get_subject_iter(sub, include_deleted::yes));

        // This should never happen (how can a record get into the
        // store without an originating sequenced record?), but return
        // an error instead of vasserting out.
        if (sub_it->second.written_at.empty()) {
            return not_found(sub);
        }

        chunked_vector<seq_marker> result;
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
    result<chunked_vector<seq_marker>>
    get_subject_mode_written_at(const context_subject& sub) const {
        auto sub_it = BOOST_OUTCOME_TRYX(
          get_subject_iter(sub, include_deleted::yes));

        // This should never happen (how can a record get into the
        // store without an originating sequenced record?), but return
        // an error instead of vasserting out.
        if (sub_it->second.written_at.empty()) {
            return not_found(sub);
        }

        chunked_vector<seq_marker> result;
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
    result<chunked_vector<seq_marker>> get_subject_version_written_at(
      const context_subject& sub, schema_version version) const {
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

        chunked_vector<seq_marker> result;
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
    project_version(const context_subject& sub, schema_id sid) const {
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

        // Once we have hit the maximum version number, we can't continue on
        if (maxver == std::numeric_limits<schema_version::type>::max()) {
            throw as_exception(versions_exhausted(sub));
        }

        return maxver + 1;
    }

    ///\brief Return a list of versions and associated schema_id.
    result<chunked_vector<subject_version_entry>>
    get_version_ids(const context_subject& sub, include_deleted inc_del) const {
        auto sub_it = BOOST_OUTCOME_TRYX(get_subject_iter(sub, inc_del));
        chunked_vector<subject_version_entry> res;
        std::ranges::copy_if(
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
      const context_subject& sub, schema_id id, include_deleted inc_del) const {
        auto sub_it = BOOST_OUTCOME_TRYX(get_subject_iter(sub, inc_del));
        const auto& vs = sub_it->second.versions;
        return std::ranges::any_of(vs, [id, inc_del](const auto& entry) {
            return entry.id == id && (inc_del || !entry.deleted);
        });
    }

    schema_id_set referenced_by(
      const context_subject& sub, std::optional<schema_version> ver) {
        schema_id_set references;
        for (const auto& s : _schemas) {
            for (const auto& r : s.second.definition.refs()) {
                if (
                  r.sub.resolve(s.first.ctx) == sub
                  && (!ver.has_value() || r.version == *ver)) {
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
                auto ctx_id = context_schema_id{s.first.ctx, r.id};
                if (!r.deleted && ids.contains(ctx_id)) {
                    has_ids.insert(ctx_id);
                }
            }
        }
        return has_ids;
    }

    bool subject_versions_has_any_of(
      const schema_id_set& ids, include_deleted inc_del) {
        return std::ranges::any_of(_subjects, [&ids, inc_del](const auto& s) {
            return std::ranges::any_of(
              s.second.versions, [&ids, &s, inc_del](const auto& v) {
                  return (inc_del || !s.second.deleted)
                         && ids.contains({s.first.ctx, v.id});
              });
        });
    }

    ///\brief Delete a subject.
    result<chunked_vector<schema_version>> delete_subject(
      seq_marker marker,
      const context_subject& sub,
      permanent_delete permanent) {
        auto sub_it = BOOST_OUTCOME_TRYX(
          get_subject_iter(sub, include_deleted::yes));

        if (permanent && !sub_it->second.deleted) {
            return not_deleted(sub);
        }

        if (!permanent && sub_it->second.deleted) {
            return soft_deleted(sub);
        }

        // Track the subject's deleted status before modification
        is_deleted was_deleted = sub_it->second.deleted;

        sub_it->second.written_at.push_back(marker);
        sub_it->second.deleted = is_deleted::yes;

        auto& versions = sub_it->second.versions;
        chunked_vector<schema_version> res;
        res.reserve(versions.size());
        for (const auto& ver : versions) {
            if (permanent || !ver.deleted) {
                res.push_back(ver.version);
            }
        }

        if (permanent) {
            // Permanent delete - decrement appropriate counter and erase
            auto ctx_it = _context_stores.find(sub.ctx);
            if (ctx_it != _context_stores.end()) {
                // Decrement based on current status
                ctx_it->second.decrement_subject_count(is_deleted::yes);
            }
            _subjects.erase(sub_it);
        } else {
            // Soft delete - move from not-deleted to deleted if it wasn't
            // already deleted
            if (!was_deleted) {
                get_or_create_context_store(sub.ctx)
                  .update_subject_deleted_status(
                    is_deleted::no, is_deleted::yes);
            }
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
      const context_subject& sub,
      schema_version version,
      force force = force::no) {
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

        // chunked_vector doesn't support erase(), so we need to
        // manually erase
        std::shift_left(v_it, versions.end(), 1);
        versions.pop_back();

        // Trim any seq_markers referring to this version, so
        // that when we later hard-delete the subject, we do not
        // emit more tombstones for versions already tombstoned
        auto& markers = sub_it->second.written_at;
        markers.erase_to_end(
          std::ranges::remove(markers, version, &seq_marker::version).begin());

        if (versions.empty()) {
            _subjects.erase(sub_it);
        }

        return true;
    }

    ///\brief Get the mode of a context.
    result<mode>
    get_mode(const context& ctx, default_to_global fallback) const {
        // Check context's own mode
        if (
          auto it = _context_stores.find(ctx);
          it != _context_stores.end() && it->second._mode.has_value()) {
            return it->second._mode.value();
        }

        // Default and global contexts always return a value, never
        // an error. Other contexts enter this block only when
        // fallback is enabled; otherwise they reach the
        // error return below. Within this block, non-global contexts
        // consult global_context first, and if that has no value,
        // return the hard-coded default.
        if (fallback || ctx == default_context || ctx == global_context) {
            if (fallback && ctx != global_context) {
                if (
                  auto global_it = _context_stores.find(global_context);
                  global_it != _context_stores.end()
                  && global_it->second._mode.has_value()) {
                    return *global_it->second._mode;
                }
            }
            return default_top_level_mode;
        }

        return mode_not_found(ctx);
    }

    ///\brief Get the mode for a subject, or fallback to global.
    result<mode>
    get_mode(const context_subject& sub, default_to_global fallback) const {
        auto sub_it = get_subject_iter(sub, include_deleted::yes);
        if (sub_it && (sub_it.assume_value())->second.mode.has_value()) {
            return (sub_it.assume_value())->second.mode.value();
        }
        // Fall through to context-level mode.
        // global_context subjects always fall through; other contexts only fall
        // through when fallback is set.
        if (sub.ctx == global_context || fallback) {
            return get_mode(sub.ctx, fallback);
        }
        return mode_not_found(sub);
    }

    ///\brief Set the mode of a context.
    result<bool>
    set_mode(seq_marker marker, const context& ctx, mode m, force f) {
        BOOST_OUTCOME_TRYX(check_mode_mutability(f));
        auto& context = get_or_create_context_store(ctx);
        context._mode_written_at.emplace_back(marker);
        return std::exchange(context._mode, m) != m;
    }

    ///\brief Set the mode for a subject.
    result<bool>
    set_mode(seq_marker marker, const context_subject& sub, mode m, force f) {
        BOOST_OUTCOME_TRYX(check_mode_mutability(f));
        auto& sub_entry = get_or_create_subject_entry(sub);
        sub_entry.written_at.push_back(marker);
        return std::exchange(sub_entry.mode, m) != m;
    }

    ///\brief Clear the mode for a subject.
    result<bool>
    clear_mode(const seq_marker& marker, const context_subject& sub, force f) {
        BOOST_OUTCOME_TRYX(check_mode_mutability(f));
        auto sub_it = BOOST_OUTCOME_TRYX(
          get_subject_iter(sub, include_deleted::yes));
        auto& vec = sub_it->second.written_at;
        vec.erase_to_end(std::ranges::remove(vec, marker).begin());
        return std::exchange(sub_it->second.mode, std::nullopt) != std::nullopt;
    }

    ///\brief Clear the mode for a context.
    result<bool> clear_mode(const context& ctx, force f) {
        BOOST_OUTCOME_TRYX(check_mode_mutability(f));
        auto& context = get_or_create_context_store(ctx);
        context._mode_written_at.clear();
        return std::exchange(context._mode, std::nullopt) != std::nullopt;
    }

    /// \brief Return the seq_marker write history of a context, but only
    /// mode keys
    result<chunked_vector<seq_marker>>
    get_context_mode_written_at(const context& ctx) const {
        auto it = _context_stores.find(ctx);
        if (
          it == _context_stores.end() || it->second._mode_written_at.empty()) {
            return not_found({ctx, subject{""}});
        }

        return it->second._mode_written_at.copy();
    }

    ///\brief Get the compatibility level of a context.
    result<compatibility_level>
    get_compatibility(const context& ctx, default_to_global fallback) const {
        // Check context's own config
        if (
          auto it = _context_stores.find(ctx);
          it != _context_stores.end()
          && it->second._compatibility.has_value()) {
            return it->second._compatibility.value();
        }

        // Default and global contexts always return a value, never
        // an error. Other contexts enter this block only when
        // fallback is enabled; otherwise they reach the
        // error return below. Within this block, non-global contexts
        // consult global_context first, and if that has no value,
        // return the hard-coded default.
        if (fallback || ctx == default_context || ctx == global_context) {
            if (fallback && ctx != global_context) {
                if (
                  auto global_it = _context_stores.find(global_context);
                  global_it != _context_stores.end()
                  && global_it->second._compatibility.has_value()) {
                    return *global_it->second._compatibility;
                }
            }
            return default_top_level_compat;
        }

        return compatibility_not_found(ctx);
    }

    ///\brief Get the compatibility level for a subject, or fallback to global.
    result<compatibility_level> get_compatibility(
      const context_subject& sub, default_to_global fallback) const {
        // Check subject's own config (if the subject exists)
        auto sub_it_res = get_subject_iter(sub, include_deleted::no);
        if (sub_it_res.has_value()) {
            auto sub_it = std::move(sub_it_res).assume_value();
            auto compat = sub_it->second.compatibility;
            if (compat) {
                return compat.value();
            }
        }

        // Fall through to context-level compatibility.
        // global_context subjects always fall through; other contexts only fall
        // through when fallback is set.
        if (sub.ctx == global_context || fallback) {
            return get_compatibility(sub.ctx, fallback);
        }

        return compatibility_not_found(sub);
    }

    ///\brief Set the compatibility level of a context.
    result<bool> set_compatibility(
      seq_marker marker,
      const context& ctx,
      compatibility_level compatibility) {
        auto& context = get_or_create_context_store(ctx);
        context._config_written_at.push_back(marker);
        return std::exchange(context._compatibility, compatibility)
               != compatibility;
    }

    ///\brief Set the compatibility level for a subject.
    result<bool> set_compatibility(
      seq_marker marker,
      const context_subject& sub,
      compatibility_level compatibility) {
        auto& sub_entry = get_or_create_subject_entry(sub);
        sub_entry.written_at.push_back(marker);
        return std::exchange(sub_entry.compatibility, compatibility)
               != compatibility;
    }

    ///\brief Clear the compatibility level of a context.
    result<bool> clear_compatibility(const context& ctx) {
        auto& context = get_or_create_context_store(ctx);
        context._config_written_at.clear();
        return std::exchange(context._compatibility, std::nullopt)
               != std::nullopt;
    }

    ///\brief Clear the compatibility level for a subject.
    result<bool>
    clear_compatibility(const seq_marker& marker, const context_subject& sub) {
        auto sub_it = BOOST_OUTCOME_TRYX(
          get_subject_iter(sub, include_deleted::yes));
        auto& vec = sub_it->second.written_at;
        vec.erase_to_end(std::ranges::remove(vec, marker).begin());
        return std::exchange(sub_it->second.compatibility, std::nullopt)
               != std::nullopt;
    }

    /// \brief Return the seq_marker write history of a context, but only
    /// config keys
    result<chunked_vector<seq_marker>>
    get_context_config_written_at(const context& ctx) const {
        auto it = _context_stores.find(ctx);
        if (
          it == _context_stores.end()
          || it->second._config_written_at.empty()) {
            return not_found({ctx, subject{""}});
        }

        return it->second._config_written_at.copy();
    }

    struct insert_schema_result {
        schema_id id;
        bool inserted;
    };
    insert_schema_result
    insert_schema(const context& ctx, schema_definition def) {
        if (auto id = get_schema_id(ctx, def); id.has_value()) {
            return {*id, false};
        }

        const auto id = _schemas.empty()
                          ? schema_id{1}
                          : std::prev(_schemas.end())->first.id + 1;
        auto [_, inserted] = _schemas.try_emplace(
          context_schema_id{ctx, id}, std::move(def));

        if (inserted) {
            get_or_create_context_store(ctx).increment_schema_count();
        }

        return {id, inserted};
    }

    bool upsert_schema(
      context_schema_id id, schema_definition def, bool mark_schema) {
        if (mark_schema) {
            _marked_schemas.push_back(id);
        }
        auto [it, inserted] = _schemas.insert_or_assign(
          std::move(id), schema_entry(std::move(def)));

        if (inserted) {
            get_or_create_context_store(it->first.ctx).increment_schema_count();
        }

        return inserted;
    }

    void delete_schema(const context_schema_id& id) {
        auto it = _schemas.find(id);
        if (it != _schemas.end()) {
            auto ctx_it = _context_stores.find(id.ctx);
            if (ctx_it != _context_stores.end()) {
                ctx_it->second.decrement_schema_count();
            }
            _schemas.erase(it);
        }
    }

    // This function returns and unmarkes all marked schemas.
    chunked_vector<context_schema_id> extract_marked_schemas() {
        return std::exchange(_marked_schemas, {});
    }

    struct insert_subject_result {
        schema_version version;
        bool inserted;
    };
    insert_subject_result insert_subject(context_subject sub, schema_id id) {
        auto [it, inserted] = _subjects.try_emplace(sub, sub);
        auto& subject_entry = it->second;

        // Track the previous deleted status
        is_deleted was_deleted = subject_entry.deleted;
        subject_entry.deleted = is_deleted::no;

        // Update counters based on whether this is a new subject or revival
        if (inserted) {
            // New subject - increment not-deleted counter
            get_or_create_context_store(sub.ctx).increment_subject_count(
              is_deleted::no);
        } else if (was_deleted) {
            // Reviving a deleted subject - move from deleted to not-deleted
            get_or_create_context_store(sub.ctx).update_subject_deleted_status(
              is_deleted::yes, is_deleted::no);
        }

        auto& versions = subject_entry.versions;
        const auto v_it = std::find_if(
          versions.begin(), versions.end(), [id](auto v) {
              return v.id == id;
          });
        if (v_it != versions.cend()) {
            auto was_deleted = std::exchange(v_it->deleted, is_deleted::no);
            return {v_it->version, bool(was_deleted)};
        }

        const auto version = versions.empty() ? schema_version{1}
                                              : versions.back().version + 1;
        versions.emplace_back(version, id, is_deleted::no);
        return {version, true};
    }

    bool upsert_subject(
      seq_marker marker,
      context_subject sub,
      schema_version version,
      schema_id id,
      is_deleted deleted) {
        auto [it, inserted] = _subjects.try_emplace(sub, sub);
        auto& subject_entry = it->second;

        // Track if subject deletion status changed
        is_deleted old_deleted = subject_entry.deleted;

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
            // chunked_vector doesn't support emplace(), so we need to manually
            // emplace at the back and rotate it into position
            auto idx = v_it - versions.begin();
            versions.emplace_back(version, id, deleted);
            std::rotate(
              versions.begin() + idx, versions.end() - 1, versions.end());
        }

        const auto all_deleted = is_deleted(
          std::all_of(versions.begin(), versions.end(), [](const auto& v) {
              return static_cast<bool>(v.deleted);
          }));

        if (deleted == all_deleted) {
            // - If we're deleting and all are deleted, subject is deleted
            // - If we're not deleting and some are not deleted, the subject
            //   is not deleted.
            subject_entry.deleted = deleted;
        }

        // Update counters based on whether this is new or status changed
        if (inserted) {
            // New subject - increment appropriate counter
            get_or_create_context_store(sub.ctx).increment_subject_count(
              deleted);
        } else if (old_deleted != subject_entry.deleted) {
            // Deletion status changed - move between counters
            get_or_create_context_store(sub.ctx).update_subject_deleted_status(
              old_deleted, subject_entry.deleted);
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

    void setup_metrics() {
        namespace sm = ss::metrics;
        const auto make_schema_bytes = [this]() {
            return sm::make_gauge(
              "schema_memory_bytes",
              [this] {
                  return absl::c_accumulate(
                    _schemas | std::views::transform([](const auto& s) {
                        return s.second.definition.raw()().size_bytes();
                    }),
                    size_t{0});
              },
              sm::description("The memory usage of schemas in the store"));
        };
        auto group_name = prometheus_sanitize::metrics_name(
          "schema_registry_cache");
        const std::vector<sm::label> agg{{sm::shard_label}};

        if (!config::shard_local_cfg().disable_metrics()) {
            _metrics.add_group(
              group_name,
              {
                make_schema_bytes(),
              },
              {},
              agg);
        }

        if (!config::shard_local_cfg().disable_public_metrics()) {
            _public_metrics.add_group(
              group_name,
              {
                make_schema_bytes().aggregate(agg),
              });
        }
    };

    void maybe_update_max_schema_id(const context_schema_id& id) {
        auto& nsi = get_or_create_context_store(id.ctx)._next_schema_id;
        auto old = nsi;
        nsi = std::max(nsi, id.id + schema_id{1});
        vlog(
          srlog.debug, "Context {} next_schema_id: {} -> {}", id.ctx, old, nsi);
    }

    /// \brief Get the schema ID to be used for next insert
    schema_id project_schema_id(const context& ctx) {
        // This is very simple because we only allow one write in
        // flight at a time.  Could be extended to track N in flight
        // operations if needed.  _next_schema_id gets updated
        // if the operation was successful, as a side effect
        // of applying the write to the store.
        return get_or_create_context_store(ctx)._next_schema_id;
    }

    chunked_vector<context> get_materialized_contexts() const {
        chunked_vector<context> result;
        result.push_back(default_context);
        for (const auto& [ctx, store] : _context_stores) {
            if (store._materialized && ctx != default_context) {
                result.push_back(ctx);
            }
        }
        return result;
    }

    bool is_context_materialized(const context& ctx) const {
        if (ctx == default_context) {
            return true;
        }
        auto it = _context_stores.find(ctx);
        return it != _context_stores.end() && it->second._materialized;
    }

    void set_context_materialized(const context& ctx, bool materialized) {
        get_or_create_context_store(ctx)._materialized = materialized;
    }

private:
    struct schema_entry {
        explicit schema_entry(schema_definition definition)
          : definition{std::move(definition)} {}

        schema_definition definition;
    };

    class subject_entry {
    public:
        explicit subject_entry(const context_subject& sub) {
            setup_metrics(sub);
        }
        std::optional<compatibility_level> compatibility;
        std::optional<mode> mode;
        chunked_vector<subject_version_entry> versions;
        is_deleted deleted{false};

        chunked_vector<seq_marker> written_at;

    private:
        metrics::internal_metric_groups _metrics;
        metrics::public_metric_groups _public_metrics;

        void setup_metrics(const context_subject& sub) {
            namespace sm = ss::metrics;
            auto group_name = prometheus_sanitize::metrics_name(
              "schema_registry_cache");
            const auto make_subject_version_count = [this,
                                                     &sub](is_deleted deleted) {
                return sm::make_gauge(
                  "subject_version_count",
                  [this, deleted] {
                      return std::ranges::count_if(
                        versions, [deleted](const subject_version_entry& v) {
                            return v.deleted == deleted;
                        });
                  },
                  sm::description("The number of versions in the subject"),
                  {
                    sm::label{"context"}(sub.ctx),
                    sm::label{"subject"}(sub.sub),
                    sm::label{"deleted"}(deleted),
                  });
            };
            if (!config::shard_local_cfg().disable_metrics()) {
                _metrics.add_group(
                  group_name,
                  {make_subject_version_count(is_deleted::no),
                   make_subject_version_count(is_deleted::yes)},
                  {},
                  {sm::shard_label});
            }
            if (!config::shard_local_cfg().disable_public_metrics()) {
                _public_metrics.add_group(
                  group_name,
                  {make_subject_version_count(is_deleted::no)
                     .aggregate({sm::shard_label}),
                   make_subject_version_count(is_deleted::yes)
                     .aggregate({sm::shard_label})});
            }
        }
    };
    using schema_map = absl::btree_map<context_schema_id, schema_entry>;
    using subject_map = absl::node_hash_map<context_subject, subject_entry>;

    subject_entry& get_or_create_subject_entry(context_subject sub) {
        return _subjects.try_emplace(sub, sub).first->second;
    }

    result<subject_map::iterator>
    get_subject_iter(const context_subject& sub, include_deleted inc_del) {
        const store* const_this = this;
        auto res = const_this->get_subject_iter(sub, inc_del);
        return detail::make_non_const_iterator(_subjects, res);
    }

    result<subject_map::const_iterator> get_subject_iter(
      const context_subject& sub, include_deleted inc_del) const {
        auto sub_it = _subjects.find(sub);
        if (sub_it == _subjects.end()) {
            return not_found(sub);
        }

        if (sub_it->second.deleted && !inc_del) {
            return not_found(sub);
        }
        return sub_it;
    }

    static result<chunked_vector<subject_version_entry>::iterator>
    get_version_iter(
      subject_map::value_type& sub_entry,
      schema_version version,
      include_deleted inc_del) {
        const subject_map::value_type& const_entry = sub_entry;
        // Get equivalent non-const iterator
        auto& v = sub_entry.second.versions;
        auto cit = BOOST_OUTCOME_TRYX(
          get_version_iter(const_entry, version, inc_del));
        return v.begin() + std::distance(v.cbegin(), cit);
    }

    static result<chunked_vector<subject_version_entry>::const_iterator>
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

    struct context_store {
        std::optional<compatibility_level> _compatibility{std::nullopt};
        std::optional<mode> _mode{std::nullopt};
        schema_id _next_schema_id{1};
        bool _materialized{false};

        void increment_schema_count() { _schema_count++; }

        void decrement_schema_count() {
            if (_schema_count > 0) {
                _schema_count--;
            }
        }

        void clear_schema_count() { _schema_count = 0; }

        void increment_subject_count(is_deleted deleted) {
            if (deleted == is_deleted::yes) {
                _subject_count_deleted++;
            } else {
                _subject_count_not_deleted++;
            }
        }

        void decrement_subject_count(is_deleted deleted) {
            if (deleted == is_deleted::yes) {
                if (_subject_count_deleted > 0) {
                    _subject_count_deleted--;
                }
            } else {
                if (_subject_count_not_deleted > 0) {
                    _subject_count_not_deleted--;
                }
            }
        }

        void update_subject_deleted_status(
          is_deleted old_status, is_deleted new_status) {
            if (old_status != new_status) {
                decrement_subject_count(old_status);
                increment_subject_count(new_status);
            }
        }

        void clear_subject_counts() {
            _subject_count_deleted = 0;
            _subject_count_not_deleted = 0;
        }

        chunked_vector<seq_marker> _config_written_at;
        chunked_vector<seq_marker> _mode_written_at;

    private:
        metrics::internal_metric_groups _metrics;
        metrics::public_metric_groups _public_metrics;
        size_t _schema_count{0};
        size_t _subject_count_not_deleted{0};
        size_t _subject_count_deleted{0};

        void setup_metrics(const context& ctx) {
            namespace sm = ss::metrics;
            auto group_name = prometheus_sanitize::metrics_name(
              "schema_registry_cache");

            const auto make_schema_count = [this, &ctx]() {
                return sm::make_gauge(
                  "schema_count",
                  [this] { return _schema_count; },
                  sm::description("The number of schemas in the store"),
                  {sm::label{"context"}(ctx)});
            };

            const auto make_subject_count = [this, &ctx](is_deleted deleted) {
                return sm::make_gauge(
                  "subject_count",
                  [this, deleted] {
                      return deleted == is_deleted::yes
                               ? _subject_count_deleted
                               : _subject_count_not_deleted;
                  },
                  sm::description("The number of subjects in the store"),
                  {sm::label{"context"}(ctx), sm::label{"deleted"}(deleted)});
            };

            if (!config::shard_local_cfg().disable_metrics()) {
                _metrics.add_group(
                  group_name,
                  {make_schema_count(),
                   make_subject_count(is_deleted::no),
                   make_subject_count(is_deleted::yes)},
                  {},
                  {sm::shard_label});
            }

            if (!config::shard_local_cfg().disable_public_metrics()) {
                _public_metrics.add_group(
                  group_name,
                  {make_schema_count().aggregate({sm::shard_label}),
                   make_subject_count(is_deleted::no)
                     .aggregate({sm::shard_label}),
                   make_subject_count(is_deleted::yes)
                     .aggregate({sm::shard_label})});
            }
        }

        friend class store;
    };
    using context_store_map = absl::node_hash_map<context, context_store>;

    context_store& get_or_create_context_store(const context& ctx) {
        auto [it, inserted] = _context_stores.try_emplace(ctx);
        if (inserted) {
            it->second.setup_metrics(ctx);
        }
        return it->second;
    }

    // NOTE: sharded_store shards data into multiple store instances, so some
    // fields are only present on certain shards.
    // _schemas: sharded by (context, schema_id)
    // _subjects: sharded by (context, subject)
    // _marked_schemas: sharded by (context, schema_id)
    // context_store:
    //  - next_schema_id: sharded by context
    //  - compatibility: replicated across all shards
    //  - mode: replicated across all shards
    //  - materialized: replicated across all shards
    //  - _config_written_at: replicated across all shards
    //  - _mode_written_at: replicated across all shards
    // _mutable: replicated across all shards

    // Alternative: Keep the store as is, but key the existing state under a
    // context. Yet shard state by (context, subject), (context, schema_id)
    // still.

    schema_map _schemas;
    subject_map _subjects;
    chunked_vector<context_schema_id> _marked_schemas;
    context_store_map _context_stores;

    is_mutable _mutable;
    metrics::internal_metric_groups _metrics;
    metrics::public_metric_groups _public_metrics;
};

} // namespace pandaproxy::schema_registry
