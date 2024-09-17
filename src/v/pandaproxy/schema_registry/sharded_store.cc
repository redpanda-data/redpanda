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

#include "pandaproxy/schema_registry/sharded_store.h"

#include "base/vlog.h"
#include "config/configuration.h"
#include "hashing/jump_consistent_hash.h"
#include "hashing/xx.h"
#include "pandaproxy/logger.h"
#include "pandaproxy/schema_registry/avro.h"
#include "pandaproxy/schema_registry/error.h"
#include "pandaproxy/schema_registry/errors.h"
#include "pandaproxy/schema_registry/exceptions.h"
#include "pandaproxy/schema_registry/json.h"
#include "pandaproxy/schema_registry/protobuf.h"
#include "pandaproxy/schema_registry/store.h"
#include "pandaproxy/schema_registry/types.h"
#include "pandaproxy/schema_registry/util.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/smp.hh>
#include <seastar/coroutine/exception.hh>

#include <absl/algorithm/container.h>
#include <fmt/core.h>

#include <functional>
#include <iterator>

namespace pandaproxy::schema_registry {

namespace {

ss::shard_id shard_for(const subject& sub) {
    auto hash = xxhash_64(sub().data(), sub().length());
    return jump_consistent_hash(hash, ss::smp::count);
}

ss::shard_id shard_for(schema_id id) {
    return jump_consistent_hash(id(), ss::smp::count);
}

compatibility_result check_compatible(
  const valid_schema& reader, const valid_schema& writer, verbose is_verbose) {
    return reader.visit([&](const auto& reader) -> compatibility_result {
        return writer.visit([&](const auto& writer) -> compatibility_result {
            if constexpr (std::is_same_v<decltype(reader), decltype(writer)>) {
                return check_compatible(reader, writer, is_verbose);
            }
            return {.is_compat = false};
        });
    });
}

constexpr auto set_accumulator =
  [](store::schema_id_set acc, store::schema_id_set refs) {
      acc.insert(refs.begin(), refs.end());
      return acc;
  };

} // namespace

ss::future<> sharded_store::start(is_mutable mut, ss::smp_service_group sg) {
    _smp_opts = ss::smp_submit_to_options{sg};
    return _store.start(mut);
}

ss::future<> sharded_store::stop() { return _store.stop(); }

ss::future<canonical_schema>
sharded_store::make_canonical_schema(unparsed_schema schema, normalize norm) {
    switch (schema.type()) {
    case schema_type::avro: {
        auto [sub, unparsed] = std::move(schema).destructure();
        co_return canonical_schema{
          std::move(sub),
          sanitize_avro_schema_definition(std::move(unparsed)).value()};
    }
    case schema_type::protobuf:
        co_return co_await make_canonical_protobuf_schema(
          *this, std::move(schema));
    case schema_type::json:
        co_return co_await make_canonical_json_schema(
          *this, std::move(schema), norm);
    }
    __builtin_unreachable();
}

ss::future<> sharded_store::validate_schema(canonical_schema schema) {
    switch (schema.type()) {
    case schema_type::avro: {
        co_await make_avro_schema_definition(*this, std::move(schema));
        co_return;
    }
    case schema_type::protobuf:
        co_await validate_protobuf_schema(*this, std::move(schema));
        co_return;
    case schema_type::json:
        co_await make_json_schema_definition((*this), std::move(schema));
        co_return;
    }
    __builtin_unreachable();
}

ss::future<valid_schema>
sharded_store::make_valid_schema(canonical_schema schema) {
    // This method seems to confuse clang 12.0.1
    // See #3596 for details, especially if modifying it.
    switch (schema.type()) {
    case schema_type::avro: {
        co_return co_await make_avro_schema_definition(
          *this, std::move(schema));
    }
    case schema_type::protobuf: {
        co_return co_await make_protobuf_schema_definition(
          *this, std::move(schema));
    }
    case schema_type::json:
        co_return co_await make_json_schema_definition(
          *this, std::move(schema));
    }
    throw as_exception(invalid_schema_type(schema.type()));
}

ss::future<sharded_store::has_schema_result>
sharded_store::get_schema_version(subject_schema schema) {
    // Validate the schema (may throw)
    co_await validate_schema(schema.schema.share());

    // Determine if the definition already exists
    auto map = [&schema](store& s) {
        return s.get_schema_id(schema.schema.def());
    };
    auto reduce = [](
                    std::optional<schema_id> acc,
                    std::optional<schema_id> s_id) { return acc ? acc : s_id; };
    auto s_id = co_await _store.map_reduce0(
      map, std::optional<schema_id>{}, reduce);

    // Determine if a provided schema id is appropriate
    if (schema.id != invalid_schema_id) {
        if (s_id.has_value() && s_id != schema.id) {
            co_return ss::coroutine::return_exception(exception(
              error_code::subject_version_schema_id_already_exists,
              fmt::format(
                "Schema already registered with id {} instead of input id {}",
                s_id.value()(),
                schema.id())));
        } else if (co_await has_schema(schema.id)) {
            // The supplied id already exists, but the schema is different
            co_return ss::coroutine::return_exception(exception(
              error_code::subject_version_schema_id_already_exists,
              fmt::format(
                "Overwrite new schema with id {} is not permitted.",
                schema.id())));
        } else {
            // Use the supplied id
            s_id = schema.id;
            vlog(plog.debug, "project_ids: using supplied ID {}", s_id.value());
        }
    } else if (s_id) {
        vlog(plog.debug, "project_ids: existing ID {}", s_id.value());
    }

    // Determine if the subject already has a version that references this
    // schema, deleted versions are seen.
    const auto& sub = schema.schema.sub();
    const auto versions = co_await _store.invoke_on(
      shard_for(sub),
      _smp_opts,
      [sub](auto& s) -> std::vector<subject_version_entry> {
          auto res = s.get_version_ids(sub, include_deleted::no);
          if (
            res.has_error()
            && res.assume_error().code() == error_code::subject_not_found) {
              return {};
          }
          return res.value();
      });

    std::optional<schema_version> v_id;
    if (s_id.has_value()) {
        auto v_it = absl::c_find_if(versions, [id = *s_id](const auto& s_id_v) {
            return s_id_v.id == id;
        });
        if (v_it != versions.end()) {
            v_id.emplace(v_it->version);
        }
    }

    // Check compatibility of the schema
    if (!v_id.has_value() && !versions.empty()) {
        auto compat = co_await is_compatible(
          versions.back().version, schema.schema.share(), verbose::yes);
        if (!compat.is_compat) {
            throw exception(
              error_code::schema_incompatible,
              fmt::format(
                "Schema being registered is incompatible with an earlier "
                "schema for subject \"{}\", details: [{}]",
                sub,
                fmt::join(compat.messages, ", ")));
        }
    }
    co_return has_schema_result{s_id, v_id};
}

ss::future<sharded_store::insert_result>
sharded_store::project_ids(subject_schema schema) {
    const auto& sub = schema.schema.sub();
    auto s_id = schema.id;
    if (s_id == invalid_schema_id) {
        // New schema, project an ID for it.
        s_id = co_await project_schema_id();
        vlog(plog.debug, "project_ids: projected new ID {}", s_id);
    }

    auto sub_shard{shard_for(sub)};
    auto v_id = co_await _store.invoke_on(
      sub_shard, _smp_opts, [sub, s_id](store& s) {
          return s.project_version(sub, s_id);
      });

    const bool is_new = v_id.has_value();
    if (is_new && schema.version != invalid_schema_version) {
        v_id = schema.version;
    }

    co_return insert_result{
      v_id.value_or(invalid_schema_version), s_id, is_new};
}

ss::future<bool> sharded_store::upsert(
  seq_marker marker,
  unparsed_schema schema,
  schema_id id,
  schema_version version,
  is_deleted deleted) {
    auto norm = normalize{
      config::shard_local_cfg().schema_registry_normalize_on_startup()};
    co_return co_await upsert(
      marker,
      co_await make_canonical_schema(std::move(schema), norm),
      id,
      version,
      deleted);
}

ss::future<bool> sharded_store::upsert(
  seq_marker marker,
  canonical_schema schema,
  schema_id id,
  schema_version version,
  is_deleted deleted) {
    auto [sub, def] = std::move(schema).destructure();
    co_await upsert_schema(id, std::move(def));
    co_return co_await upsert_subject(
      marker, std::move(sub), version, id, deleted);
}

ss::future<bool> sharded_store::has_schema(schema_id id) {
    co_return co_await _store.invoke_on(
      shard_for(id), _smp_opts, [id](store& s) {
          return s.get_schema_definition(id).has_value();
      });
}

ss::future<> sharded_store::delete_schema(schema_id id) {
    return _store.invoke_on(
      shard_for(id), _smp_opts, [id](store& s) { s.delete_schema(id); });
}

ss::future<subject_schema>
sharded_store::has_schema(canonical_schema schema, include_deleted inc_del) {
    auto versions = co_await get_versions(schema.sub(), inc_del);

    try {
        co_await validate_schema(schema.share());
    } catch (const exception& e) {
        throw as_exception(invalid_subject_schema(schema.sub()));
    }

    std::optional<subject_schema> sub_schema;
    for (auto ver : versions) {
        try {
            auto res = co_await get_subject_schema(schema.sub(), ver, inc_del);
            if (schema.def() == res.schema.def()) {
                sub_schema.emplace(std::move(res));
                break;
            }
        } catch (const exception& e) {
            if (
              e.code() == error_code::subject_not_found
              || e.code() == error_code::subject_version_not_found) {
            } else {
                throw;
            }
        }
    };
    if (!sub_schema.has_value()) {
        throw as_exception(schema_not_found());
    }
    co_return std::move(sub_schema).value();
}

ss::future<canonical_schema_definition>
sharded_store::get_schema_definition(schema_id id) {
    co_return co_await _store.invoke_on(
      shard_for(id), _smp_opts, [id](store& s) {
          return s.get_schema_definition(id).value();
      });
}

ss::future<chunked_vector<subject_version>>
sharded_store::get_schema_subject_versions(schema_id id) {
    using subject_versions = chunked_vector<subject_version>;
    auto map = [id](store& s) { return s.get_schema_subject_versions(id); };
    auto reduce = [](subject_versions acc, subject_versions svs) {
        acc.reserve(acc.size() + svs.size());
        std::move(svs.begin(), svs.end(), std::back_inserter(acc));
        return acc;
    };
    co_return co_await _store.map_reduce0(map, subject_versions{}, reduce);
}

ss::future<chunked_vector<subject>>
sharded_store::get_schema_subjects(schema_id id, include_deleted inc_del) {
    using subjects = chunked_vector<subject>;
    auto map = [id, inc_del](store& s) {
        return s.get_schema_subjects(id, inc_del);
    };
    auto reduce = [](subjects acc, subjects subs) {
        acc.reserve(acc.size() + subs.size());
        std::move(subs.begin(), subs.end(), std::back_inserter(acc));
        return acc;
    };
    auto subs = co_await _store.map_reduce0(map, subjects{}, reduce);
    absl::c_sort(subs);
    co_return subs;
}

ss::future<subject_schema> sharded_store::get_subject_schema(
  subject sub, std::optional<schema_version> version, include_deleted inc_del) {
    auto sub_shard{shard_for(sub)};
    auto v_id = co_await _store.invoke_on(
      sub_shard, _smp_opts, [sub, version, inc_del](store& s) {
          return s.get_subject_version_id(sub, version, inc_del).value();
      });

    auto def = co_await _store.invoke_on(
      shard_for(v_id.id), _smp_opts, [id{v_id.id}](store& s) {
          return s.get_schema_definition(id).value();
      });

    co_return subject_schema{
      .schema = {sub, std::move(def)},
      .version = v_id.version,
      .id = v_id.id,
      .deleted = v_id.deleted};
}

ss::future<chunked_vector<subject>> sharded_store::get_subjects(
  include_deleted inc_del, std::optional<ss::sstring> subject_prefix) {
    using subjects = chunked_vector<subject>;
    auto map = [inc_del, &subject_prefix](store& s) {
        return s.get_subjects(inc_del, subject_prefix);
    };
    auto reduce = [](subjects acc, subjects subs) {
        acc.reserve(acc.size() + subs.size());
        std::move(subs.begin(), subs.end(), std::back_inserter(acc));
        return acc;
    };
    co_return co_await _store.map_reduce0(map, subjects{}, reduce);
}

ss::future<bool> sharded_store::has_subjects(include_deleted inc_del) {
    auto map = [inc_del](store& s) { return s.has_subjects(inc_del); };
    return _store.map_reduce0(map, false, std::logical_or<>{});
}

ss::future<std::vector<schema_version>>
sharded_store::get_versions(subject sub, include_deleted inc_del) {
    auto sub_shard{shard_for(sub)};
    co_return co_await _store.invoke_on(
      sub_shard, _smp_opts, [sub{std::move(sub)}, inc_del](store& s) {
          return s.get_versions(sub, inc_del).value();
      });
}

ss::future<bool> sharded_store::is_referenced(subject sub, schema_version ver) {
    // Find all the schema that reference this sub-ver
    auto references = co_await _store.map_reduce0(
      [sub{std::move(sub)}, ver](store& s) {
          return s.referenced_by(sub, ver);
      },
      store::schema_id_set{},
      set_accumulator);

    // Find whether any subject version reference any of the schema
    co_return co_await _store.map_reduce0(
      [refs{std::move(references)}](store& s) {
          return s.subject_versions_has_any_of(refs, include_deleted::no);
      },
      false,
      std::logical_or<>{});
}

ss::future<std::vector<schema_id>> sharded_store::referenced_by(
  subject sub, std::optional<schema_version> opt_ver) {
    schema_version ver;
    // Ensure the subject exists
    auto versions = co_await get_versions(sub, include_deleted::no);
    if (opt_ver.has_value()) {
        ver = *opt_ver;
        auto version_not_found = std::none_of(
          versions.begin(), versions.end(), [ver](const auto& v) {
              return ver == v;
          });
        if (version_not_found) {
            throw as_exception(not_found(sub, ver));
        }
    } else {
        vassert(
          !versions.empty(), "get_versions should not return empty versions");
        ver = versions.back();
    }

    // Find all the schema that reference this sub-ver
    auto references = co_await _store.map_reduce0(
      [sub{std::move(sub)}, ver](store& s) {
          return s.referenced_by(sub, ver);
      },
      store::schema_id_set{},
      set_accumulator);

    // Find all the subject versions that reference any of the schema
    references = co_await _store.map_reduce0(
      [refs{std::move(references)}](store& s) {
          return s.subject_versions_with_any_of(refs);
      },
      store::schema_id_set{},
      set_accumulator);

    co_return std::vector<schema_id>{references.begin(), references.end()};
}

ss::future<std::vector<schema_version>> sharded_store::delete_subject(
  seq_marker marker, subject sub, permanent_delete permanent) {
    auto sub_shard{shard_for(sub)};
    co_return co_await _store.invoke_on(
      sub_shard, _smp_opts, [marker, sub{std::move(sub)}, permanent](store& s) {
          return s.delete_subject(marker, sub, permanent).value();
      });
}

ss::future<is_deleted> sharded_store::is_subject_deleted(subject sub) {
    auto sub_shard{shard_for(sub)};
    co_return co_await _store.invoke_on(
      sub_shard, _smp_opts, [sub{std::move(sub)}](store& s) {
          return s.is_subject_deleted(sub).value();
      });
}

ss::future<is_deleted>
sharded_store::is_subject_version_deleted(subject sub, schema_version ver) {
    auto sub_shard{shard_for(sub)};
    co_return co_await _store.invoke_on(
      sub_shard, _smp_opts, [sub{std::move(sub)}, ver](store& s) {
          return s.is_subject_version_deleted(sub, ver).value();
      });
}

ss::future<std::vector<seq_marker>>
sharded_store::get_subject_written_at(subject sub) {
    auto sub_shard{shard_for(sub)};
    co_return co_await _store.invoke_on(
      sub_shard, _smp_opts, [sub{std::move(sub)}](store& s) {
          return s.store::get_subject_written_at(sub).value();
      });
}

ss::future<std::vector<seq_marker>>
sharded_store::get_subject_config_written_at(subject sub) {
    auto sub_shard{shard_for(sub)};
    co_return co_await _store.invoke_on(
      sub_shard, _smp_opts, [sub{std::move(sub)}](store& s) {
          return s.store::get_subject_config_written_at(sub).value();
      });
}

ss::future<std::vector<seq_marker>>
sharded_store::get_subject_mode_written_at(subject sub) {
    auto sub_shard{shard_for(sub)};
    co_return co_await _store.invoke_on(
      sub_shard, _smp_opts, [sub{std::move(sub)}](store& s) {
          return s.store::get_subject_mode_written_at(sub).value();
      });
}

ss::future<std::vector<seq_marker>>
sharded_store::get_subject_version_written_at(subject sub, schema_version ver) {
    auto sub_shard{shard_for(sub)};
    co_return co_await _store.invoke_on(
      sub_shard, _smp_opts, [sub{std::move(sub)}, ver](store& s) {
          return s.get_subject_version_written_at(sub, ver).value();
      });
}

ss::future<bool> sharded_store::delete_subject_version(
  subject sub, schema_version ver, force force) {
    auto sub_shard = shard_for(sub);
    auto [schema_id, result] = co_await _store.invoke_on(
      sub_shard, _smp_opts, [sub{std::move(sub)}, ver, force](store& s) {
          auto schema_id = s.get_subject_version_id(
                              sub, ver, include_deleted::yes)
                             .value()
                             .id;
          auto result = s.delete_subject_version(sub, ver, force).value();
          return std::make_pair(schema_id, result);
      });

    auto remaining_subjects_exist = co_await _store.map_reduce0(
      [schema_id](store& s) {
          return s.subject_versions_has_any_of(
            {schema_id}, include_deleted::yes);
      },
      false,
      std::logical_or{});

    if (!remaining_subjects_exist) {
        co_await delete_schema(schema_id);
    }

    co_return result;
}

ss::future<mode> sharded_store::get_mode() {
    co_return _store.local().get_mode().value();
}

ss::future<mode>
sharded_store::get_mode(subject sub, default_to_global fallback) {
    auto sub_shard{shard_for(sub)};
    co_return co_await _store.invoke_on(
      sub_shard, [sub{std::move(sub)}, fallback](store& s) {
          return s.get_mode(sub, fallback).value();
      });
}

ss::future<bool> sharded_store::set_mode(mode m, force f) {
    auto map = [m, f](store& s) { return s.set_mode(m, f).value(); };
    auto reduce = std::logical_and<>{};
    co_return co_await _store.map_reduce0(map, true, reduce);
}

ss::future<bool>
sharded_store::set_mode(seq_marker marker, subject sub, mode m, force f) {
    auto sub_shard{shard_for(sub)};
    co_return co_await _store.invoke_on(
      sub_shard, _smp_opts, [marker, sub{std::move(sub)}, m, f](store& s) {
          return s.set_mode(marker, sub, m, f).value();
      });
}

ss::future<bool>
sharded_store::clear_mode(seq_marker marker, subject sub, force f) {
    auto sub_shard{shard_for(sub)};
    co_return co_await _store.invoke_on(
      sub_shard, _smp_opts, [marker, sub{std::move(sub)}, f](store& s) {
          return s.clear_mode(marker, sub, f).value();
      });
}

ss::future<compatibility_level> sharded_store::get_compatibility() {
    co_return _store.local().get_compatibility().value();
}

ss::future<compatibility_level>
sharded_store::get_compatibility(subject sub, default_to_global fallback) {
    auto sub_shard{shard_for(sub)};
    co_return co_await _store.invoke_on(
      sub_shard, [sub{std::move(sub)}, fallback](store& s) {
          return s.get_compatibility(sub, fallback).value();
      });
}

ss::future<bool>
sharded_store::set_compatibility(compatibility_level compatibility) {
    auto map = [compatibility](store& s) {
        return s.set_compatibility(compatibility).value();
    };
    auto reduce = std::logical_and<>{};
    co_return co_await _store.map_reduce0(map, true, reduce);
}

ss::future<bool> sharded_store::set_compatibility(
  seq_marker marker, subject sub, compatibility_level compatibility) {
    auto sub_shard{shard_for(sub)};
    co_return co_await _store.invoke_on(
      sub_shard,
      _smp_opts,
      [marker, sub{std::move(sub)}, compatibility](store& s) {
          return s.set_compatibility(marker, sub, compatibility).value();
      });
}

ss::future<bool>
sharded_store::clear_compatibility(seq_marker marker, subject sub) {
    auto sub_shard{shard_for(sub)};
    co_return co_await _store.invoke_on(
      sub_shard, _smp_opts, [marker, sub{std::move(sub)}](store& s) {
          return s.clear_compatibility(marker, sub).value();
      });
}

ss::future<bool>
sharded_store::upsert_schema(schema_id id, canonical_schema_definition def) {
    co_await maybe_update_max_schema_id(id);
    co_return co_await _store.invoke_on(
      shard_for(id), _smp_opts, [id, def{std::move(def)}](store& s) mutable {
          return s.upsert_schema(id, std::move(def));
      });
}

ss::future<sharded_store::insert_subject_result>
sharded_store::insert_subject(subject sub, schema_id id) {
    auto sub_shard{shard_for(sub)};
    auto [version, inserted] = co_await _store.invoke_on(
      sub_shard, _smp_opts, [sub{std::move(sub)}, id](store& s) mutable {
          return s.insert_subject(sub, id);
      });
    co_return insert_subject_result{version, inserted};
}

ss::future<bool> sharded_store::upsert_subject(
  seq_marker marker,
  subject sub,
  schema_version version,
  schema_id id,
  is_deleted deleted) {
    auto sub_shard{shard_for(sub)};
    co_return co_await _store.invoke_on(
      sub_shard,
      _smp_opts,
      [marker, sub{std::move(sub)}, version, id, deleted](store& s) mutable {
          return s.upsert_subject(marker, std::move(sub), version, id, deleted);
      });
}

/// \brief Get the schema ID to be used for next insert
ss::future<schema_id> sharded_store::project_schema_id() {
    // This is very simple because we only allow one write in
    // flight at a time.  Could be extended to track N in flight
    // operations if needed.  _next_schema_id gets updated
    // if the operation was successful, as a side effect
    // of applying the write to the store.
    auto fetch = [this] { return _next_schema_id; };
    co_return co_await ss::smp::submit_to(
      ss::shard_id{0}, _smp_opts, std::move(fetch));
}

ss::future<> sharded_store::maybe_update_max_schema_id(schema_id id) {
    auto update = [this, id] {
        auto old = _next_schema_id;
        _next_schema_id = std::max(_next_schema_id, id + 1);
        vlog(
          plog.debug,
          "maybe_update_max_schema_id: {}->{}",
          old,
          _next_schema_id);
    };
    co_return co_await ss::smp::submit_to(
      ss::shard_id{0}, _smp_opts, std::move(update));
}

ss::future<bool> sharded_store::is_compatible(
  schema_version version, canonical_schema new_schema) {
    auto rslt = co_await do_is_compatible(
      version, std::move(new_schema), verbose::no);
    co_return rslt.is_compat;
}

ss::future<compatibility_result> sharded_store::is_compatible(
  schema_version version, canonical_schema new_schema, verbose is_verbose) {
    return do_is_compatible(version, std::move(new_schema), is_verbose);
}

ss::future<compatibility_result> sharded_store::do_is_compatible(
  schema_version version, canonical_schema new_schema, verbose is_verbose) {
    // Lookup the version_ids
    const auto sub = new_schema.sub();
    const auto versions = co_await _store.invoke_on(
      shard_for(sub), _smp_opts, [sub](auto& s) {
          return s.get_version_ids(sub, include_deleted::no).value();
      });

    auto ver_it = std::lower_bound(
      versions.begin(),
      versions.end(),
      version,
      [](const subject_version_entry& lhs, schema_version rhs) {
          return lhs.version < rhs;
      });
    if (ver_it == versions.end() || ver_it->version != version) {
        throw as_exception(not_found(sub, version));
    }
    if (ver_it->deleted) {
        throw as_exception(not_found(sub, version));
    }

    // Lookup the schema at the version
    auto old_schema = co_await get_subject_schema(
      sub, version, include_deleted::no);

    // Lookup the compatibility level
    auto compat = co_await get_compatibility(sub, default_to_global::yes);

    // Types must always match
    if (old_schema.schema.type() != new_schema.type()) {
        compatibility_result result{.is_compat = false};
        if (is_verbose) {
            result.messages = {
              "Incompatible because of different schema type",
              fmt::format("{{compatibility: {}}}", compat)};
        }
        co_return result;
    }

    if (compat == compatibility_level::none) {
        co_return compatibility_result{.is_compat = true};
    }

    // Currently support JSON, PROTOBUF, AVRO
    if (![type = new_schema.type()] {
            switch (type) {
            case schema_type::avro:
            case schema_type::protobuf:
            case schema_type::json:
                return true;
            }
            return false;
        }()) {
        throw as_exception(invalid_schema_type(new_schema.type()));
    }

    // search backwards
    // if transitive, search all, seach until version
    if (
      compat == compatibility_level::backward_transitive
      || compat == compatibility_level::forward_transitive
      || compat == compatibility_level::full_transitive) {
        ver_it = versions.begin();
    }

    auto it = std::reverse_iterator(versions.end());
    auto it_end = std::reverse_iterator(ver_it);

    auto new_valid = co_await make_valid_schema(std::move(new_schema));

    compatibility_result result{.is_compat = true};

    auto formatter = [](std::string_view rdr, std::string_view wrtr) {
        return [rdr, wrtr](std::string_view msg) {
            return fmt::format(
              fmt::runtime(msg),
              fmt::arg("reader", rdr),
              fmt::arg("writer", wrtr));
        };
    };

    for (; result.is_compat && it != it_end; ++it) {
        if (it->deleted) {
            continue;
        }

        auto old_schema = co_await get_subject_schema(
          sub, it->version, include_deleted::no);
        auto old_valid = co_await make_valid_schema(
          std::move(old_schema.schema));

        std::vector<ss::sstring> version_messages;

        if (
          compat == compatibility_level::backward
          || compat == compatibility_level::backward_transitive
          || compat == compatibility_level::full
          || compat == compatibility_level::full_transitive) {
            auto r = check_compatible(new_valid, old_valid, is_verbose);
            result.is_compat = result.is_compat && r.is_compat;
            version_messages.reserve(
              version_messages.size() + r.messages.size());
            std::transform(
              std::make_move_iterator(r.messages.begin()),
              std::make_move_iterator(r.messages.end()),
              std::back_inserter(version_messages),
              formatter("new", "old"));
        }
        if (
          compat == compatibility_level::forward
          || compat == compatibility_level::forward_transitive
          || compat == compatibility_level::full
          || compat == compatibility_level::full_transitive) {
            auto r = check_compatible(old_valid, new_valid, is_verbose);
            result.is_compat = result.is_compat && r.is_compat;
            version_messages.reserve(
              version_messages.size() + r.messages.size());
            std::transform(
              std::make_move_iterator(r.messages.begin()),
              std::make_move_iterator(r.messages.end()),
              std::back_inserter(version_messages),
              formatter("old", "new"));
        }

        if (is_verbose && !result.is_compat) {
            version_messages.emplace_back(
              fmt::format("{{oldSchemaVersion: {}}}", old_schema.version));
            version_messages.emplace_back(
              fmt::format("{{oldSchema: '{}'}}", to_string(old_valid.raw())));
            version_messages.emplace_back(
              fmt::format("{{compatibility: '{}'}}", compat));
        }

        result.messages.reserve(
          result.messages.size() + version_messages.size());
        std::move(
          version_messages.begin(),
          version_messages.end(),
          std::back_inserter(result.messages));
    }
    co_return result;
}

void sharded_store::check_mode_mutability(force f) const {
    _store.local().check_mode_mutability(f).value();
}

ss::future<bool> sharded_store::has_version(
  const subject& sub, schema_id id, include_deleted i) {
    auto sub_shard{shard_for(sub)};
    auto has_id = co_await _store.invoke_on(
      sub_shard, _smp_opts, [id, sub, i](class store& s) mutable {
          return s.has_version(sub, id, i);
      });
    co_return has_id.has_value() && has_id.assume_value();
}

} // namespace pandaproxy::schema_registry
