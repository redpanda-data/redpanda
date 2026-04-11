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
#include "container/chunked_vector.h"
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
#include <seastar/coroutine/as_future.hh>
#include <seastar/coroutine/exception.hh>

#include <fmt/core.h>

#include <algorithm>
#include <functional>
#include <iterator>

namespace pandaproxy::schema_registry {

namespace {

ss::shard_id shard_for(const context_subject& sub) {
    auto hasher = incremental_xxhash64{};
    hasher.update(sub.ctx());
    hasher.update(sub.sub());
    return jump_consistent_hash(hasher.digest(), ss::smp::count);
}

ss::shard_id shard_for(const context_schema_id& id) {
    auto hasher = incremental_xxhash64{};
    hasher.update(id.ctx());
    hasher.update(id.id());
    return jump_consistent_hash(hasher.digest(), ss::smp::count);
}

ss::shard_id shard_for(const context& ctx) {
    auto hash = xxhash_64(ctx().data(), ctx().length());
    return jump_consistent_hash(hash, ss::smp::count);
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

ss::future<subject_schema> sharded_store::make_canonical_schema(
  subject_schema schema,
  normalize norm,
  bool consider_always_normalize_config) {
    if (consider_always_normalize_config) {
        norm = norm
               || normalize{
                 config::shard_local_cfg().schema_registry_always_normalize()};
    }
    switch (schema.type()) {
    case schema_type::avro:
        co_return co_await make_canonical_avro_schema(
          *this, std::move(schema), norm);
    case schema_type::protobuf:
        co_return co_await make_canonical_protobuf_schema(
          *this, std::move(schema), norm);
    case schema_type::json:
        co_return co_await make_canonical_json_schema(
          *this, std::move(schema), norm);
    }
    __builtin_unreachable();
}

ss::future<> sharded_store::validate_schema(subject_schema schema) {
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

ss::future<schema_definition>
sharded_store::format_schema(schema_definition schema, output_format format) {
    switch (schema.type()) {
    case schema_type::avro:
        co_return co_await format_avro_schema_definition(
          *this, std::move(schema), format);
    case schema_type::protobuf:
        co_return co_await format_protobuf_schema_definition(
          *this, std::move(schema), format);
    case schema_type::json:
        // json doesn't act on format parameter
        co_return schema;
    }
}

ss::future<valid_schema>
sharded_store::make_valid_schema(subject_schema schema) {
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

ss::future<sharded_store::insert_result>
sharded_store::project_ids(stored_schema schema) {
    const auto& sub = schema.schema.sub();
    auto s_id = schema.id;
    if (s_id == invalid_schema_id) {
        // New schema, project an ID for it.
        s_id = co_await project_schema_id(sub.ctx);
        vlog(
          srlog.debug,
          "project_ids (context: {}): projected new ID {}",
          sub.ctx,
          s_id);
    }

    auto ctx_sub = sub;
    auto sub_shard{shard_for(ctx_sub)};
    auto v_id = co_await _store.invoke_on(
      sub_shard, _smp_opts, [ctx_sub, s_id](store& s) {
          return s.project_version(ctx_sub, s_id);
      });

    const bool is_new = v_id.has_value();
    if (is_new && schema.version != invalid_schema_version) {
        const auto mode = co_await get_mode(ctx_sub, default_to_global::yes);
        if (v_id != schema.version && mode != mode::import) {
            throw as_exception(
              error_info{
                error_code::schema_version_not_next,
                "Version is not one more than previous version"});
        }

        v_id = schema.version;
    }

    co_return insert_result{
      v_id.value_or(invalid_schema_version), s_id, is_new};
}

ss::future<bool> sharded_store::upsert(
  seq_marker marker,
  subject_schema schema,
  schema_id id,
  schema_version version,
  is_deleted deleted) {
    auto canonical_fut = co_await ss::coroutine::as_future(
      make_canonical_schema(schema.share(), normalize::no, false));
    bool processing_failed = canonical_fut.failed();
    if (processing_failed) {
        canonical_fut.ignore_ready_future();
    } else {
        schema = canonical_fut.get();
    }

    auto [sub, def] = std::move(schema).destructure();
    // mark schemas that failed to be processed here. They will be given
    // one more chance once we have loaded all the topic to the store.
    co_await upsert_schema(
      context_schema_id{sub.ctx, id}, std::move(def), processing_failed);
    co_return co_await upsert_subject(
      marker, std::move(sub), version, id, deleted);
}

ss::future<> sharded_store::process_marked_schemas() {
    return _store.invoke_on_all([this](store& store) {
        return ss::do_with(
          store.extract_marked_schemas(), [this, &store](auto& marked) {
              return ss::do_for_each(marked, [this, &store](auto id) {
                  auto schema = store.get_schema_definition(id);
                  if (schema.has_failure()) {
                      // schema not found, ignore
                      return ss::now();
                  }
                  return make_canonical_schema(
                           {context_subject{id.ctx, subject{}},
                            std::move(schema).assume_value()},
                           normalize::no,
                           false)
                    .then([id, &store](auto canonical) {
                        // Update the stored form of this schema to its
                        // canonical form
                        auto [sub, schema] = std::move(canonical).destructure();
                        store.upsert_schema(id, std::move(schema), false);
                    })
                    .handle_exception([id](const std::exception_ptr& ep) {
                        // processing attempt failed on marked schema. This is
                        // not an issue of forward references. Ignore error and
                        // keep schema in the store as-is
                        vlog(
                          srlog.debug,
                          "process_marked_schemas failed for ctx={} id={}: {}",
                          id.ctx,
                          id.id,
                          ep);
                    });
              });
          });
    });
}

ss::future<bool> sharded_store::has_schema(context_schema_id id) {
    co_return co_await _store.invoke_on(
      shard_for(id), _smp_opts, [id](store& s) {
          return s.get_schema_definition(id).has_value();
      });
}

ss::future<> sharded_store::delete_schema(context_schema_id id) {
    return _store.invoke_on(
      shard_for(id), _smp_opts, [id](store& s) { s.delete_schema(id); });
}

ss::future<stored_schema>
sharded_store::has_schema(subject_schema schema, include_deleted inc_del) {
    auto versions = co_await get_subject_versions(schema.sub(), inc_del);

    try {
        co_await validate_schema(schema.share());
    } catch (const exception& e) {
        throw as_exception(invalid_subject_schema(schema.sub()));
    }

    // Find the maximal id schema with the given definition
    std::ranges::sort(versions, std::greater<>{}, [](const auto& v) {
        return std::tie(v.id, v.version);
    });
    for (auto entry : versions) {
        try {
            auto def = co_await get_schema_definition(
              {schema.sub().ctx, entry.id});
            if (schema.def() == def) {
                co_return stored_schema{
                  .schema = {schema.sub(), std::move(def)},
                  .version = entry.version,
                  .id = entry.id,
                  .deleted = entry.deleted};
            }
        } catch (const exception& e) {
            if (
              // Stored schemas might be invalid if imported improperly
              e.code() == error_code::schema_invalid) {
                vlog(
                  srlog.warn,
                  "Failed to parse stored schema, subject '{}', version {}. "
                  "Error: {}",
                  schema.sub(),
                  entry.version,
                  e.what());
            } else {
                throw;
            }
        }
    }
    throw as_exception(schema_not_found());
}

ss::future<std::optional<schema_definition>>
sharded_store::maybe_get_schema_definition(context_schema_id id) {
    co_return co_await _store.invoke_on(
      shard_for(id),
      _smp_opts,
      [id](store& s) -> std::optional<schema_definition> {
          auto s_res = s.get_schema_definition(id);
          if (
            s_res.has_error()
            && s_res.error().code() == error_code::schema_id_not_found) {
              return std::nullopt;
          }
          return std::move(s_res.value());
      });
}

ss::future<schema_definition>
sharded_store::get_schema_definition(context_schema_id id) {
    return get_schema_definition(id, output_format::none);
}

ss::future<schema_definition> sharded_store::get_schema_definition(
  context_schema_id id, output_format format) {
    auto def = co_await _store.invoke_on(
      shard_for(id), _smp_opts, [id](store& s) {
          return s.get_schema_definition(id).value();
      });

    co_return co_await format_schema(std::move(def), format);
}

ss::future<chunked_vector<subject_version>>
sharded_store::get_schema_subject_versions(context_schema_id id) {
    using subject_versions = chunked_vector<subject_version>;
    auto map = [id](store& s) { return s.get_schema_subject_versions(id); };
    auto reduce = [](subject_versions acc, subject_versions svs) {
        acc.reserve(acc.size() + svs.size());
        std::move(svs.begin(), svs.end(), std::back_inserter(acc));
        return acc;
    };
    co_return co_await _store.map_reduce0(map, subject_versions{}, reduce);
}

ss::future<chunked_vector<subject_version_entry>>
sharded_store::get_subject_versions(
  context_subject sub, include_deleted inc_del) {
    co_return co_await _store.invoke_on(
      shard_for(sub),
      _smp_opts,
      [sub, inc_del](store& s) -> chunked_vector<subject_version_entry> {
          auto res = s.get_version_ids(sub, inc_del);
          if (
            res.has_error()
            && res.assume_error().code() == error_code::subject_not_found) {
              return {};
          }
          return std::move(res.value());
      });
}

ss::future<chunked_vector<context_subject>> sharded_store::get_schema_subjects(
  context_schema_id id, include_deleted inc_del) {
    using subjects = chunked_vector<context_subject>;
    auto map = [id, inc_del](store& s) {
        return s.get_schema_subjects(id, inc_del);
    };
    auto reduce = [](subjects acc, subjects subs) {
        acc.reserve(acc.size() + subs.size());
        std::move(subs.begin(), subs.end(), std::back_inserter(acc));
        return acc;
    };
    auto subs = co_await _store.map_reduce0(map, subjects{}, reduce);
    std::ranges::sort(subs);
    co_return subs;
}

ss::future<context_schema_id> sharded_store::get_id(
  context_subject sub, std::optional<schema_version> version) {
    auto v_id = co_await _store.invoke_on(
      shard_for(sub), _smp_opts, [sub, version](store& s) {
          return s.get_subject_version_id(sub, version, include_deleted::yes)
            .value();
      });

    co_return context_schema_id{sub.ctx, v_id.id};
}

ss::future<stored_schema> sharded_store::get_subject_schema(
  context_subject sub,
  std::optional<schema_version> version,
  include_deleted inc_del) {
    auto sub_shard{shard_for(sub)};
    auto v_id = co_await _store.invoke_on(
      sub_shard, _smp_opts, [sub, version, inc_del](store& s) {
          return s.get_subject_version_id(sub, version, inc_del).value();
      });
    auto ctx_id = context_schema_id{sub.ctx, v_id.id};
    auto ctx_id_shard = shard_for(ctx_id);
    auto def = co_await _store.invoke_on(
      ctx_id_shard, _smp_opts, [ctx_id{std::move(ctx_id)}](store& s) {
          return s.get_schema_definition(ctx_id).value();
      });

    co_return stored_schema{
      .schema = {std::move(sub), std::move(def)},
      .version = v_id.version,
      .id = v_id.id,
      .deleted = v_id.deleted};
}

ss::future<chunked_vector<context_subject>> sharded_store::get_subjects(
  include_deleted inc_del, std::optional<ss::sstring> subject_prefix) {
    using subjects = chunked_vector<context_subject>;
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

ss::future<bool>
sharded_store::has_subjects(context ctx, include_deleted inc_del) {
    auto map = [ctx, inc_del](store& s) {
        return s.has_subjects(ctx, inc_del);
    };
    return _store.map_reduce0(map, false, std::logical_or<>{});
}

ss::future<chunked_vector<schema_version>>
sharded_store::get_versions(context_subject sub, include_deleted inc_del) {
    auto sub_shard{shard_for(sub)};
    co_return co_await _store.invoke_on(
      sub_shard, _smp_opts, [sub{std::move(sub)}, inc_del](store& s) {
          return s.get_versions(sub, inc_del).value();
      });
}

ss::future<bool> sharded_store::is_referenced(
  context_subject sub, std::optional<schema_version> ver) {
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

ss::future<chunked_vector<context_schema_id>> sharded_store::referenced_by(
  context_subject sub, std::optional<schema_version> opt_ver) {
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

    co_return chunked_vector<context_schema_id>{
      references.begin(), references.end()};
}

ss::future<chunked_vector<schema_version>> sharded_store::delete_subject(
  seq_marker marker, context_subject sub, permanent_delete permanent) {
    auto sub_shard{shard_for(sub)};
    co_return co_await _store.invoke_on(
      sub_shard, _smp_opts, [marker, sub{std::move(sub)}, permanent](store& s) {
          return s.delete_subject(marker, sub, permanent).value();
      });
}

ss::future<is_deleted> sharded_store::is_subject_deleted(context_subject sub) {
    auto sub_shard{shard_for(sub)};
    co_return co_await _store.invoke_on(
      sub_shard, _smp_opts, [sub{std::move(sub)}](store& s) {
          return s.is_subject_deleted(sub).value();
      });
}

ss::future<is_deleted> sharded_store::is_subject_version_deleted(
  context_subject sub, schema_version ver) {
    auto sub_shard{shard_for(sub)};
    co_return co_await _store.invoke_on(
      sub_shard, _smp_opts, [sub{std::move(sub)}, ver](store& s) {
          return s.is_subject_version_deleted(sub, ver).value();
      });
}

ss::future<chunked_vector<seq_marker>>
sharded_store::get_subject_written_at(context_subject sub) {
    auto sub_shard{shard_for(sub)};
    co_return co_await _store.invoke_on(
      sub_shard, _smp_opts, [sub{std::move(sub)}](store& s) {
          return s.store::get_subject_written_at(sub).value();
      });
}

ss::future<chunked_vector<seq_marker>>
sharded_store::get_subject_config_written_at(context_subject sub) {
    auto sub_shard{shard_for(sub)};
    co_return co_await _store.invoke_on(
      sub_shard, _smp_opts, [sub{std::move(sub)}](store& s) {
          return s.store::get_subject_config_written_at(sub).value();
      });
}

ss::future<chunked_vector<seq_marker>>
sharded_store::get_subject_mode_written_at(context_subject sub) {
    auto sub_shard{shard_for(sub)};
    co_return co_await _store.invoke_on(
      sub_shard, _smp_opts, [sub{std::move(sub)}](store& s) {
          return s.store::get_subject_mode_written_at(sub).value();
      });
}

ss::future<chunked_vector<seq_marker>>
sharded_store::get_subject_version_written_at(
  context_subject sub, schema_version ver) {
    auto sub_shard{shard_for(sub)};
    co_return co_await _store.invoke_on(
      sub_shard, _smp_opts, [sub{std::move(sub)}, ver](store& s) {
          return s.get_subject_version_written_at(sub, ver).value();
      });
}

ss::future<bool> sharded_store::delete_subject_version(
  context_subject sub, schema_version ver, force force) {
    auto sub_shard = shard_for(sub);
    auto [schema_id, result] = co_await _store.invoke_on(
      sub_shard, _smp_opts, [sub{std::move(sub)}, ver, force](store& s) {
          auto schema_id = s.get_subject_version_id(
                              sub, ver, include_deleted::yes)
                             .value()
                             .id;
          auto result = s.delete_subject_version(sub, ver, force).value();
          return std::make_pair(context_schema_id{sub.ctx, schema_id}, result);
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

ss::future<mode>
sharded_store::get_mode(context ctx, default_to_global fallback) {
    co_return _store.local().get_mode(ctx, fallback).value();
}

ss::future<mode>
sharded_store::get_mode(context_subject sub, default_to_global fallback) {
    auto sub_shard{shard_for(sub)};
    co_return co_await _store.invoke_on(
      sub_shard, [sub{std::move(sub)}, fallback](store& s) {
          return s.get_mode(sub, fallback).value();
      });
}

ss::future<bool>
sharded_store::set_mode(seq_marker marker, context ctx, mode m, force f) {
    auto map = [marker, m, f, ctx{std::move(ctx)}](store& s) {
        return s.set_mode(marker, ctx, m, f).value();
    };
    auto reduce = std::logical_and<>{};
    co_return co_await _store.map_reduce0(map, true, reduce);
}

ss::future<bool> sharded_store::set_mode(
  seq_marker marker, context_subject sub, mode m, force f) {
    auto sub_shard{shard_for(sub)};
    co_return co_await _store.invoke_on(
      sub_shard, _smp_opts, [marker, sub{std::move(sub)}, m, f](store& s) {
          return s.set_mode(marker, sub, m, f).value();
      });
}

ss::future<bool> sharded_store::clear_mode(context ctx, force f) {
    auto map = [f, ctx{std::move(ctx)}](store& s) {
        return s.clear_mode(ctx, f).value();
    };
    auto reduce = std::logical_and<>{};
    co_return co_await _store.map_reduce0(map, true, reduce);
}

ss::future<bool>
sharded_store::clear_mode(seq_marker marker, context_subject sub, force f) {
    auto sub_shard{shard_for(sub)};
    co_return co_await _store.invoke_on(
      sub_shard, _smp_opts, [marker, sub{std::move(sub)}, f](store& s) {
          return s.clear_mode(marker, sub, f).value();
      });
}

ss::future<chunked_vector<seq_marker>>
sharded_store::get_context_mode_written_at(context ctx) {
    co_return _store.local().get_context_mode_written_at(ctx).value();
}

ss::future<compatibility_level>
sharded_store::get_compatibility(context ctx, default_to_global fallback) {
    co_return _store.local().get_compatibility(ctx, fallback).value();
}

ss::future<compatibility_level> sharded_store::get_compatibility(
  context_subject sub, default_to_global fallback) {
    auto sub_shard{shard_for(sub)};
    co_return co_await _store.invoke_on(
      sub_shard, [sub{std::move(sub)}, fallback](store& s) {
          return s.get_compatibility(sub, fallback).value();
      });
}

ss::future<bool> sharded_store::set_compatibility(
  seq_marker marker, context ctx, compatibility_level compatibility) {
    auto map = [marker, compatibility, ctx{std::move(ctx)}](store& s) {
        return s.set_compatibility(marker, ctx, compatibility).value();
    };
    auto reduce = std::logical_and<>{};
    co_return co_await _store.map_reduce0(map, true, reduce);
}

ss::future<bool> sharded_store::set_compatibility(
  seq_marker marker, context_subject sub, compatibility_level compatibility) {
    auto sub_shard{shard_for(sub)};
    co_return co_await _store.invoke_on(
      sub_shard,
      _smp_opts,
      [marker, sub{std::move(sub)}, compatibility](store& s) {
          return s.set_compatibility(marker, sub, compatibility).value();
      });
}

ss::future<bool> sharded_store::clear_compatibility(context ctx) {
    auto map = [ctx{std::move(ctx)}](store& s) {
        return s.clear_compatibility(ctx).value();
    };
    auto reduce = std::logical_and<>{};
    co_return co_await _store.map_reduce0(map, true, reduce);
}

ss::future<bool>
sharded_store::clear_compatibility(seq_marker marker, context_subject sub) {
    auto sub_shard{shard_for(sub)};
    co_return co_await _store.invoke_on(
      sub_shard, _smp_opts, [marker, sub{std::move(sub)}](store& s) {
          return s.clear_compatibility(marker, sub).value();
      });
}

ss::future<chunked_vector<seq_marker>>
sharded_store::get_context_config_written_at(context ctx) {
    co_return _store.local().get_context_config_written_at(ctx).value();
}

ss::future<bool> sharded_store::upsert_schema(
  context_schema_id id, schema_definition def, bool mark_schema) {
    co_await _store.invoke_on(shard_for(id.ctx), _smp_opts, [id](store& s) {
        s.maybe_update_max_schema_id(id);
    });

    co_return co_await _store.invoke_on(
      shard_for(id),
      _smp_opts,
      [id, mark_schema, def{std::move(def)}](store& s) mutable {
          return s.upsert_schema(id, std::move(def), mark_schema);
      });
}

ss::future<bool> sharded_store::upsert_subject(
  seq_marker marker,
  context_subject sub,
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
ss::future<schema_id> sharded_store::project_schema_id(context ctx) {
    co_return co_await _store.invoke_on(
      shard_for(ctx), _smp_opts, [ctx](store& s) {
          return s.project_schema_id(ctx);
      });
}

ss::future<bool> sharded_store::is_compatible(
  schema_version version, subject_schema new_schema) {
    auto rslt = co_await do_is_compatible(
      version, std::move(new_schema), verbose::no);
    co_return rslt.is_compat;
}

ss::future<compatibility_result> sharded_store::is_compatible(
  schema_version version, subject_schema new_schema, verbose is_verbose) {
    return do_is_compatible(version, std::move(new_schema), is_verbose);
}

ss::future<compatibility_result> sharded_store::do_is_compatible(
  schema_version version, subject_schema new_schema, verbose is_verbose) {
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

        chunked_vector<ss::sstring> version_messages;

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
  context_subject sub, schema_id id, include_deleted i) {
    auto sub_shard{shard_for(sub)};
    auto has_id = co_await _store.invoke_on(
      sub_shard, _smp_opts, [id, sub{std::move(sub)}, i](class store& s) {
          return s.has_version(sub, id, i);
      });
    co_return has_id.has_value() && has_id.assume_value();
}

ss::future<std::optional<schema_id>>
sharded_store::get_schema_id(context ctx, schema_definition def) const {
    auto map = [&ctx, &def](const store& s) {
        return s.get_schema_id(ctx, def);
    };
    auto reduce = [](auto acc, auto s_id) { return std::max(acc, s_id); };
    co_return co_await _store.map_reduce0(
      map, std::optional<schema_id>{}, reduce);
}

ss::future<chunked_vector<context>>
sharded_store::get_materialized_contexts() const {
    using contexts = chunked_vector<context>;
    auto map = [](const store& s) { return s.get_materialized_contexts(); };
    auto reduce = [](contexts acc, contexts ctxs) {
        acc.reserve(acc.size() + ctxs.size());
        std::ranges::move(ctxs, std::back_inserter(acc));
        return acc;
    };
    auto ctxs = co_await _store.map_reduce0(map, contexts{}, reduce);
    std::ranges::sort(ctxs);
    auto uniq = std::ranges::unique(ctxs);
    ctxs.erase_to_end(uniq.begin());
    co_return ctxs;
}

ss::future<bool> sharded_store::is_context_materialized(context ctx) const {
    co_return _store.local().is_context_materialized(ctx);
}

ss::future<>
sharded_store::set_context_materialized(context ctx, bool materialized) {
    co_await _store.invoke_on_all(
      _smp_opts, [ctx{std::move(ctx)}, materialized](store& s) {
          s.set_context_materialized(ctx, materialized);
      });
}

} // namespace pandaproxy::schema_registry
