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

#include "pandaproxy/schema_registry/sharded_store.h"

#include "hashing/jump_consistent_hash.h"
#include "hashing/xx.h"
#include "kafka/protocol/errors.h"
#include "pandaproxy/logger.h"
#include "pandaproxy/schema_registry/avro.h"
#include "pandaproxy/schema_registry/errors.h"
#include "pandaproxy/schema_registry/exceptions.h"
#include "pandaproxy/schema_registry/protobuf.h"
#include "pandaproxy/schema_registry/store.h"
#include "pandaproxy/schema_registry/types.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/std-coroutine.hh>

namespace pandaproxy::schema_registry {

namespace {

ss::shard_id shard_for(const subject& sub) {
    auto hash = xxhash_64(sub().data(), sub().length());
    return jump_consistent_hash(hash, ss::smp::count);
}

ss::shard_id shard_for(schema_id id) {
    return jump_consistent_hash(id(), ss::smp::count);
}

bool check_compatible(const valid_schema& reader, const valid_schema& writer) {
    return reader.visit([&](const auto& reader) {
        return writer.visit([&](const auto& writer) {
            if constexpr (std::is_same_v<decltype(reader), decltype(writer)>) {
                return check_compatible(reader, writer);
            }
            return false;
        });
    });
}

} // namespace

ss::future<> sharded_store::start(ss::smp_service_group sg) {
    _smp_opts = ss::smp_submit_to_options{sg};
    return _store.start();
}

ss::future<> sharded_store::stop() { return _store.stop(); }

ss::future<canonical_schema>
sharded_store::make_canonical_schema(unparsed_schema schema) {
    switch (schema.type()) {
    case schema_type::avro: {
        co_return canonical_schema{
          std::move(schema.sub()),
          sanitize_avro_schema_definition(schema.def()).value(),
          std::move(schema.refs())};
    }
    case schema_type::protobuf:
        co_return co_await make_canonical_protobuf_schema(
          *this, std::move(schema));
    case schema_type::json:
        throw as_exception(invalid_schema_type(schema.type()));
    }
    __builtin_unreachable();
}

ss::future<> sharded_store::validate_schema(const canonical_schema& schema) {
    switch (schema.type()) {
    case schema_type::avro: {
        make_avro_schema_definition(schema.def().raw()()).value();
        co_return;
    }
    case schema_type::protobuf:
        co_await validate_protobuf_schema(*this, schema);
        co_return;
    case schema_type::json:
        throw as_exception(invalid_schema_type(schema.type()));
    }
    __builtin_unreachable();
}

ss::future<valid_schema>
sharded_store::make_valid_schema(const canonical_schema& schema) {
    switch (schema.type()) {
    case schema_type::avro:
        co_return make_avro_schema_definition(schema.def().raw()()).value();
    case schema_type::protobuf:
        co_return co_await make_protobuf_schema_definition(*this, schema);
    case schema_type::json:
        throw as_exception(invalid_schema_type(schema.type()));
    }
    __builtin_unreachable();
}

ss::future<sharded_store::insert_result>
sharded_store::project_ids(const canonical_schema& schema) {
    // Validate the schema (may throw)
    co_await validate_schema(schema);

    // Check compatibility
    std::vector<schema_version> versions;
    try {
        versions = co_await get_versions(schema.sub(), include_deleted::no);
    } catch (const exception& e) {
        if (e.code() != error_code::subject_not_found) {
            throw;
        }
    }
    if (!versions.empty()) {
        auto compat = co_await is_compatible(versions.back(), schema);
        if (!compat) {
            throw exception(
              error_code::schema_incompatible,
              fmt::format(
                "Schema being registered is incompatible with an earlier "
                "schema for subject \"{}\"",
                schema.sub()));
        }
    }

    // Figure out if the definition already exists
    auto map = [&schema](store& s) { return s.get_schema_id(schema.def()); };
    auto reduce = [](
                    std::optional<schema_id> acc,
                    std::optional<schema_id> s_id) { return acc ? acc : s_id; };
    auto s_id = co_await _store.map_reduce0(
      map, std::optional<schema_id>{}, reduce);

    if (!s_id) {
        // New schema, project an ID for it.
        s_id = co_await project_schema_id();
        vlog(plog.debug, "project_ids: projected new ID {}", s_id.value());
    } else {
        vlog(plog.debug, "project_ids: existing ID {}", s_id.value());
    }

    auto v_id = co_await _store.invoke_on(
      shard_for(schema.sub()),
      _smp_opts,
      &store::project_version,
      schema.sub(),
      s_id.value());

    co_return insert_result{
      v_id.value_or(invalid_schema_version), s_id.value(), v_id.has_value()};
}

ss::future<bool> sharded_store::upsert(
  seq_marker marker,
  canonical_schema schema,
  schema_id id,
  schema_version version,
  is_deleted deleted) {
    co_await upsert_schema(id, std::move(schema).def());
    co_return co_await upsert_subject(
      marker,
      std::move(schema).sub(),
      std::move(schema).refs(),
      version,
      id,
      deleted);
}

ss::future<subject_schema>
sharded_store::has_schema(const canonical_schema& schema) {
    auto versions = co_await get_versions(schema.sub(), include_deleted::no);

    try {
        co_await validate_schema(schema);
    } catch (const exception& e) {
        throw as_exception(invalid_subject_schema(schema.sub()));
    }

    std::optional<subject_schema> sub_schema;
    for (auto ver : versions) {
        try {
            auto res = co_await get_subject_schema(
              schema.sub(), ver, include_deleted::no);
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
sharded_store::get_schema_definition(const schema_id& id) {
    auto schema = co_await _store.invoke_on(
      shard_for(id), _smp_opts, &store::get_schema_definition, id);
    co_return std::move(schema).value();
}

ss::future<std::vector<subject_version>>
sharded_store::get_schema_subject_versions(schema_id id) {
    auto map = [id](store& s) { return s.get_schema_subject_versions(id); };
    auto reduce =
      [](std::vector<subject_version> acc, std::vector<subject_version> svs) {
          acc.insert(acc.end(), svs.begin(), svs.end());
          return acc;
      };
    co_return co_await _store.map_reduce0(
      map, std::vector<subject_version>{}, reduce);
}

ss::future<subject_schema> sharded_store::get_subject_schema(
  const subject& sub,
  std::optional<schema_version> version,
  include_deleted inc_del) {
    auto v_id = (co_await _store.invoke_on(
                   shard_for(sub),
                   _smp_opts,
                   &store::get_subject_version_id,
                   sub,
                   version,
                   inc_del))
                  .value();

    auto def = (co_await _store.invoke_on(
                  shard_for(v_id.id),
                  _smp_opts,
                  &store::get_schema_definition,
                  v_id.id))
                 .value();

    co_return subject_schema{
      .schema = {sub, std::move(def), std::move(v_id.refs)},
      .version = v_id.version,
      .id = v_id.id,
      .deleted = v_id.deleted};
}

ss::future<std::vector<subject>>
sharded_store::get_subjects(include_deleted inc_del) {
    auto map = [inc_del](store& s) { return s.get_subjects(inc_del); };
    auto reduce = [](std::vector<subject> acc, std::vector<subject> subs) {
        acc.insert(
          acc.end(),
          std::make_move_iterator(subs.begin()),
          std::make_move_iterator(subs.end()));
        return acc;
    };
    co_return co_await _store.map_reduce0(map, std::vector<subject>{}, reduce);
}

ss::future<std::vector<schema_version>>
sharded_store::get_versions(const subject& sub, include_deleted inc_del) {
    auto versions = co_await _store.invoke_on(
      shard_for(sub), _smp_opts, &store::get_versions, sub, inc_del);
    co_return std::move(versions).value();
}

ss::future<std::vector<schema_version>> sharded_store::delete_subject(
  seq_marker marker, const subject& sub, permanent_delete permanent) {
    auto versions = co_await _store.invoke_on(
      shard_for(sub),
      _smp_opts,
      &store::delete_subject,
      marker,
      sub,
      permanent);
    co_return std::move(versions).value();
}

ss::future<is_deleted> sharded_store::is_subject_deleted(const subject& sub) {
    auto deleted = co_await _store.invoke_on(
      shard_for(sub), _smp_opts, &store::is_subject_deleted, sub);

    co_return std::move(deleted).value();
}

ss::future<is_deleted> sharded_store::is_subject_version_deleted(
  const subject& sub, const schema_version version) {
    auto deleted = co_await _store.invoke_on(
      shard_for(sub),
      _smp_opts,
      &store::is_subject_version_deleted,
      sub,
      version);

    co_return std::move(deleted).value();
}

ss::future<std::vector<seq_marker>>
sharded_store::get_subject_written_at(const subject& sub) {
    auto history = co_await _store.invoke_on(
      shard_for(sub), _smp_opts, &store::get_subject_written_at, sub);

    co_return std::move(history).value();
}

ss::future<std::vector<seq_marker>>
sharded_store::get_subject_version_written_at(
  const subject& sub, schema_version version) {
    auto history = co_await _store.invoke_on(
      shard_for(sub),
      _smp_opts,
      &store::get_subject_version_written_at,
      sub,
      version);

    co_return std::move(history).value();
}

ss::future<bool> sharded_store::delete_subject_version(
  const subject& sub, schema_version version) {
    auto deleted = co_await _store.invoke_on(
      shard_for(sub), _smp_opts, &store::delete_subject_version, sub, version);
    co_return deleted.value();
}

ss::future<compatibility_level> sharded_store::get_compatibility() {
    co_return _store.local().get_compatibility().value();
}

ss::future<compatibility_level> sharded_store::get_compatibility(
  const subject& sub, default_to_global fallback) {
    auto get = [sub, fallback](store& s) {
        return s.get_compatibility(sub, fallback);
    };
    auto level = co_await _store.invoke_on(shard_for(sub), get);
    co_return level.value();
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
  seq_marker marker, const subject& sub, compatibility_level compatibility) {
    using overload_t = result<bool> (store::*)(
      seq_marker, const subject&, compatibility_level);
    auto set = co_await _store.invoke_on(
      shard_for(sub),
      _smp_opts,
      static_cast<overload_t>(&store::set_compatibility),
      marker,
      sub,
      compatibility);
    co_return set.value();
}

ss::future<bool> sharded_store::clear_compatibility(const subject& sub) {
    auto cleared = co_await _store.invoke_on(
      shard_for(sub), _smp_opts, &store::clear_compatibility, sub);
    co_return cleared.value();
}

ss::future<bool>
sharded_store::upsert_schema(schema_id id, canonical_schema_definition def) {
    co_await maybe_update_max_schema_id(id);
    co_return co_await _store.invoke_on(
      shard_for(id), _smp_opts, &store::upsert_schema, id, std::move(def));
}

ss::future<sharded_store::insert_subject_result> sharded_store::insert_subject(
  subject sub, canonical_schema::references refs, schema_id id) {
    auto [version, inserted] = co_await _store.invoke_on(
      shard_for(sub),
      _smp_opts,
      &store::insert_subject,
      sub,
      std::move(refs),
      id);
    co_return insert_subject_result{version, inserted};
}

ss::future<bool> sharded_store::upsert_subject(
  seq_marker marker,
  subject sub,
  canonical_schema::references refs,
  schema_version version,
  schema_id id,
  is_deleted deleted) {
    co_return co_await _store.invoke_on(
      shard_for(sub),
      _smp_opts,
      &store::upsert_subject,
      marker,
      sub,
      std::move(refs),
      version,
      id,
      deleted);
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
  schema_version version, const canonical_schema& new_schema) {
    // Lookup the version_ids
    const auto& sub = new_schema.sub();
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

    // Types must always match
    if (old_schema.schema.type() != new_schema.type()) {
        co_return false;
    }

    // Lookup the compatibility level
    auto compat = co_await get_compatibility(sub, default_to_global::yes);

    if (compat == compatibility_level::none) {
        co_return true;
    }

    // Currently only support AVRO
    if (new_schema.type() != schema_type::avro) {
        throw as_exception(invalid_schema_type(new_schema.type()));
    }

    // if transitive, search all, otherwise seach forwards from version
    if (
      compat == compatibility_level::backward_transitive
      || compat == compatibility_level::forward_transitive
      || compat == compatibility_level::full_transitive) {
        ver_it = versions.begin();
    }

    auto new_valid = co_await make_valid_schema(new_schema);

    auto is_compat = true;
    for (; is_compat && ver_it != versions.end(); ++ver_it) {
        if (ver_it->deleted) {
            continue;
        }

        auto old_schema = co_await get_subject_schema(
          sub, ver_it->version, include_deleted::no);
        auto old_valid = co_await make_valid_schema(old_schema.schema);

        if (
          compat == compatibility_level::backward
          || compat == compatibility_level::backward_transitive
          || compat == compatibility_level::full) {
            is_compat = is_compat && check_compatible(new_valid, old_valid);
        }
        if (
          compat == compatibility_level::forward
          || compat == compatibility_level::forward_transitive
          || compat == compatibility_level::full) {
            is_compat = is_compat && check_compatible(old_valid, new_valid);
        }
    }
    co_return is_compat;
}

} // namespace pandaproxy::schema_registry
