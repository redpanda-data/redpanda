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
#include "pandaproxy/logger.h"
#include "pandaproxy/schema_registry/avro.h"
#include "pandaproxy/schema_registry/errors.h"
#include "pandaproxy/schema_registry/store.h"
#include "pandaproxy/schema_registry/types.h"

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

} // namespace

ss::future<> sharded_store::start(ss::smp_service_group sg) {
    _smp_opts = ss::smp_submit_to_options{sg};
    return _store.start();
}

ss::future<> sharded_store::stop() { return _store.stop(); }

ss::future<sharded_store::insert_result>
sharded_store::insert(subject sub, schema_definition def, schema_type type) {
    auto id = (co_await insert_schema(std::move(def), type)).id;
    auto [version, inserted] = co_await _store.invoke_on(
      shard_for(sub), &store::insert_subject, sub, id);
    co_return insert_result{version, id, inserted};
}

ss::future<bool> sharded_store::upsert(
  subject sub,
  schema_definition def,
  schema_type type,
  schema_id id,
  schema_version version,
  is_deleted deleted) {
    co_await upsert_schema(id, std::move(def), type);
    co_return co_await upsert_subject(sub, version, id, deleted);
}

ss::future<schema> sharded_store::get_schema(const schema_id& id) {
    auto schema = co_await _store.invoke_on(
      shard_for(id), &store::get_schema, id);
    co_return std::move(schema).value();
}

ss::future<subject_schema> sharded_store::get_subject_schema(
  const subject& sub, schema_version version, include_deleted inc_del) {
    auto v_id = (co_await _store.invoke_on(
                   shard_for(sub),
                   &store::get_subject_version_id,
                   sub,
                   version,
                   inc_del))
                  .value();
    auto s = co_await get_schema(v_id.id);

    co_return subject_schema{
      .sub = sub,
      .version = v_id.version,
      .id = v_id.id,
      .type = s.type,
      .definition = std::move(s.definition),
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
      shard_for(sub), &store::get_versions, sub, inc_del);
    co_return std::move(versions).value();
}

ss::future<std::vector<schema_version>>
sharded_store::delete_subject(const subject& sub, permanent_delete permanent) {
    auto versions = co_await _store.invoke_on(
      shard_for(sub), &store::delete_subject, sub, permanent);
    co_return std::move(versions).value();
}

ss::future<bool> sharded_store::delete_subject_version(
  const subject& sub,
  schema_version version,
  permanent_delete permanent,
  include_deleted inc_del) {
    auto deleted = co_await _store.invoke_on(
      shard_for(sub),
      &store::delete_subject_version,
      sub,
      version,
      permanent,
      inc_del);
    co_return deleted.value();
}

ss::future<compatibility_level> sharded_store::get_compatibility() {
    co_return _store.local().get_compatibility().value();
}

ss::future<compatibility_level>
sharded_store::get_compatibility(const subject& sub) {
    using overload_t = result<compatibility_level> (store::*)(const subject&)
      const;
    auto level = co_await _store.invoke_on(
      shard_for(sub), static_cast<overload_t>(&store::get_compatibility), sub);
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
  const subject& sub, compatibility_level compatibility) {
    using overload_t = result<bool> (store::*)(
      const subject&, compatibility_level);
    auto set = co_await _store.invoke_on(
      shard_for(sub),
      static_cast<overload_t>(&store::set_compatibility),
      sub,
      compatibility);
    co_return set.value();
}

ss::future<bool> sharded_store::clear_compatibility(const subject& sub) {
    auto cleared = co_await _store.invoke_on(
      shard_for(sub), &store::clear_compatibility, sub);
    co_return cleared.value();
}

ss::future<sharded_store::insert_schema_result>
sharded_store::insert_schema(schema_definition def, schema_type type) {
    auto map = [&def, type](store& s) { return s.get_schema_id(def, type); };
    auto reduce = [](
                    std::optional<schema_id> acc,
                    std::optional<schema_id> s_id) { return acc ? acc : s_id; };
    auto s_id = co_await _store.map_reduce0(
      map, std::optional<schema_id>{}, reduce);
    if (s_id) {
        co_return insert_schema_result{*s_id, false};
    }

    auto new_s_id = co_await allocate_schema_id();
    auto inserted = co_await upsert_schema(new_s_id, std::move(def), type);
    co_return insert_schema_result{new_s_id, inserted};
}

ss::future<bool> sharded_store::upsert_schema(
  schema_id id, schema_definition def, schema_type type) {
    co_await maybe_update_max_schema_id(id);
    co_return co_await _store.invoke_on(
      shard_for(id), &store::upsert_schema, id, std::move(def), type);
}

ss::future<sharded_store::insert_subject_result>
sharded_store::insert_subject(subject sub, schema_id id) {
    auto [version, inserted] = co_await _store.invoke_on(
      shard_for(sub), &store::insert_subject, sub, id);
    co_return insert_subject_result{version, inserted};
}

ss::future<bool> sharded_store::upsert_subject(
  subject sub, schema_version version, schema_id id, is_deleted deleted) {
    co_return co_await _store.invoke_on(
      shard_for(sub), &store::upsert_subject, sub, version, id, deleted);
}

ss::future<schema_id> sharded_store::allocate_schema_id() {
    auto increment = [this] { return _next_schema_id++; };
    co_return co_await ss::smp::submit_to(
      ss::shard_id{0}, _smp_opts, std::move(increment));
}

ss::future<> sharded_store::maybe_update_max_schema_id(schema_id id) {
    auto update = [this, id] {
        _next_schema_id = std::max(_next_schema_id, id + 1);
    };
    co_return co_await ss::smp::submit_to(
      ss::shard_id{0}, _smp_opts, std::move(update));
}

ss::future<bool> sharded_store::is_compatible(
  const subject& sub,
  schema_version version,
  const schema_definition& new_schema,
  schema_type new_schema_type) {
    // Lookup the version_ids
    const auto versions = co_await _store.invoke_on(
      shard_for(sub), _smp_opts, [&sub](auto& s) {
          return s.get_version_ids(sub, include_deleted::no).value();
      });

    auto ver_it = std::lower_bound(
      versions.begin(),
      versions.end(),
      version,
      [](const subject_version_id& lhs, schema_version rhs) {
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
    if (old_schema.type != new_schema_type) {
        co_return false;
    }

    // Lookup the compatibility level
    auto compat = co_await get_compatibility(sub);

    if (compat == compatibility_level::none) {
        co_return true;
    }

    // Currently only support AVRO
    if (new_schema_type != schema_type::avro) {
        throw as_exception(error_info{
          error_code::schema_invalid,
          fmt::format(
            "Invalid schema type {}", to_string_view(new_schema_type))});
    }

    // if transitive, search all, otherwise seach forwards from version
    if (
      compat == compatibility_level::backward_transitive
      || compat == compatibility_level::forward_transitive
      || compat == compatibility_level::full_transitive) {
        ver_it = versions.begin();
    }

    auto new_avro = make_avro_schema_definition(new_schema()).value();
    auto is_compat = true;
    for (; is_compat && ver_it != versions.end(); ++ver_it) {
        if (ver_it->deleted) {
            continue;
        }

        auto old_schema = co_await get_schema(ver_it->id);
        auto old_avro
          = make_avro_schema_definition(old_schema.definition()).value();

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
    co_return is_compat;
}

} // namespace pandaproxy::schema_registry
