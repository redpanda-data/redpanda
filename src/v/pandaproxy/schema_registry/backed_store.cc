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

#include "pandaproxy/schema_registry/backed_store.h"

#include "pandaproxy/logger.h"
#include "pandaproxy/schema_registry/storage.h"
#include "utils/retry.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/std-coroutine.hh>

#include <system_error>

namespace pandaproxy::schema_registry {

ss::future<> backed_store::start(ss::smp_service_group sg) {
    return _store.start(sg);
}

ss::future<> backed_store::stop() { return _store.stop(); }

ss::future<backed_store::insert_result>
backed_store::insert(subject sub, schema_definition def, schema_type type) {
    return retry_with_backoff(
      5, [this, sub, def, type]() { return do_insert(sub, def, type); });
}

ss::future<backed_store::insert_result>
backed_store::do_insert(subject sub, schema_definition def, schema_type type) {
    auto res = co_await _store.would_insert(sub, def, type);
    if (res.inserted) {
        auto batch = make_schema_batch(
          sub, res.version, res.id, def, type, is_deleted::no);

        auto res = co_await _client.local().produce_record_batch(
          model::schema_registry_internal_tp, std::move(batch));

        if (res.error_code != kafka::error_code::none) {
            throw kafka::exception(res.error_code, *res.error_message);
        }

        auto conflicted = co_await wait(res.base_offset);
        if (conflicted) {
            throw exception(error_code::write_conflict);
        }
    }
    co_return res;
}

ss::future<bool> backed_store::upsert(
  subject sub,
  schema_definition def,
  schema_type type,
  schema_id id,
  schema_version version,
  is_deleted deleted) {
    return _store.upsert(
      std::move(sub), std::move(def), type, id, version, deleted);
}

ss::future<schema> backed_store::get_schema(const schema_id& id) {
    return _store.get_schema(id);
}

ss::future<subject_schema> backed_store::get_subject_schema(
  const subject& sub, schema_version version, include_deleted inc_del) {
    return _store.get_subject_schema(sub, version, inc_del);
}

ss::future<std::vector<subject>>
backed_store::get_subjects(include_deleted inc_del) {
    return _store.get_subjects(inc_del);
}

ss::future<std::vector<schema_version>>
backed_store::get_versions(const subject& sub, include_deleted inc_del) {
    return _store.get_versions(sub, inc_del);
}

ss::future<std::vector<schema_version>>
backed_store::delete_subject(const subject& sub, permanent_delete permanent) {
    return _store.delete_subject(sub, permanent);
}

ss::future<bool> backed_store::delete_subject_version(
  const subject& sub,
  schema_version version,
  permanent_delete permanent,
  include_deleted inc_del) {
    return _store.delete_subject_version(sub, version, permanent, inc_del);
}

ss::future<compatibility_level> backed_store::get_compatibility() {
    return _store.get_compatibility();
}

ss::future<compatibility_level>
backed_store::get_compatibility(const subject& sub) {
    return _store.get_compatibility(sub);
}

ss::future<bool>
backed_store::set_compatibility(compatibility_level compatibility) {
    return _store.set_compatibility(compatibility);
}

ss::future<bool> backed_store::set_compatibility(
  const subject& sub, compatibility_level compatibility) {
    return _store.set_compatibility(sub, compatibility);
}

ss::future<bool> backed_store::clear_compatibility(const subject& sub) {
    return _store.clear_compatibility(sub);
}

ss::future<bool> backed_store::is_compatible(
  const subject& sub,
  schema_version version,
  const schema_definition& new_schema,
  schema_type new_schema_type) {
    return _store.is_compatible(sub, version, new_schema, new_schema_type);
}

ss::future<>
backed_store::signal(model::offset offset, offset_conflict conflict) {
    return ss::smp::submit_to(ss::shard_id{0}, [this, offset, conflict]() {
        _offsets.signal(offset, conflict);
    });
}

ss::future<offset_conflict> backed_store::wait(model::offset offset) {
    return ss::smp::submit_to(
      ss::shard_id{0}, [this, offset]() { return _offsets.wait(offset); });
}

} // namespace pandaproxy::schema_registry
