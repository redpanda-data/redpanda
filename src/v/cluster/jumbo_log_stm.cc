// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/jumbo_log_stm.h"

#include "base/outcome.h"
#include "base/vassert.h"
#include "base/vlog.h"
#include "bytes/iobuf.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "errc.h"
#include "jumbo_log/metadata.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/record_batch_types.h"
#include "model/timeout_clock.h"
#include "persisted_stm.h"
#include "raft/consensus.h"
#include "replicate.h"
#include "serde/envelope.h"
#include "serde/rw/rw.h"
#include "storage/record_batch_builder.h"
#include "utils/named_type.h"

#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/util/bool_class.hh>
#include <seastar/util/later.hh>
#include <seastar/util/log.hh>

#include <fmt/core.h>

#include <stdexcept>
#include <stdint.h>
#include <tuple>
#include <vector>

namespace cluster {

struct jumbo_log_stm::write_intent
  : public serde::
      envelope<write_intent, serde::version<0>, serde::compat_version<0>> {
    jumbo_log::write_intent_segment segment;
};

using cmd_key = named_type<uint8_t, struct cmd_key_tag>;
struct jumbo_log_stm::create_write_intent_cmd {
    static constexpr cmd_key key{0};
    using value = jumbo_log_stm::write_intent;
};

jumbo_log_stm::jumbo_log_stm(ss::logger& logger, raft::consensus* c)
  : jumbo_log_stm(logger, c, config::shard_local_cfg()) {}

jumbo_log_stm::jumbo_log_stm(
  ss::logger& logger, raft::consensus* c, config::configuration& cfg)
  : persisted_stm(jumbo_log_snapshot, logger, c)
  , _next_write_intent_id(1) {
    std::ignore = cfg;
}

ss::future<result<jumbo_log::write_intent_id_t>>
jumbo_log_stm::create_write_intent(
  jumbo_log::segment_object object, model::timeout_clock::duration timeout) {
    // TODO(nv): this lock needs a timeout
    auto u = co_await _lock.get_units();

    if (!co_await sync(timeout)) {
        co_return raft::make_error_code(raft::errc::timeout);
    }

    // Arbitrary limit to prevent unbounded backlog and memory growth.
    // This also puts a limit on how many objects we'll have to scan in parallel
    // during reconciliation/committing.
    const auto MAX_WRITE_INTENTS = 1000;

    if (_write_intents.size() > MAX_WRITE_INTENTS) {
        co_return raft::make_error_code(raft::errc::timeout);
    }

    // If this is a retry, just return the existing write intent id.
    auto write_intent_id_it = _write_intent_id_by_object_id.find(object.id);
    if (write_intent_id_it != _write_intent_id_by_object_id.end()) {
        // Ensure that size_bytes also matches and the client is not reusing
        // object ids.
        if (auto write_intent_it = _write_intents.find(
              write_intent_id_it->second);
            write_intent_it != _write_intents.end()
            && write_intent_it->second.object != object) {
            co_return raft::make_error_code(raft::errc::invalid_input_records);
        }
        co_return write_intent_id_it->second;
    }

    storage::record_batch_builder batch_builder(
      model::record_batch_type::jumbo_log_cmd, model::offset{});

    auto intent_id = _next_write_intent_id;

    auto record = create_write_intent_cmd::value{
      .segment = jumbo_log::write_intent_segment(intent_id, object)};

    batch_builder.add_raw_kv(
      serde::to_iobuf(create_write_intent_cmd::key), serde::to_iobuf(record));

    // TODO(nv): Do we want to validate that the object exists in s3?
    // If we don't a bug in the uploader returns success but the object is not
    // in s3, we will have a dangling write intent which we won't be able to
    // clean up. An alternative failure scenario is the uploader uploading the
    // object at the wrong path. How risky is this? A head request to s3 would
    // be a good way to validate the object exists.

    // No timeouts here! We can't allow concurrency as it will break write
    // intent id allocation invariants.
    auto r = co_await _raft->replicate(
      _insync_term,
      model::make_memory_record_batch_reader(std::move(batch_builder).build()),
      raft::replicate_options(raft::consistency_level::quorum_ack));
    if (!r) {
        co_return raft::make_error_code(raft::errc::timeout);
    }

    // TODO(nv): Sure ok to timeout here?
    if (!co_await wait_no_throw(
          model::offset(r.value().last_offset()),
          model::timeout_clock::now() + timeout)) {
        co_return raft::make_error_code(raft::errc::timeout);
    }

    co_return intent_id;
}

ss::future<result<std::vector<jumbo_log::write_intent_segment>>>
jumbo_log_stm::get_write_intents(
  std::vector<jumbo_log::write_intent_id_t> ids,
  model::timeout_clock::duration timeout) {
    {
        auto u = co_await _lock.get_units();

        // We can propagate terms and offsets through the system to this sync in
        // the hot path. Most of the callers don't need "latest" state. They
        // need reasonable up to date state. They can tell us what "reasonable"
        // is.
        if (!co_await sync(timeout)) {
            co_return raft::make_error_code(raft::errc::timeout);
        }
    }

    // We release the lock since the rest of this method is synchronous so the
    // in-memory state is consistent.

    std::vector<jumbo_log::write_intent_segment> result;
    result.reserve(noexcept(ids.size()));

    for (auto id : ids) {
        auto it = _write_intents.find(id);
        if (it == _write_intents.end()) {
            // This shouldn't happen often. The write intents will be deleted
            // only after the "client" NTP was informed about the new fence. So
            // this will happen only if the client had a request in-flight.
            // Unlikely.
            co_return raft::make_error_code(raft::errc::invalid_input_records);
        }

        result.push_back(it->second);
    }

    co_return result;
}

ss::future<bool> jumbo_log_stm::sync(model::timeout_clock::duration timeout) {
    auto is_synced = co_await persisted_stm::sync(timeout);

    co_return is_synced;
}

ss::future<> jumbo_log_stm::apply(const model::record_batch& batch) {
    if (batch.header().type != model::record_batch_type::jumbo_log_cmd) {
        co_return;
    }

    batch.for_each_record([this](model::record&& r) {
        auto key = serde::from_iobuf<cmd_key>(r.release_key());

        switch (key) {
        case create_write_intent_cmd::key: {
            auto intent = serde::from_iobuf<create_write_intent_cmd::value>(
              r.release_value());

            apply_create_write_intent(std::move(intent));
            break;
        }
        default:
            throw std::runtime_error(fmt_with_ctx(
              fmt::format,
              "Unknown jumbo_log_stm command {}",
              static_cast<int>(key)));
        }

        return ss::stop_iteration::no;
    });

    co_return;
}

ss::future<> jumbo_log_stm::write_snapshot() {
    if (_is_writing_snapshot) {
        return ss::now();
    }
    _is_writing_snapshot = true;
    // return _raft
    //   ->write_snapshot(raft::write_snapshot_cfg(0, iobuf()))
    //   .finally([this] { _is_writing_snapshot = false; });
    return ss::now();
}

ss::future<>
jumbo_log_stm::apply_local_snapshot(raft::stm_snapshot_header, iobuf&&) {
    return ss::make_exception_future<>(
      std::logic_error("jumbo_log_stm doesn't support snapshots"));
}

ss::future<raft::stm_snapshot> jumbo_log_stm::take_local_snapshot() {
    return ss::make_exception_future<raft::stm_snapshot>(
      std::logic_error("jumbo_log_stm doesn't support snapshots"));
}

ss::future<> jumbo_log_stm::apply_raft_snapshot(const iobuf&) {
    return ss::now();
}

void jumbo_log_stm::apply_create_write_intent(write_intent intent) {
    auto intent_id = intent.segment.id;
    auto object_id = intent.segment.object.id;

    // Using vassert to avoid having to deal with exception safety which can
    // violate consistency invariants between data structures.

    vassert(
      intent_id == _next_write_intent_id,
      "Invariant violation: Write intent ID {} does not match expected ID {}",
      intent_id,
      _next_write_intent_id);

    auto intent_res = _write_intents.emplace(
      intent_id, std::move(intent.segment));
    vassert(
      intent_res.second,
      "Invariant violation: Write intent {} already exists",
      intent_id);

    auto object_id_index_res = _write_intent_id_by_object_id.emplace(
      object_id, intent_id);

    vassert(
      object_id_index_res.second,
      "Invariant violation: Write intent object {} already exists",
      object_id);

    _next_write_intent_id++;
}

} // namespace cluster
