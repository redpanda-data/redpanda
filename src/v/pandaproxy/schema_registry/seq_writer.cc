//// Copyright 2021 Vectorized, Inc.
////
//// Use of this software is governed by the Business Source License
//// included in the file licenses/BSL.md
////
//// As of the Change Date specified in that file, in accordance with
//// the Business Source License, use of this software will be governed
//// by the Apache License, Version 2.

#include "pandaproxy/schema_registry/seq_writer.h"

#include "pandaproxy/error.h"
#include "pandaproxy/logger.h"
#include "pandaproxy/schema_registry/client_fetch_batch_reader.h"
#include "pandaproxy/schema_registry/storage.h"
#include "random/simple_time_jitter.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>

using namespace std::chrono_literals;

namespace pandaproxy::schema_registry {

/// Call this before reading from the store, if servicing
/// a REST API endpoint that requires global knowledge of latest
/// data (i.e. any listings)
ss::future<> seq_writer::read_sync() {
    auto offsets = co_await _client.local().list_offsets(
      model::schema_registry_internal_tp);

    const auto& topics = offsets.data.topics;
    if (topics.size() != 1 || topics.front().partitions.size() != 1) {
        auto ec = kafka::error_code::unknown_topic_or_partition;
        throw kafka::exception(ec, make_error_code(ec).message());
    }

    const auto& partition = topics.front().partitions.front();
    if (partition.error_code != kafka::error_code::none) {
        auto ec = partition.error_code;
        throw kafka::exception(ec, make_error_code(ec).message());
    }

    co_await wait_for(partition.offset - model::offset{1});
}

ss::future<> seq_writer::wait_for(model::offset offset) {
    return container().invoke_on(0, _smp_opts, [offset](seq_writer& seq) {
        return ss::with_semaphore(seq._wait_for_sem, 1, [&seq, offset]() {
            if (offset > seq._loaded_offset) {
                vlog(
                  plog.debug,
                  "wait_for dirty!  Reading {}..{}",
                  seq._loaded_offset,
                  offset);

                return make_client_fetch_batch_reader(
                         seq._client.local(),
                         model::schema_registry_internal_tp,
                         seq._loaded_offset + model::offset{1},
                         offset + model::offset{1})
                  .consume(
                    consume_to_store{seq._store, seq}, model::no_timeout);
            } else {
                vlog(plog.debug, "wait_for clean (offset  {})", offset);
                return ss::make_ready_future<>();
            }
        });
    });
}

/// Helper for write methods that need to check + retry if their
/// write landed where they expected it to.
///
/// \param write_at Offset at which caller expects their write to land
/// \param batch Message to write
/// \return true if the write landed at `write_at`, else false
ss::future<bool> seq_writer::produce_and_check(
  model::offset write_at, model::record_batch batch) {
    // Because we rely on checking exactly where our message (singular) landed,
    // only use this function with batches of a single message.
    vassert(batch.record_count() == 1, "Only single-message batches allowed");

    kafka::partition_produce_response res
      = co_await _client.local().produce_record_batch(
        model::schema_registry_internal_tp, std::move(batch));

    // TODO(Ben): Check the error reporting here
    if (res.error_code != kafka::error_code::none) {
        throw kafka::exception(res.error_code, *res.error_message);
    }

    auto wrote_at = res.base_offset;
    if (wrote_at == write_at) {
        vlog(plog.debug, "seq_writer: Successful write at {}", wrote_at);

        co_return true;
    } else {
        vlog(
          plog.debug,
          "seq_writer: Failed write at {} (wrote at {})",
          write_at,
          wrote_at);
        co_return false;
    }
};

ss::future<> seq_writer::advance_offset(model::offset offset) {
    auto remote = [offset](seq_writer& s) { s.advance_offset_inner(offset); };

    return container().invoke_on(0, _smp_opts, remote);
}

void seq_writer::advance_offset_inner(model::offset offset) {
    if (_loaded_offset < offset) {
        vlog(
          plog.debug,
          "seq_writer::advance_offset {}->{}",
          _loaded_offset,
          offset);
        _loaded_offset = offset;
    } else {
        vlog(
          plog.debug,
          "seq_writer::advance_offset ignoring {} (have {})",
          offset,
          _loaded_offset);
    }
}

ss::future<schema_id> seq_writer::write_subject_version(
  subject sub, schema_definition def, schema_type type) {
    auto do_write = [sub, def, type](
                      model::offset write_at,
                      seq_writer& seq) -> ss::future<std::optional<schema_id>> {
        // Check if store already contains this data: if
        // so, we do no I/O and return the schema ID.
        auto projected = co_await seq._store.project_ids(sub, def, type);

        if (!projected.inserted) {
            vlog(plog.debug, "write_subject_version: no-op");
            co_return projected.id;
        } else {
            vlog(
              plog.debug,
              "seq_writer::write_subject_version project offset={} subject={} "
              "schema={} "
              "version={}",
              write_at,
              sub,
              projected.id,
              projected.version);

            auto key = schema_key{
              .seq{write_at},
              .node{_node_id},
              .sub{sub},
              .version{projected.version}};
            auto value = schema_value{
              .sub{sub},
              .version{projected.version},
              .type = type,
              .id{projected.id},
              .schema{def},
              .deleted = is_deleted::no};

            auto batch = as_record_batch(key, value);

            auto success = co_await seq.produce_and_check(
              write_at, std::move(batch));
            if (success) {
                auto applier = consume_to_store(seq._store, seq);
                co_await applier.apply(write_at, key, value);
                seq.advance_offset_inner(write_at);
                co_return projected.id;
            } else {
                co_return std::nullopt;
            }
        }
    };

    return sequenced_write(do_write);
}

ss::future<bool> seq_writer::write_config(
  std::optional<subject> sub, compatibility_level compat) {
    auto do_write = [sub, compat](
                      model::offset write_at,
                      seq_writer& seq) -> ss::future<std::optional<bool>> {
        // Check for no-op case
        compatibility_level existing;
        if (sub.has_value()) {
            existing = co_await seq._store.get_compatibility(sub.value());
        } else {
            existing = co_await seq._store.get_compatibility();
        }
        if (existing == compat) {
            co_return false;
        }

        auto key = config_key{.seq{write_at}, .node{seq._node_id}, .sub{sub}};
        auto value = config_value{.compat = compat};
        auto batch = as_record_batch(key, value);

        auto success = co_await seq.produce_and_check(
          write_at, std::move(batch));
        if (success) {
            auto applier = consume_to_store(seq._store, seq);
            co_await applier.apply(write_at, key, value);
            seq.advance_offset_inner(write_at);
            co_return true;
        } else {
            // Pass up a None, our caller's cue to retry
            co_return std::nullopt;
        }
    };

    co_return co_await sequenced_write(do_write);
}

} // namespace pandaproxy::schema_registry
