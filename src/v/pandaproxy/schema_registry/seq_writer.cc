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

} // namespace pandaproxy::schema_registry
