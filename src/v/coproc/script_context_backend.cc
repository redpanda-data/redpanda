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

#include "script_context_backend.h"

#include "coproc/logger.h"
#include "coproc/reference_window_consumer.hpp"
#include "storage/parser_utils.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>

namespace coproc {
/// Sets all of the term_ids in a batch of record batches to be a newly
/// desired term.
class set_term_id_to_zero {
public:
    ss::future<ss::stop_iteration> operator()(model::record_batch& b) {
        b.header().ctx.term = model::term_id(0);
        _batches.push_back(std::move(b));
        co_return ss::stop_iteration::no;
    }

    model::record_batch_reader end_of_stream() {
        return model::make_memory_record_batch_reader(std::move(_batches));
    }

private:
    model::record_batch_reader::data_t _batches;
};

static ss::future<bool> do_write_materialized_partition(
  storage::log log, model::record_batch_reader reader) {
    /// Re-write all batch term_ids to 1, otherwise they will carry the
    /// term ids of records coming from parent batches
    auto [success, batch_w_correct_terms]
      = co_await std::move(reader).for_each_ref(
        reference_window_consumer(
          model::record_batch_crc_checker(), set_term_id_to_zero()),
        model::no_timeout);
    if (!success) {
        /// In the case crc checks failed, do NOT write records to storage
        co_return false;
    }
    /// Compress the data before writing...
    auto compressed = co_await std::move(batch_w_correct_terms)
                        .for_each_ref(
                          storage::internal::compress_batch_consumer(
                            model::compression::zstd, 512),
                          model::no_timeout);
    const storage::log_append_config write_cfg{
      .should_fsync = storage::log_append_config::fsync::no,
      .io_priority = ss::default_priority_class(),
      .timeout = model::no_timeout};
    /// Finally, write the batch
    co_await std::move(compressed)
      .for_each_ref(log.make_appender(write_cfg), model::no_timeout)
      .discard_result();
    co_return true;
}

static ss::future<storage::log>
get_log(storage::log_manager& log_mgr, const model::ntp& ntp) {
    auto found = log_mgr.get(ntp);
    if (found) {
        return ss::make_ready_future<storage::log>(*found);
    }
    vlog(coproclog.info, "Making new log: {}", ntp);
    return log_mgr.manage(storage::ntp_config(ntp, log_mgr.config().base_dir));
}

static ss::future<bool> write_materialized_partition(
  const model::materialized_ntp& m_ntp,
  model::record_batch_reader reader,
  output_write_args args) {
    auto found = args.locks.find(m_ntp.input_ntp());
    if (found == args.locks.end()) {
        found = args.locks.emplace(m_ntp.input_ntp(), mutex()).first;
    }
    return found->second.with(
      [m_ntp, args, reader = std::move(reader)]() mutable {
          return get_log(args.rs.storage.local().log_mgr(), m_ntp.input_ntp())
            .then([reader = std::move(reader)](storage::log log) mutable {
                return do_write_materialized_partition(
                  std::move(log), std::move(reader));
            });
      });
}

static ss::future<>
process_one_reply(process_batch_reply::data e, output_write_args args) {
    /// Ensure this 'script_context' instance is handling the correct reply
    if (e.id != args.id()) {
        /// TODO: Maybe in the future errors of these type should mean redpanda
        /// kill -9's the wasm engine.
        vlog(
          coproclog.error,
          "erranous reply from wasm engine, mismatched id observed, expected: "
          "{} and observed {}",
          args.id,
          e.id);
        co_return;
    }
    if (!e.reader) {
        throw script_failed_exception(
          e.id,
          fmt::format(
            "script id {} will auto deregister due to an internal syntax "
            "error",
            e.id));
    }
    /// Use the source topic portion of the materialized topic to perform a
    /// lookup for the relevent 'ntp_context'
    auto materialized_ntp = model::materialized_ntp(e.ntp);
    auto found = args.inputs.find(materialized_ntp.source_ntp());
    if (found == args.inputs.end()) {
        vlog(
          coproclog.warn,
          "script {} unknown source ntp: {}",
          args.id,
          materialized_ntp.source_ntp());
        co_return;
    }
    auto ntp_ctx = found->second;
    auto success = co_await write_materialized_partition(
      materialized_ntp, std::move(*e.reader), args);
    if (!success) {
        vlog(coproclog.warn, "record_batch failed to pass crc checks");
        co_return;
    }
    auto ofound = ntp_ctx->offsets.find(args.id);
    vassert(
      ofound != ntp_ctx->offsets.end(),
      "Offset not found for script id {} for ntp owning context: {}",
      args.id,
      ntp_ctx->ntp());
    /// Reset the acked offset so that progress can be made
    ofound->second.last_acked = ofound->second.last_read;
}

ss::future<>
write_materialized(output_write_inputs replies, output_write_args args) {
    if (replies.empty()) {
        vlog(
          coproclog.error, "Wasm engine interpreted the request as erraneous");
    } else {
        for (auto& e : replies) {
            co_await process_one_reply(std::move(e), args);
        }
    }
}

} // namespace coproc
