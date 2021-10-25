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

#include "cluster/metadata_cache.h"
#include "cluster/non_replicable_topics_frontend.h"
#include "coproc/exception.h"
#include "coproc/logger.h"
#include "coproc/reference_window_consumer.hpp"
#include "storage/parser_utils.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>

namespace coproc {
class log_not_yet_created_exception final : public exception {
    using exception::exception;
};

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

static ss::future<> do_write_materialized_partition(
  storage::log log, model::record_batch_reader reader) {
    /// Re-write all batch term_ids to 1, otherwise they will carry the
    /// term ids of records coming from parent batches
    auto [success, batch_w_correct_terms]
      = co_await std::move(reader).for_each_ref(
        reference_window_consumer(
          model::record_batch_crc_checker(), set_term_id_to_zero()),
        model::no_timeout);
    vassert(
      success,
      "Wasm engine impl error, failed crc checks, check wasm engine impl: {}",
      log.config().ntp());

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
}

static ss::future<storage::log>
get_log(storage::log_manager& log_mgr, model::ntp ntp) {
    /// It is likely that when a topic is created, it exists in topic metadata
    /// for a moment before its storage::log is instantiated. This is due to the
    /// controller_backend performing this work out of band. This loop attempts
    /// to minimize the number of retries that would be performed due to this
    /// case.
    int8_t attempts = 5;
    while (attempts-- > 0) {
        auto found = log_mgr.get(ntp);
        if (found) {
            /// Log exists, do nothing and return it
            co_return *found;
        }
        co_await ss::sleep(100ms);
    }
    /// Method `logs_for_context` will be catching this exception and if that
    /// occurs will omit this group from the current working manifest, meaning
    /// no writes and no offset increment as well
    throw log_not_yet_created_exception(fmt::format(
      "Materialized topic {} created but underlying log doesn't yet exist",
      ntp));
}

struct ss::future<> write_materialized_partition(
  storage::log log,
  std::vector<model::record_batch_reader> readers,
  absl::node_hash_map<model::ntp, mutex>& locks) {
    const auto& ntp = log.config().ntp();
    auto found = locks.find(ntp);
    if (found == locks.end()) {
        found = locks.emplace(ntp, mutex()).first;
    }
    /// Mutex used to protect concurrent log writes from the same shard by
    /// coprocessors across different fibers
    return found->second.with(
      [log, readers = std::move(readers)]() mutable -> ss::future<> {
          for (auto& reader : readers) {
              co_await do_write_materialized_partition(log, std::move(reader));
          }
      });
}

static ss::lw_shared_ptr<ntp_context>
interpret_reply(const process_batch_reply::data& e, output_write_args args) {
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
        return nullptr;
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
    auto found = args.inputs.find(e.source);
    if (found == args.inputs.end()) {
        vlog(
          coproclog.error,
          "wasm engine, desires to write to a materialized log whose source {} "
          "has been deleted: {}",
          e.source,
          e.id);
        return nullptr;
    }
    return found->second;
}

static void push_offsets_ahead(
  absl::flat_hash_set<ss::lw_shared_ptr<ntp_context>> ctxs, script_id id) {
    for (auto& ctx : ctxs) {
        auto found = ctx->offsets.find(id);
        vassert(
          found != ctx->offsets.end(),
          "Offset not found for script id {} for ntp owning context: {}",
          id,
          ctx->ntp());
        /// Reset the acked offset so that progress can be made
        found->second.last_acked = found->second.last_read;
    }
}

using batch_of_batches = std::vector<model::record_batch_reader>;
struct request_context {
    struct state {
        ss::lw_shared_ptr<ntp_context> ctx;
        batch_of_batches bobs;
    };
    std::vector<cluster::non_replicable_topic> topics;
    /// Key is a materialized ntp
    absl::flat_hash_map<model::ntp, state> rgrp;
};

static request_context
assemble_request_context(output_write_inputs replies, output_write_args args) {
    request_context rctx;
    for (auto& e : replies) {
        auto ctx = interpret_reply(e, args);
        if (!ctx) {
            continue;
        }
        auto new_materialized = model::topic_namespace(
          e.ntp.ns, e.ntp.tp.topic);
        auto& cache = args.rs.metadata_cache.local();
        if (cache.get_topic_cfg(new_materialized)) {
            if (!cache.get_source_topic(new_materialized)) {
                throw script_illegal_action_exception(
                  args.id,
                  fmt::format(
                    "Script {} attempted to write to a normal topic: {}",
                    args.id,
                    new_materialized));
            }
        } else {
            if (ctx->partition->is_leader()) {
                rctx.topics.push_back(cluster::non_replicable_topic{
                  .source = model::topic_namespace(
                    e.source.ns, e.source.tp.topic),
                  .name = new_materialized});
            } else {
                vlog(
                  coproclog.warn,
                  "Attempt to create materialized topic before leader "
                  "instantiated it {}",
                  new_materialized);
                continue;
            }
        }
        /// Add the reply to the manifest aggregate
        auto [itr, success] = rctx.rgrp.try_emplace(e.ntp);
        itr->second.bobs.push_back(std::move(*e.reader));
        itr->second.ctx = ctx;
    }
    return rctx;
}

static ss::future<std::vector<cluster::topic_result>> make_materialized_topics(
  ss::sharded<cluster::non_replicable_topics_frontend>& frontend,
  std::vector<cluster::non_replicable_topic> topics) {
    /// All requests are debounced, therefore if multiple entities attempt
    /// to create a materialized topic (would occur across fibers), all requests
    /// will wait for the first to complete.
    co_return co_await frontend.invoke_on(
      cluster::non_replicable_topics_frontend_shard,
      [topics = std::move(topics)](
        cluster::non_replicable_topics_frontend& mtfe) mutable {
          return mtfe.create_non_replicable_topics(
            std::move(topics), model::no_timeout);
      });
}

static ss::future<std::tuple<
  std::vector<std::tuple<storage::log, batch_of_batches>>,
  absl::flat_hash_set<ss::lw_shared_ptr<ntp_context>>>>
logs_for_context(
  storage::log_manager& log_mgr,
  absl::flat_hash_map<model::ntp, request_context::state> wgrp) {
    std::vector<std::tuple<storage::log, batch_of_batches>> labs;
    absl::flat_hash_set<ss::lw_shared_ptr<ntp_context>> ipts;
    absl::flat_hash_set<ss::lw_shared_ptr<ntp_context>> failures;
    for (auto& [ntp, op] : wgrp) {
        try {
            if (failures.contains(op.ctx)) {
                /// One output failed to obtain its log which means a failure
                /// for all other writes in the 'transaction', which are
                /// outputs that share the same input
                continue;
            }
            auto log = co_await get_log(log_mgr, ntp);
            labs.emplace_back(log, std::move(op.bobs));
            ipts.emplace(op.ctx);
        } catch (const log_not_yet_created_exception& ex) {
            failures.emplace(op.ctx);
            vlog(
              coproclog.warn,
              "Removing {} from result set, reason {}",
              ntp,
              ex);
        }
    }
    co_return std::make_tuple(std::move(labs), std::move(ipts));
}

static void prune_failures(
  request_context& rctx, std::vector<cluster::topic_result> results) {
    for (const auto& r : results) {
        if (
          r.ec != cluster::errc::success
          && r.ec != cluster::errc::topic_already_exists) {
            vlog(coproclog.warn, "Removing {} from result set", r.tp_ns);
            absl::erase_if(rctx.rgrp, [tp = r.tp_ns](const auto& value_type) {
                const auto& ntp = value_type.first;
                return ntp.ns == tp.ns && ntp.tp.topic == tp.tp;
            });
        }
    }
}

ss::future<>
write_materialized(output_write_inputs replies, output_write_args args) {
    if (replies.empty()) {
        vlog(
          coproclog.error, "Wasm engine interpreted the request as erraneous");
        co_return;
    }
    /// Group requests into transactions by source topics, so errors across one
    /// source topics and their respective materialized topics don't affect
    /// others. So when input offsets are incremented, it is guaranteed that
    /// either all child topics for an input topic succeeded in writing to disk,
    /// or all were aborted.
    request_context rctx = assemble_request_context(std::move(replies), args);
    try {
        /// 1. Make all materialized topics
        auto results = co_await make_materialized_topics(
          args.rs.mt_frontend, std::move(rctx.topics));
        /// 2. Remove replies w/ error from manifest
        prune_failures(rctx, std::move(results));
        /// 3. Obtain all storage::logs for all requests
        auto [labs, ipts] = co_await logs_for_context(
          args.rs.storage.local().log_mgr(), std::move(rctx.rgrp));
        /// 4. Perform writes (only failures are fatal, i.e. crc exception)
        co_await ss::parallel_for_each(labs, [args](auto& t) -> ss::future<> {
            co_await write_materialized_partition(
              std::get<0>(t), std::move(std::get<1>(t)), args.locks);
        });
        /// 5. Commit offsets
        push_offsets_ahead(std::move(ipts), args.id);
    } catch (const cluster::non_replicable_topic_creation_exception& ex) {
        vlog(
          coproclog.error,
          "Failed to make request to make materialized topics: {}",
          ex.what());
    } catch (const ss::gate_closed_exception& ex) {
        /// Shouldn't occur as all fibers should shut down before `mt_frontend`
        vlog(coproclog.debug, "gate_closed when writing: {}", ex);
    }
}

} // namespace coproc
