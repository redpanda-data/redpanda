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
#include "coproc/logger.h"
#include "coproc/materialized_topics_frontend.h"
#include "coproc/reference_window_consumer.hpp"
#include "storage/parser_utils.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>

namespace coproc {
class crc_failed_exception final : public std::exception {
public:
    explicit crc_failed_exception(ss::sstring msg) noexcept
      : _msg(std::move(msg)) {}

    const char* what() const noexcept final { return _msg.c_str(); }

private:
    ss::sstring _msg;
};

class follower_create_topic_exception final : public std::exception {
public:
    explicit follower_create_topic_exception(ss::sstring msg) noexcept
      : _msg(std::move(msg)) {}

    const char* what() const noexcept final { return _msg.c_str(); }

private:
    ss::sstring _msg;
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
    if (!success) {
        /// In the case crc checks failed, do NOT write records to storage
        co_return;
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
}

static ss::future<storage::log>
get_log(storage::log_manager& log_mgr, const model::ntp& ntp) {
    auto found = log_mgr.get(ntp);
    if (found) {
        /// Log exists, do nothing and return it
        return ss::make_ready_future<storage::log>(*found);
    }
    /// In the case the storage::log has not been created, but the topic has,
    /// register to recieve a notification when it is created.
    ss::promise<storage::log> p;
    auto f = p.get_future();
    auto id = log_mgr.register_manage_notification(
      ntp, [p = std::move(p)](storage::log log) mutable { p.set_value(log); });
    return f.then([&log_mgr, id](storage::log log) {
        log_mgr.unregister_manage_notification(id);
        return log;
    });
}

static ss::future<> maybe_make_materialized_log(
  model::topic_namespace source,
  model::topic_namespace new_materialized,
  bool is_leader,
  output_write_args args) {
    /// If the materialized log already exists in cluster metadata, do nothing
    if (args.rs.metadata_cache.local().get_topic_cfg(new_materialized)) {
        return ss::now();
    }
    /// Leader could be on a different machine, can only wait until log comes
    /// into existance
    if (!is_leader) {
        /// TODO: We can do better, when we implement partition_movement we can
        /// obtain events from topic_table, which could notify coproc that an
        /// interested topic/partition has been created by the leader
        return ss::sleep(1s).then([source, new_materialized] {
            return ss::make_exception_future<>(
              follower_create_topic_exception(fmt::format(
                "Follower of source topic {} attempted to created materialzied "
                "topic {} before leader partition had a chance to, sleeping "
                "1s...",
                source,
                new_materialized)));
        });
    }
    /// For sure the source log must exist, query its topic
    /// configuration to create a new materaizlied log with.
    auto item = args.rs.metadata_cache.local().get_topic_cfg(source);
    vassert(
      item, "Source topic for materialized topic doesn't exist: {}", source);
    item->tp_ns.tp = new_materialized.tp;
    item->properties.source_topic = source.tp;
    std::vector<cluster::topic_configuration> cfg;
    cfg.push_back(std::move(*item));
    /// All requests are debounced, therefore if multiple entities attempt to
    /// create a materialzied topic, all requests will wait for the first to
    /// complete.
    return args.rs.mt_frontend.invoke_on(
      0, [topics = std::move(cfg)](materialized_topics_frontend& mtfe) mutable {
          return mtfe.create_materialized_topics(
            std::move(topics), model::no_timeout);
      });
}

static ss::future<> write_materialized_partition(
  const model::ntp& ntp,
  model::record_batch_reader reader,
  ss::lw_shared_ptr<ntp_context> ctx,
  output_write_args args) {
    /// For the rational of why theres mutex uses here read relevent comments in
    /// coproc/ntp_context.h
    auto found = args.locks.find(ntp);
    if (found == args.locks.end()) {
        found = args.locks.emplace(ntp, mutex()).first;
    }
    return found->second.with(
      [args, ntp, ctx, reader = std::move(reader)]() mutable {
          model::topic_namespace source(ctx->ntp().ns, ctx->ntp().tp.topic);
          model::topic_namespace new_materialized(ntp.ns, ntp.tp.topic);
          return maybe_make_materialized_log(
                   source, new_materialized, ctx->partition->is_leader(), args)
            .then([args, ntp] {
                return get_log(args.rs.storage.local().log_mgr(), ntp);
            })
            .then([ntp, reader = std::move(reader)](storage::log log) mutable {
                return do_write_materialized_partition(log, std::move(reader));
            });
      });
}

/// TODO: If we group replies by destination topic, we can increase write
/// throughput, be attentive to maintain relative ordering though..
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
    auto found = args.inputs.find(e.source);
    if (found == args.inputs.end()) {
        vlog(
          coproclog.warn, "script {} unknown source ntp: {}", args.id, e.ntp);
        co_return;
    }
    auto ntp_ctx = found->second;
    try {
        co_await write_materialized_partition(
          e.ntp, std::move(*e.reader), ntp_ctx, args);
    } catch (const crc_failed_exception& ex) {
        vlog(
          coproclog.error,
          "Reprocessing record, failure encountered, {}",
          ex.what());
        co_return;
    } catch (const materialized_topic_replication_exception& ex) {
        vlog(
          coproclog.error,
          "Failed to create materialized topic: {}",
          ex.what());
        co_return;
    } catch (const follower_create_topic_exception& ex) {
        vlog(
          coproclog.debug,
          "Waiting to create materialized topic: {}",
          ex.what());
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
