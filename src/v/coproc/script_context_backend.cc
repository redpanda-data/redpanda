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

#include "script_context_backend.h"

#include "cluster/metadata_cache.h"
#include "cluster/non_replicable_topics_frontend.h"
#include "coproc/exception.h"
#include "coproc/logger.h"
#include "coproc/partition.h"
#include "coproc/partition_manager.h"
#include "coproc/reference_window_consumer.hpp"
#include "storage/api.h"
#include "storage/log.h"
#include "storage/log_manager.h"
#include "storage/parser_utils.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>

namespace coproc {
class follower_create_topic_exception final : public exception {
    using exception::exception;
};

class log_not_yet_created_exception final : public exception {
    using exception::exception;
};

class malformed_batch_exception final : public exception {
    using exception::exception;
};

class bad_reply_exception final : public exception {
    using exception::exception;
};

class retry_trigger_exception final : public exception {
    using exception::exception;
};

class write_error_exception final : public exception {
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

/// Necessary because the javascript project has potential areas where
/// inconsistent record batches may be built and sent back to rp
class verify_record_batch_consistency {
public:
    ss::future<ss::stop_iteration> operator()(const model::record_batch& b) {
        _is_consistent &= (b.header().record_count == b.record_count());
        co_return _is_consistent ? ss::stop_iteration::no
                                 : ss::stop_iteration::yes;
    }

    bool end_of_stream() const { return _is_consistent; }

private:
    bool _is_consistent{true};
};

static ss::future<> do_write_materialized_partition(
  ss::lw_shared_ptr<partition> p, model::record_batch_reader reader) {
    /// Re-write all batch term_ids to 1, otherwise they will carry the
    /// term ids of records coming from parent batches
    auto [crc_success, all_headers_valid, batch_w_correct_terms]
      = co_await std::move(reader).for_each_ref(
        reference_window_consumer(
          model::record_batch_crc_checker(),
          verify_record_batch_consistency(),
          set_term_id_to_zero()),
        model::no_timeout);

    if (!crc_success) {
        throw malformed_batch_exception(
          "Wasm engine failed to compile correct crc check");
    }
    if (!all_headers_valid) {
        throw malformed_batch_exception(
          "Wasm engine returned malformatted batch/header");
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
      .for_each_ref(p->make_appender(write_cfg), model::no_timeout)
      .discard_result();
}

static ss::future<ss::lw_shared_ptr<partition>>
get_partition(partition_manager& pm, model::ntp ntp) {
    auto found = pm.get(ntp);
    if (found) {
        /// Log exists, do nothing and return it
        co_return found;
    }
    /// In the case the storage::log has not been created, but the topic has.
    /// Retry again.
    co_await ss::sleep(100ms);
    throw log_not_yet_created_exception(ssx::sformat(
      "Materialized topic created but underlying log doesn't yet exist: {}",
      ntp));
}

static ss::future<> maybe_make_materialized_log(
  model::topic_namespace source,
  model::topic_namespace new_materialized,
  bool is_leader,
  output_write_args args) {
    const auto& cache = args.metadata.local();
    if (cache.get_topic_cfg(new_materialized)) {
        if (cache.get_source_topic(new_materialized)) {
            /// Log already exists
            co_return;
        }
        /// Attempt to produce onto a normal topic..., shutdown
        throw script_illegal_action_exception(
          args.id,
          fmt::format(
            "Script {} attempted to write to a normal topic: {}",
            args.id,
            new_materialized));
    }
    /// Leader could be on a different machine, can only wait until log comes
    /// into existance
    if (!is_leader) {
        throw follower_create_topic_exception(fmt::format(
          "Follower of source topic {} attempted to created materialzied "
          "topic {} before leader partition had a chance to, sleeping "
          "1s...",
          source,
          new_materialized));
    }
    /// Create new materialized topic
    cluster::non_replicable_topic mt{
      .source = std::move(source), .name = std::move(new_materialized)};
    std::vector<cluster::non_replicable_topic> topics{std::move(mt)};
    /// All requests are debounced, therefore if multiple entities attempt to
    /// create a materialzied topic, all requests will wait for the first to
    /// complete.
    co_return co_await args.frontend.invoke_on(
      cluster::non_replicable_topics_frontend_shard,
      [topics = std::move(topics)](
        cluster::non_replicable_topics_frontend& mtfe) mutable {
          return mtfe.create_non_replicable_topics(
            std::move(topics), model::max_duration);
      });
}

static ss::future<> write_materialized_partition(
  const model::ntp& ntp,
  model::record_batch_reader reader,
  ss::lw_shared_ptr<cluster::partition> input,
  output_write_args args) {
    /// For the rational of why theres mutex uses here read relevent comments in
    /// coproc/shared_script_resources.h
    auto found = args.locks.find(ntp);
    if (found == args.locks.end()) {
        found = args.locks.emplace(ntp, mutex()).first;
    }
    return found->second.with(
      [args, ntp, input, reader = std::move(reader)]() mutable {
          model::topic_namespace source(input->ntp().ns, input->ntp().tp.topic);
          model::topic_namespace new_materialized(ntp.ns, ntp.tp.topic);
          return maybe_make_materialized_log(
                   source, new_materialized, input->is_elected_leader(), args)
            .then([args, ntp] { return get_partition(args.pm.local(), ntp); })
            .then([ntp, reader = std::move(reader)](
                    ss::lw_shared_ptr<partition> p) mutable {
                return do_write_materialized_partition(p, std::move(reader))
                  .handle_exception([ntp](std::exception_ptr eptr) {
                      try {
                          std::rethrow_exception(eptr);
                      } catch (const coproc::exception& ex) {
                          return ss::make_exception_future<>(ex);
                      } catch (const std::exception& ex) {
                          return ss::make_exception_future<>(
                            write_error_exception(ex.what()));
                      }
                  });
            });
      });
}

static ss::future<> process_one_reply(
  process_batch_reply::data e,
  ss::lw_shared_ptr<source> src,
  output_write_args args) {
    if (e.id != args.id()) {
        /// Engine got response mixed up with another request, protocol error
        throw bad_reply_exception(ssx::sformat(
          "erranous reply from wasm engine, mismatched id observed, expected: "
          "{} and observed {}",
          args.id,
          e.id));
    }
    if (!e.reader) {
        /// The wasm engine set the reader to std::nullopt meaning a fatal
        /// error has occurred and redpanda should not send more data to
        /// this coprocessor
        throw script_failed_exception(
          e.id,
          ssx::sformat(
            "script id {} will auto deregister due to an internal syntax "
            "error",
            e.id));
    }
    if (args.denylist.contains(e.ntp)) {
        /// The script attempted to write to a blacklisted ntp, meaning that
        /// this ntp has been marked for deletion. Ignore this particular
        /// portion of the request, until the ntp leaves the denlylist (after it
        /// has been fully torn down). If the script later responds with this
        /// ntp, it will be eventually re-created.
        co_return;
    }
    /// Possible filter response
    if (e.ntp == e.source) {
        /// If the reader is empty, then the wasm engine returned a nil
        /// response indicating a filter is desired to be performed.
        auto data = co_await model::consume_reader_to_memory(
          std::move(*e.reader), model::no_timeout);
        if (!data.empty()) {
            /// Otherwise the user attempted to produce onto a source topic
            throw script_failed_exception(
              e.id,
              ssx::sformat(
                "Script {} attempted to produce onto source topic {}",
                e.id,
                e.source));
        }
        co_return;
    }
    auto [cur, new_topic] = src->wctx.offsets.try_emplace(
      e.ntp, src->rctx.absolute_start);
    if (new_topic && src->rctx.last_acked > cur->second) {
        /// New topic appears after coprocs first batch of initially created
        /// topics. The new topic should start from its respective absolute
        /// offset, if this is behind the read head, initiate retry
        throw retry_trigger_exception(
          ssx::sformat("Partition {} identified as behind", e.ntp));
    }
    /// Only write to partitions for which are up to date with the current
    /// in progress read. Upon success bump offset to next batches start
    if (cur->second == src->rctx.last_acked) {
        co_await write_materialized_partition(
          e.ntp, std::move(*e.reader), src->rctx.input, args);
        src->wctx.offsets[e.ntp] = src->rctx.last_read;
    } else {
        /// In this case a retry is in progress and a response is being
        /// processed for a materialized log that is already up to date.
        /// Publishing here would re-write already written data, so do
        /// nothing
    }
}

using grouping_t = absl::flat_hash_map<model::ntp, output_write_inputs>;

static grouping_t group_replies(output_write_inputs&& replies) {
    grouping_t gt;
    for (auto& r : replies) {
        auto [itr, _] = gt.try_emplace(r.source);
        itr->second.push_back(std::move(r));
    }
    return gt;
}

static ss::future<> process_reply_group(
  model::ntp source, output_write_inputs reply_group, output_write_args args) {
    auto found = args.inputs.find(source);
    if (found == args.inputs.end()) {
        throw bad_reply_exception(
          ssx::sformat("script {} unknown source ntp: {}", args.id, source));
    }
    auto src_ptr = found->second;
    for (auto& e : reply_group) {
        co_await process_one_reply(std::move(e), src_ptr, args);
    }
    /// If we've reached here without error, all materialized
    /// partitions (working under the input) can be promoted, rational is to
    /// support filter operations, i.e. responses were returned for some
    /// materialized partitions but not others. If offsets for missing ones
    /// were not bumped in case of success, there would be an infinite retry
    /// loop.
    for (auto& [_, o] : src_ptr->wctx.offsets) {
        if (o <= src_ptr->rctx.last_read) {
            /// Omit increasing offets that are ahead of current read, this
            /// could occur upon load of a stored offset thats further ahead.
            /// Also during retry avoids promoting offsets of materialized
            /// topics that are ahead of the materialized topic that is the
            /// intended target for the current read
            o = src_ptr->rctx.last_read;
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
    grouping_t grs = group_replies(std::move(replies));
    bool err{false};
    co_await ss::parallel_for_each(
      grs, [args, &err](grouping_t::value_type& vt) -> ss::future<> {
          return process_reply_group(vt.first, std::move(vt.second), args)
            .handle_exception([args, &err](const std::exception_ptr& e) {
                try {
                    std::rethrow_exception(e);
                } catch (const bad_reply_exception& ex) {
                    vlog(coproclog.error, "No source for reply: {}", ex);
                } catch (const script_failed_exception& ex) {
                    throw ex;
                } catch (const malformed_batch_exception& ex) {
                    throw engine_protocol_failure(args.id, ex.what());
                } catch (
                  const cluster::non_replicable_topic_creation_exception& ex) {
                    vlog(
                      coproclog.warn,
                      "Failed to create non_replicable topic: {}",
                      ex);
                } catch (const coproc::exception& ex) {
                    /// For any type of failure the offset will not be touched,
                    /// the read phase will always read from the global min of
                    /// all offsets ever registered.
                    vlog(
                      coproclog.info, "Error while processing reply: {}", ex);
                    err = true;
                }
            });
      });
    if (err) {
        co_await ss::sleep(100ms);
    }
}

} // namespace coproc
