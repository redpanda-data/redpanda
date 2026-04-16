// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/fetch.h"

#include "base/likely.h"
#include "base/vlog.h"
#include "cluster/metadata_cache.h"
#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "config/configuration.h"
#include "container/chunked_vector.h"
#include "features/enterprise_feature_messages.h"
#include "kafka/data/partition_proxy.h"
#include "kafka/protocol/batch_consumer.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/fetch.h"
#include "kafka/server/fetch_session.h"
#include "kafka/server/fwd.h"
#include "kafka/server/handlers/details/leader_epoch.h"
#include "kafka/server/handlers/fetch/fetch_plan_executor.h"
#include "kafka/server/handlers/fetch/fetch_planner.h"
#include "kafka/server/handlers/fetch/replica_selector.h"
#include "kafka/server/kafka_probe.h"
#include "kafka/server/read_distribution_probe.h"
#include "model/fundamental.h"
#include "model/kitp.h"
#include "model/ktp.h"
#include "model/limits.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/timeout_clock.h"
#include "net/connection.h"
#include "random/generators.h"
#include "ssx/future-util.h"
#include "ssx/semaphore.h"

#include <seastar/core/do_with.hh>
#include <seastar/core/future.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/timer.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/util/log.hh>

#include <boost/range/irange.hpp>
#include <fmt/ostream.h>

#include <chrono>
#include <exception>
#include <ranges>

namespace {

std::optional<kafka::leader_id_and_epoch> get_leader_id_and_epoch(
  const cluster::metadata_cache& md_cache, const model::ktp& ktp) {
    auto lt = md_cache.get_leader_term(ktp.as_tn_view(), ktp.get_partition());
    if (lt && lt->leader) {
        return kafka::leader_id_and_epoch{
          .leader_id = *lt->leader,
          .leader_epoch = kafka::leader_epoch_from_term(lt->term)};
    }
    return std::nullopt;
}

kafka::read_result make_errored_read_result(
  const cluster::metadata_cache& md_cache,
  const model::ktp& ktp,
  kafka::error_code err) {
    if (auto l = get_leader_id_and_epoch(md_cache, ktp); l.has_value()) {
        // remap unknown to not_leader as it's in the metadata_cache
        if (err == kafka::error_code::unknown_topic_or_partition) {
            err = kafka::error_code::not_leader_for_partition;
        }
        return {err, std::move(*l)};
    }
    return kafka::read_result(err);
}
} // namespace

namespace kafka {
static constexpr std::chrono::milliseconds default_fetch_timeout = 5s;
/**
 * Make a partition response error.
 */
static fetch_response::partition_response
make_partition_response_error(model::partition_id p_id, error_code error) {
    return fetch_response::partition_response{
      .partition_index = p_id,
      .error_code = error,
      .high_watermark = model::offset(-1),
      .last_stable_offset = model::offset(-1),
      .log_start_offset = model::offset(-1),
      .records = batch_reader(),
    };
}

/**
 * Low-level handler for reading from an ntp. Runs on ntp's home core.
 */
static ss::future<read_result> read_from_partition(
  kafka::partition_proxy part,
  model::offset lso,
  fetch_config config,
  std::optional<model::timeout_clock::time_point> deadline) {
    auto hw = part.high_watermark();
    auto start_o = part.start_offset();
    // if we have no data read, return fast
    if (
      hw < config.start_offset || config.skip_read
      || config.start_offset > config.max_offset) {
        co_return read_result(start_o, hw, lso);
    }

    kafka::log_reader_config reader_config(
      model::offset_cast(config.start_offset),
      model::offset_cast(config.max_offset),
      0,
      config.max_bytes,
      std::nullopt,
      config.abort_source.has_value()
        ? config.abort_source.value().get().local()
        : model::opt_abort_source_t{},
      config.client_address,
      config.strict_max_bytes);

    auto rdr = co_await part.make_reader(reader_config);
    std::exception_ptr e;
    std::unique_ptr<iobuf> data;
    std::vector<cluster::tx::tx_range> aborted_transactions;
    std::optional<std::chrono::milliseconds> delta_from_tip_ms;
    model::offset data_base_offset, data_last_offset;
    size_t batch_count = 0;

    try {
        auto result = co_await rdr.reader.consume(
          kafka_batch_serializer(), deadline ? *deadline : model::no_timeout);
        data = std::make_unique<iobuf>(std::move(result.data));
        data_base_offset = result.base_offset;
        data_last_offset = result.last_offset;
        batch_count = result.batch_count;
        part.probe().add_records_fetched(result.record_count);
        part.probe().add_bytes_fetched(data->size_bytes());
        if (!part.is_leader() && config.read_from_follower) {
            part.probe().add_bytes_fetched_from_follower(data->size_bytes());
        }

        if (data->size_bytes() > 0) {
            auto curr_timestamp = model::timestamp::now();
            if (curr_timestamp >= result.first_timestamp) {
                delta_from_tip_ms = std::chrono::milliseconds{
                  curr_timestamp() - result.first_timestamp()};
            }
        }
        // Only return aborted transactions range if consumer is using
        // read_committed isolation level and there are tx batches in the
        // response
        if (
          config.isolation_level == model::isolation_level::read_committed
          && result.first_tx_batch_offset && result.record_count > 0) {
            // Reader should live at least until this point to hold on to the
            // segment locks so that prefix truncation doesn't happen.
            aborted_transactions = co_await part.aborted_transactions(
              result.first_tx_batch_offset.value(),
              result.last_offset,
              std::move(rdr.ot_state));

            // Check that the underlying data did not get truncated while
            // consuming. If so, it's possible the search for aborted
            // transactions missed out on transactions that correspond to the
            // read batches.
            auto start_o = part.start_offset();
            if (config.start_offset < start_o) {
                co_return read_result(
                  error_code::offset_out_of_range,
                  start_o,
                  part.high_watermark(),
                  lso);
            }
        }

    } catch (...) {
        e = std::current_exception();
    }

    co_await std::move(rdr.reader).release()->finally();

    if (e) {
        std::rethrow_exception(e);
    }

    co_return read_result(
      std::move(data),
      start_o,
      data_base_offset,
      data_last_offset,
      batch_count,
      hw,
      lso,
      delta_from_tip_ms,
      std::move(aborted_transactions));
}

/**
 * Entry point for reading from an ntp. This is executed on NTP home core and
 * build error responses if anything goes wrong.
 */
static ss::future<read_result> do_read_from_ntp(
  cluster::partition_manager& cluster_pm,
  const cluster::metadata_cache& md_cache,
  const replica_selector& replica_selector,
  ntp_fetch_config ntp_config,
  std::optional<model::timeout_clock::time_point> deadline,
  const bool obligatory_batch_read,
  fetch_memory_units_manager& units_mgr) {
    // If it's the obligatory batch read then we need to allow for the
    // configured max bytes to exceeded if the next batch in the partition
    // is larger. This is needed to conform with KIP-74.
    ntp_config.cfg.strict_max_bytes = !obligatory_batch_read;

    // control available memory
    auto memory_units = units_mgr.zero_units();
    if (!ntp_config.cfg.skip_read) {
        memory_units = units_mgr.allocate_memory_units(
          ntp_config.ktp(),
          ntp_config.cfg.max_bytes,
          ntp_config.cfg.max_batch_size,
          ntp_config.cfg.avg_batch_size,
          obligatory_batch_read);
        if (!memory_units.has_units()) {
            ntp_config.cfg.skip_read = true;
        } else if (ntp_config.cfg.max_bytes > memory_units.num_units()) {
            ntp_config.cfg.max_bytes = memory_units.num_units();
        }
    }

    /*
     * lookup the ntp's partition
     */
    auto kafka_partition = make_partition_proxy(ntp_config.ktp(), cluster_pm);
    if (unlikely(!kafka_partition)) {
        co_return make_errored_read_result(
          md_cache, ntp_config.ktp(), error_code::unknown_topic_or_partition);
    }
    if (!ntp_config.cfg.read_from_follower && !kafka_partition->is_leader()) {
        co_return make_errored_read_result(
          md_cache, ntp_config.ktp(), error_code::not_leader_for_partition);
    }

    /**
     * validate leader epoch. for more details see KIP-320
     */
    auto leader_epoch_err = details::check_leader_epoch(
      ntp_config.cfg.current_leader_epoch, *kafka_partition);
    if (leader_epoch_err != error_code::none) {
        co_return make_errored_read_result(
          md_cache, ntp_config.ktp(), leader_epoch_err);
    }
    auto offset_ec = co_await kafka_partition->validate_fetch_offset(
      ntp_config.cfg.start_offset,
      ntp_config.cfg.read_from_follower,
      default_fetch_timeout + model::timeout_clock::now());

    auto maybe_lso = kafka_partition->last_stable_offset();
    if (unlikely(!maybe_lso)) {
        // partition is still bootstrapping
        co_return read_result(maybe_lso.error());
    }

    if (config::shard_local_cfg().enable_transactions.value()) {
        if (
          ntp_config.cfg.isolation_level
          == model::isolation_level::read_committed) {
            ntp_config.cfg.max_offset = model::prev_offset(maybe_lso.value());
        }
    }

    if (offset_ec != error_code::none) {
        co_return read_result(
          offset_ec,
          kafka_partition->start_offset(),
          kafka_partition->high_watermark(),
          maybe_lso.value());
    }
    if (
      config::shard_local_cfg().enable_rack_awareness.value()
      && ntp_config.cfg.consumer_rack_id && kafka_partition->is_leader()) {
        auto p_info_res = kafka_partition->get_partition_info();
        if (p_info_res.has_error()) {
            // TODO: add mapping here
            co_return read_result(error_code::not_leader_for_partition);
        }
        auto p_info = std::move(p_info_res.value());

        auto preferred_replica = replica_selector.select_replica(
          consumer_info{
            .fetch_offset = ntp_config.cfg.start_offset,
            .rack_id = ntp_config.cfg.consumer_rack_id},
          p_info);
        if (preferred_replica && preferred_replica.value() != p_info.leader) {
            vlog(
              klog.trace,
              "Consumer in rack: {}, preferred replica id: {}",
              *ntp_config.cfg.consumer_rack_id,
              preferred_replica.value());
            co_return read_result(
              kafka_partition->start_offset(),
              kafka_partition->high_watermark(),
              maybe_lso.value(),
              preferred_replica);
        }
    }
    auto result = co_await read_from_partition(
      std::move(*kafka_partition), maybe_lso.value(), ntp_config.cfg, deadline);

    // Note that units can be both increased and decreassed here. Increases
    // happen because there is no strict limit on read size when reading the
    // obligatory batch.
    memory_units.adjust_units(result.data_size_bytes());
    result.memory_units = std::move(memory_units);
    co_return result;
}

namespace testing {

ss::future<read_result> read_from_ntp(
  cluster::partition_manager& cluster_pm,
  const cluster::metadata_cache& md_cache,
  const replica_selector& replica_selector,
  const model::ktp& ktp,
  fetch_config config,
  std::optional<model::timeout_clock::time_point> deadline,
  const bool obligatory_batch_read,
  fetch_memory_units_manager& units_mgr) {
    return do_read_from_ntp(
      cluster_pm,
      md_cache,
      replica_selector,
      {{ktp.get_topic(), ktp.get_partition()}, std::move(config)},
      deadline,
      obligatory_batch_read,
      units_mgr);
}

} // namespace testing

static void fill_fetch_responses(
  op_context& octx,
  chunked_vector<read_result> results,
  const chunked_vector<op_context::response_placeholder_ptr>& responses,
  op_context::latency_point start_time,
  bool record_latency = true) {
    auto range = boost::irange<size_t>(0, results.size());
    if (unlikely(results.size() != responses.size())) {
        // soft assert & recovery attempt
        vlog(
          klog.error,
          "Results and responses counts must be the same. "
          "results: {}, responses: {}. "
          "Only the common subset will be processed",
          results.size(),
          responses.size());
        range = boost::irange<size_t>(
          0, std::min({results.size(), responses.size()}));
    }

    for (auto idx : range) {
        auto& res = results[idx];
        const auto& resp_it = responses[idx];
        const auto& ktp = resp_it->ktp();

        fetch_response::partition_response resp;
        resp.partition_index = res.partition;
        resp.error_code = res.error;
        if (res.current_leader) {
            resp.current_leader = *res.current_leader;
        }

        // These are set to -1 in the general error case.
        // Set to actual values in the success case or when the error is
        // offset_out_of_range as the client can make use of the returned
        // offsets.
        resp.log_start_offset = res.start_offset;
        resp.high_watermark = res.high_watermark;
        resp.last_stable_offset = res.last_stable_offset;

        // error case
        if (unlikely(resp.error_code != error_code::none)) {
            resp.records = batch_reader();
            resp_it->set(std::move(resp), {});
            continue;
        }

        /**
         * Cache fetch metadata
         */
        octx.rctx.get_fetch_metadata_cache().insert_or_assign(
          ktp,
          res.start_offset,
          res.high_watermark,
          res.last_stable_offset,
          res.offset_count(),
          res.batch_count,
          res.data_size_bytes());
        /**
         * Over response budget, we will just waste this read, it will cause
         * data to be stored in the cache so next read is fast
         */
        if (res.preferred_replica) {
            resp.preferred_read_replica = *res.preferred_replica;
        }

        std::optional<fetch_memory_units> resp_units{};
        auto current_response_size = resp_it->response_size();
        auto bytes_left = octx.bytes_left - current_response_size;

        /**
         * According to KIP-74 we have to return first batch even if it would
         * violate max_bytes fetch parameter
         */
        if (
          res.has_data()
          && (bytes_left >= res.data_size_bytes() || octx.response_size == 0)) {
            /**
             * set aborted transactions if present
             */
            if (!res.aborted_transactions.empty()) {
                chunked_vector<fetch_response::aborted_transaction> aborted;
                aborted.reserve(res.aborted_transactions.size());
                std::transform(
                  res.aborted_transactions.begin(),
                  res.aborted_transactions.end(),
                  std::back_inserter(aborted),
                  [](cluster::tx::tx_range range) {
                      return fetch_response::aborted_transaction{
                        .producer_id = kafka::producer_id(range.pid.id),
                        .first_offset = range.first};
                  });
                resp.aborted_transactions = std::move(aborted);
            }
            resp_units = std::move(res.memory_units);
            resp.records = batch_reader(std::move(res).release_data());
        } else {
            if (res.has_data()) {
                // Data was read from cloud storage but cannot fit in the
                // response budget. This is pure read amplification: S3 bytes
                // were downloaded, materialized, and now dropped.
                octx.rctx.probe().add_fetch_response_dropped_bytes(
                  res.data_size_bytes());
            }
            resp.records = batch_reader();
        }

        resp_it->set(std::move(resp), std::move(resp_units));

        if (record_latency) {
            std::chrono::microseconds fetch_latency
              = std::chrono::duration_cast<std::chrono::microseconds>(
                op_context::latency_clock::now() - start_time);
            octx.rctx.probe().record_fetch_latency(fetch_latency);
        }
    }
}

static ss::future<chunked_vector<read_result>> fetch_ntps(
  cluster::partition_manager& cluster_pm,
  const cluster::metadata_cache& md_cache,
  const replica_selector& replica_selector,
  chunked_vector<ntp_fetch_config> ntp_fetch_configs,
  read_distribution_probe& read_probe,
  std::optional<model::timeout_clock::time_point> deadline,
  model::timeout_clock::time_point fetch_deadline,
  const size_t bytes_left,
  fetch_memory_units_manager& units_mgr) {
    size_t total_read_size = 0;

    // bytes_left comes from the fetch plan and also accounts for the max_bytes
    // field in the fetch request
    const size_t max_bytes_per_fetch = std::min<size_t>(
      config::shard_local_cfg().kafka_max_bytes_per_fetch(), bytes_left);

    const auto config_indexes = std::views::iota(
      (size_t)0, ntp_fetch_configs.size());

    chunked_vector<read_result> results;
    results.reserve(ntp_fetch_configs.size());
    for (const auto& _ : config_indexes) {
        results.emplace_back(error_code::none);
    }

    // Ensure fetch_deadline is at least as large as deadline. The deadline is
    // the point in time at which fetch.max.wait has elapsed. The fetch_deadline
    // is the point in time before which we want to complete the fetch request.
    // The fetch_deadline is >= deadline and the storage layer tries to stop
    // reading if this deadline has been exceeded.
    fetch_deadline = std::max(
      deadline.value_or(model::timeout_clock::time_point::min()),
      fetch_deadline);

    co_await ss::max_concurrent_for_each(
      config_indexes,
      config::shard_local_cfg().fetch_max_read_concurrency(),
      [&](auto cfg_idx) {
          auto& ntp_cfg = ntp_fetch_configs[cfg_idx];

          // Strict checking/enforcing of max bytes per fetch occurs in
          // `fill_fetch_responses`. This check only exists to avoid unneeded
          // partition reads.
          if (total_read_size >= max_bytes_per_fetch) {
              ntp_cfg.cfg.skip_read = true;
          }

          // In Kafka first non-empty partition in a request or session
          // is considered the `obligatory` batch read. The logic below
          // is designed to approximate this behavior. Up to
          // `fetch_max_read_concurrency` partition reads will be considered
          // obligatory until a batch is read.
          const bool obligatory_batch_read = total_read_size == 0;

          return do_read_from_ntp(
                   cluster_pm,
                   md_cache,
                   replica_selector,
                   ntp_cfg,
                   fetch_deadline,
                   obligatory_batch_read,
                   units_mgr)
            .then([&, cfg_idx](read_result&& res) {
                res.partition = ntp_cfg.ktp().get_partition();

                auto read_size = res.data_size_bytes();
                total_read_size += read_size;

                if (res.delta_from_tip_ms.has_value()) {
                    read_probe.add_read_event_delta_from_tip(
                      res.delta_from_tip_ms.value());
                }

                results[cfg_idx] = std::move(res);
            })
            .handle_exception([&, cfg_idx](const std::exception_ptr& e) {
                bool is_shutdown = ssx::is_shutdown_exception(e);
                // Return not_leader_for_partition error to force clients retry
                // for potential transient errors.
                auto ec = error_code::not_leader_for_partition;
                vlogl(
                  klog,
                  is_shutdown ? ss::log_level::debug : ss::log_level::warn,
                  "ntp {}: caught unhandled exception {} in fetch path",
                  ntp_cfg.ktp(),
                  e);
                auto res = make_errored_read_result(
                  md_cache, ntp_cfg.ktp(), ec);
                res.partition = ntp_cfg.ktp().get_partition();
                results[cfg_idx] = std::move(res);
            });
      });

    vlog(
      klog.trace,
      "fetch_ntps: for {} partitions returning {} total bytes",
      results.size(),
      total_read_size);
    co_return results;
}

bool shard_fetch::empty() const {
    if (unlikely(requests.size() != responses.size())) {
        vlog(
          klog.error,
          "there have to be equal number of fetch requests and responses"
          " for single shard. requests: {}, responses: {}",
          requests.size(),
          responses.size());
    }
    return requests.empty();
}

class fetch_worker {
public:
    // Passed from the coordinator shard to fetch workers.
    // Contains either references to objects local to the fetch worker shard or
    // copies of data from the coordinator shard.
    struct shard_local_fetch_context {
        size_t bytes_left;
        // Specifies the minimum number of bytes this sub-fetch should read
        // before returning.
        size_t min_bytes;

        // Debounce timeout
        std::optional<model::timeout_clock::time_point> deadline;

        // If set then the sub-fetch should return by the specified time_point .
        model::timeout_clock::time_point fetch_deadline;

        // The fetch sub-requests of partitions local to the shard this worker
        // is running on.
        chunked_vector<ntp_fetch_config> requests;

        // References to services local to the shard this worker is running on.
        // They are protected from deletion by the coordinator.
        server& srv;
        cluster::partition_manager& mgr;
        ss::abort_source& as;
    };

    explicit fetch_worker(shard_local_fetch_context ctx)
      : _ctx(std::move(ctx)) {}

    struct worker_result {
        chunked_vector<read_result> read_results;
        // The total amount of bytes read across all results in `read_results`.
        size_t total_size;
        // The time it took for the first `fetch_ntps` to complete
        std::chrono::microseconds first_run_latency_result;
    };

    ss::future<worker_result> run() {
        // Set the worker's abort source to be a child of the orchestrator's
        // abort_source.
        auto sub_opt = _ctx.as.subscribe([this]() noexcept {
            _as.request_abort();
            _completed_waiter_count.signal();
        });

        if (!sub_opt) {
            // `_ctx.as` must've already been aborted. Query partitions once
            // then return.
            _as.request_abort();
        }

        co_return co_await do_run().finally([this] {
            // Ensure all offset waiters have been removed before returning.
            _as.request_abort();
            return _waiter_gate.close();
        });
    }

private:
    shard_local_fetch_context _ctx;
    ss::gate _waiter_gate;
    ss::abort_source _as;

    // This should roughly correspond to `_request_indexes.size()`. It's used to
    // wait for partitions to have an offset change so that it's corresponding
    // request in the fetch can be re-queried.
    ssx::semaphore _completed_waiter_count{
      0, "fetch_worker._completed_waiter_count"};
    // This contains indexes into `_ctx.requests` and is populated by raft
    // consensus waiters when an offset increases for a partition.
    std::vector<size_t> _request_indexes;
    // Contains the last observed value of `consensus->last_visible_index()` for
    // every request in `_ctx.requests`. Its used to register waiters with
    // `consensus->visible_offset_monitor()`.
    std::vector<model::offset> _last_visible_indexes;

    struct query_results {
        std::vector<model::offset> last_visible_indexes;
        // Indicates if any `read_result` in `results` has an error.
        bool has_error;
        chunked_vector<read_result> results;
        size_t total_size;
    };

    ss::future<query_results>
    query_requests(chunked_vector<ntp_fetch_config> requests) {
        // The last visible indexes need to be populated before partitions
        // are read. If they are populated afterwards then the
        // last_visible_index could be updated after the partition is read,
        // but before the index is queried. This leads to a race condition
        // where we're waiting for the last_visible_index to change even
        // though the partition has data we haven't read.
        //
        // Note that `last_visible_index` is used instead of the
        // `commit_offset` as the commit offset won't change when partitions
        // are produced to without acks=all. The `last_visible_index` more
        // closely corresponds to the Kafka high watermark as well.
        std::vector<model::offset> last_visible_indexes(requests.size());
        std::vector<std::pair<size_t, read_result>> errored_partitions;
        size_t total_size{0};
        bool has_error{false};

        for (size_t i = 0; i < requests.size(); i++) {
            const auto& req = requests[i];
            auto part = _ctx.mgr.get(req.ktp());
            auto consensus = part ? part->raft() : nullptr;
            if (!consensus) {
                errored_partitions.emplace_back(
                  i,
                  make_errored_read_result(
                    _ctx.srv.metadata_cache(),
                    req.ktp(),
                    kafka::error_code::not_leader_for_partition));
                errored_partitions.back().second.partition
                  = req.ktp().get_partition();
                continue;
            }
            last_visible_indexes[i] = consensus->last_visible_index();
        }

        // A read_result needs to be returned for every partition. Hence,
        // the function can't return before calling
        // `fetch_ntps`.
        auto results = co_await fetch_ntps(
          _ctx.mgr,
          _ctx.srv.metadata_cache(),
          _ctx.srv.get_replica_selector(),
          std::move(requests),
          _ctx.srv.read_probe(),
          _ctx.deadline,
          _ctx.fetch_deadline,
          _ctx.bytes_left,
          _ctx.srv.fetch_units_manager());

        // If we weren't able to read the last_visible_index for a partition
        // before calling `fetch_ntps_in_parallel` then we need to
        // return with an error for that partition.
        for (auto& [i, result] : errored_partitions) {
            results[i] = std::move(result);
        }

        for (const auto& r : results) {
            total_size += r.data_size_bytes();
            if (r.error != error_code::none) {
                has_error = true;
            }
        }

        co_return query_results{
          .last_visible_indexes = std::move(last_visible_indexes),
          .has_error = has_error,
          .results = std::move(results),
          .total_size = total_size,
        };
    }

    // Registers a `visible_offset_monitor` waiter for every index in
    // `request_indexes`
    //
    // `request_indexes` should contain valid indexes into `_ctx.requests`
    //
    // If a registration fails for a given index then the function immediately
    // returns with that index without registering any further waiters.
    // Otherwise the function returns std::nullopt to indicate success.
    std::optional<size_t> register_waiters(const auto& request_indexes) {
        for (size_t i : request_indexes) {
            auto part = _ctx.mgr.get(_ctx.requests[i].ktp());
            // If the partition can't be found then it's since been moved
            if (!part) {
                return {i};
            }

            auto consensus = part->raft();
            if (!consensus) {
                return {i};
            }

            auto offset = model::next_offset(_last_visible_indexes[i]);
            auto waiter = consensus->visible_offset_monitor()
                            .wait(offset, model::no_timeout, _as)
                            // All exceptions are ignored here as this is only
                            // used to signal the worker that another attempt to
                            // read the partition should be made.
                            .handle_exception([](const std::exception_ptr&) {});

            ssx::spawn_with_gate(
              _waiter_gate, [this, w = std::move(waiter), i]() mutable {
                  return w.finally([this, i] {
                      _request_indexes.push_back(i);
                      _completed_waiter_count.signal();
                  });
              });
        }

        return {};
    }

    ss::future<worker_result> do_run() {
        bool first_run{true};
        std::chrono::microseconds first_run_latency_result{0};
        // A map of indexes in `requests` to their corresponding index in
        // `_ctx.requests`.
        std::vector<size_t> requests_map;

        chunked_vector<read_result> results;
        size_t total_size{0};

        for (;;) {
            chunked_vector<ntp_fetch_config> requests;

            if (first_run) {
                requests = _ctx.requests.copy();
            } else {
                requests_map.clear();

                for (auto i : _request_indexes) {
                    requests.push_back(_ctx.requests[i]);
                    requests_map.push_back(i);
                }

                _request_indexes.clear();
                // All `_request_indexes` have been read. Reset counter
                _completed_waiter_count.consume(
                  _completed_waiter_count.current());
            }

            std::optional<op_context::latency_point> start_time;
            if (first_run) {
                start_time = op_context::latency_clock::now();
            }

            auto q_results = co_await query_requests(std::move(requests));
            if (first_run) {
                results = std::move(q_results.results);
                total_size = q_results.total_size;

                _last_visible_indexes = std::move(
                  q_results.last_visible_indexes);
                first_run_latency_result
                  = std::chrono::duration_cast<std::chrono::microseconds>(
                    op_context::latency_clock::now() - *start_time);
            } else {
                // Override the older results of the partitions with the newly
                // queried results.
                for (size_t i = 0; i < requests_map.size(); i++) {
                    auto r_i = requests_map[i];
                    auto& r = results[r_i];
                    total_size -= r.data_size_bytes();
                    r = std::move(q_results.results[i]);
                    total_size += r.data_size_bytes();

                    _last_visible_indexes[r_i]
                      = q_results.last_visible_indexes[i];
                }
            }

            if (
              total_size >= _ctx.min_bytes || q_results.has_error
              || _as.abort_requested()) {
                co_return worker_result{
                  .read_results = std::move(results),
                  .total_size = total_size,
                  .first_run_latency_result = first_run_latency_result,
                };
            }

            std::optional<size_t> has_errored_request;
            if (first_run) {
                has_errored_request = register_waiters(
                  std::ranges::iota_view{0ull, _ctx.requests.size()});
            } else {
                has_errored_request = register_waiters(requests_map);
            }

            if (has_errored_request) {
                auto r_i = has_errored_request.value();
                total_size -= results[r_i].data_size_bytes();
                results[r_i] = read_result(
                  error_code::not_leader_for_partition);
                results[r_i].partition
                  = _ctx.requests[r_i].ktp().get_partition();
                co_return worker_result{
                  .read_results = std::move(results),
                  .total_size = total_size,
                  .first_run_latency_result = first_run_latency_result,
                };
            }

            co_await _completed_waiter_count.wait();

            if (_as.abort_requested()) {
                co_return worker_result{
                  .read_results = std::move(results),
                  .total_size = total_size,
                  .first_run_latency_result = first_run_latency_result,
                };
            }

            first_run = false;
        }
    }
};

/*
 * A fetch exeutor that relies on notifications from raft to determine when to
 * query partitions rather than querying all partitions at a set polling
 * interval like the `parallel_fetch_plan_executor`.
 */
class nonpolling_fetch_plan_executor final : public fetch_plan_executor::impl {
public:
    nonpolling_fetch_plan_executor()
      : _last_result_size(ss::smp::count, 0)
      , _fetch_timeout{[this] { _has_progress.signal(); }} {}

    /**
     * Executes the supplied `plan` until `octx.should_stop_fetch` returns true.
     */
    ss::future<> execute_plan(op_context& octx, fetch_plan plan) final {
        auto fetch_read_strategy
          = config::shard_local_cfg().fetch_read_strategy();
        if (is_fetch_strategy_with_debounce(fetch_read_strategy)) {
            co_await ss::sleep(
              std::min(
                config::shard_local_cfg().fetch_reads_debounce_timeout(),
                octx.request.data.max_wait_ms));
        }
        // Ensure both fetch debounce and the fetch scheduling group are enabled
        // before trying to apply any delay.
        if (
          fetch_read_strategy
            == model::fetch_read_strategy::non_polling_with_pid
          && config::shard_local_cfg().use_fetch_scheduler_group()) {
            auto& pid = octx.rctx.server().local().pid_controller();
            auto delay = pid.current_delay();
            if (delay > 0s) {
                // Avoid unneeded scheduling points in cases where delay is
                // zero.
                co_await ss::sleep(
                  std::min(delay, octx.request.data.max_wait_ms));
            }
        }

        if (!initialize_progress_conditions(octx)) {
            // if the progress conditions were unable to be initialized then
            // either the fetch has been aborted or the deadline was reached.
            co_return;
        }

        start_worker_aborts(plan);
        co_await handle_exceptions(do_execute_plan(octx, std::move(plan)));

        // Send abort signal to workers and wait for all workers to end before
        // returning.
        co_await abort_workers().finally(
          [this] { return _workers_gate.close(); });

        if (_thrown_exception) {
            std::rethrow_exception(_thrown_exception);
        }
    }

private:
    ss::future<> do_execute_plan(op_context& octx, fetch_plan plan) {
        // start fetching from a random shard to make sure that we fetch data
        // from all the partitions even if we reach fetch message size limit
        const ss::shard_id start_shard_idx = random_generators::get_int(
          ss::smp::count - 1);
        for (size_t i = 0; i < ss::smp::count; ++i) {
            auto shard = (start_shard_idx + i) % ss::smp::count;

            ssx::spawn_with_gate(_workers_gate, [&]() mutable {
                return handle_exceptions(start_shard_fetch_worker(
                  octx, std::move(plan.fetches_per_shard[shard]), 0));
            });
        }

        for (;;) {
            co_await wait_for_progress();

            if (octx.should_stop_fetch() || _thrown_exception) {
                co_return;
            }

            std::vector<shard_fetch> completed_shard_fetches = std::move(
              _completed_shard_fetches);
            _completed_shard_fetches.clear();

            for (auto& sf : completed_shard_fetches) {
                ssx::spawn_with_gate(
                  _workers_gate, [this, &octx, sf = std::move(sf)]() mutable {
                      auto shard = sf.shard;
                      return handle_exceptions(start_shard_fetch_worker(
                        octx,
                        std::move(sf),
                        // Require that a worker returns more data than before.
                        // Otherwise it'll return right away with the previous
                        // result.
                        _last_result_size[shard] + 1));
                  });
            }
        }
    }

    /**
     * Creates abort sources for shards with non-empty sub-fetches
     */
    void start_worker_aborts(const fetch_plan& plan) {
        for (const auto& fetch : plan.fetches_per_shard) {
            if (!fetch.empty()) {
                _worker_aborts[fetch.shard];
            }
        }
    }

    ss::future<> abort_workers() {
        return seastar::parallel_for_each(_worker_aborts, [](auto& wa) {
            auto& [shard, as] = wa;
            return ss::smp::submit_to(shard, [&as] { as.request_abort(); });
        });
    }

    /**
     * Sets _has_progress to be signaled if;
     * - octx.deadline has been reached.
     * - _as has been aborted.
     * returns true if this was successful
     *         false otherwise
     */
    bool initialize_progress_conditions(op_context& octx) {
        // A connection can close and stop the sharded abort source before we
        // can subscribe to it. So we check here if that is the case and return
        // if so.
        if (!octx.rctx.abort_source().local_is_initialized()) {
            return false;
        }

        _fetch_abort_sub = octx.rctx.abort_source().subscribe(
          [this]() noexcept { _has_progress.signal(); });

        if (!_fetch_abort_sub) {
            return false;
        }

        if (octx.deadline) {
            _fetch_timeout.arm(octx.deadline.value());
        }

        return true;
    }

    bool is_fetch_strategy_with_debounce(model::fetch_read_strategy s) const {
        if (s == model::fetch_read_strategy::non_polling_with_debounce) {
            return true;
        }

        if (
          s == model::fetch_read_strategy::polling
          && !config::shard_local_cfg()
                .fetch_reads_debounce_timeout.is_default()) {
            return true;
        }

        return false;
    }

    /**
     * Waits until the should_stop_fetch() condition should be checked again.
     * The return future is set if;
     * - octx.deadline has been reached.
     * - _as has been aborted.
     * - one of the shard workers has returned results.
     */
    ss::future<> wait_for_progress() { return _has_progress.wait(); }

    /*
     * `start_shard_fetch_worker` executes on the coordinator shard. It builds
     * the `shard_local_fetch_context` struct needed to start the
     * shard_fetch_worker then makes a cross shard call to the shard fetch
     * worker if needed.
     *
     * It then waits until the shard_fetch_worker completes
     * and then notifies the fetch coordinator.
     */
    ss::future<> start_shard_fetch_worker(
      op_context& octx, shard_fetch fetch, size_t min_fetch_bytes) {
        // if over budget skip the fetch.
        if (octx.bytes_left <= 0) {
            co_return;
        }
        // no requests for this shard, do nothing
        if (fetch.empty()) {
            co_return;
        }

        fetch_worker::worker_result results
          = co_await octx.rctx.partition_manager().invoke_on(
            fetch.shard,
            [this,
             shard = fetch.shard,
             min_fetch_bytes,
             configs = fetch.requests.copy(),
             &octx](cluster::partition_manager& mgr) mutable
              -> ss::future<fetch_worker::worker_result> {
                // Although this and octx are captured by reference across
                // shards it is safe since they are not modified by any shard
                // for the duration of the capture and they are protected from
                // deletion for the duration of the capture by a gate. Both are
                // used immediately on the foreign shard to access data local to
                // that shard. This is meant to help avoiding unintended cross
                // shard access.
                return ss::do_with(
                  fetch_worker(
                    fetch_worker::shard_local_fetch_context{
                      .bytes_left = octx.bytes_left,
                      .min_bytes = min_fetch_bytes,
                      .deadline = octx.deadline,
                      .fetch_deadline = octx.fetch_deadline,
                      .requests = std::move(configs),
                      .srv = octx.rctx.server().local(),
                      .mgr = mgr,
                      .as = _worker_aborts[shard],
                    }),
                  [](auto& worker) { return worker.run(); });
            });

        fill_fetch_responses(
          octx,
          std::move(results.read_results),
          fetch.responses,
          fetch.start_time,
          false);

        octx.rctx.probe().record_fetch_latency(
          results.first_run_latency_result);

        _last_result_size[fetch.shard] = results.total_size;
        _completed_shard_fetches.push_back(std::move(fetch));
        _has_progress.signal();
    }

    static ss::future<> ignore_exceptions(ss::future<> fut) {
        return fut.handle_exception([](const std::exception_ptr&) {});
    }

    ss::future<> handle_exceptions(ss::future<> f) {
        try {
            co_await std::move(f);
        } catch (const seastar::timed_out_error& e) {
            // This exception can occur when the max allowable time for a fetch
            // has passed.
            vlog(klog.info, "timed out error: {}", e);
        } catch (const std::system_error& e) {
            if (net::is_reconnect_error(e)) {
                // This exception commonly occurs when clients disconnect.
                vlog(klog.info, "reconnect error: {}", e);
            } else {
                _thrown_exception = std::current_exception();
            }
        } catch (const seastar::named_semaphore_aborted& e) {
            // This exception commonly occurs when the handler is aborted.
            vlog(klog.info, "semaphore aborted error: {}", e);
        } catch (...) {
            _thrown_exception = std::current_exception();
        }
    }

    ss::gate _workers_gate;
    std::unordered_map<ss::shard_id, ss::abort_source> _worker_aborts;
    ss::condition_variable _has_progress;
    std::vector<shard_fetch> _completed_shard_fetches;
    std::vector<size_t> _last_result_size;
    // If any child task throws an exception this holds on to the exception
    // until all child tasks have been stopped and its safe to rethrow the
    // exception.
    std::exception_ptr _thrown_exception;
    ss::optimized_optional<ss::abort_source::subscription> _fetch_abort_sub;
    ss::timer<model::timeout_clock> _fetch_timeout;
};

size_t op_context::fetch_partition_count() const {
    if (
      session_ctx.is_sessionless()
      || (session_ctx.is_full_fetch() && initial_fetch)) {
        // too hard to get the right size, this is only an estimate
        return 0;
    } else {
        return session_ctx.session()->partitions().size();
    }
}

namespace {

// Calls f for each topic in the request, passing the associated partitions.
// This exists to adapt the sessionfull and sessionless fetch cases to a
// consistent iteration interface.
template<typename Func>
void for_each_fetch_partition(const op_context& octx, const Func& f) {
    /**
     * Iterate over original request only if it is sessionless or initial
     * full fetch request. For not initial full fetch requests we may
     * leverage the fetch session stored partitions as session was populated
     * during initial pass. Using session stored partitions will account for
     * the partitions already read and move to the end of iteration order
     */
    auto& session_ctx = octx.session_ctx;

    if (
      session_ctx.is_sessionless()
      || (session_ctx.is_full_fetch() && octx.initial_fetch)) {
        for (auto& topic : octx.request.data.topics) {
            f(topic.topic,
              topic.partitions | std::views::transform([&](const auto& p) {
                  return fetch_session_partition{
                    topic.topic_id, topic.topic, p};
              }));
        }
    } else {
        auto& sparts = session_ctx.session()->partitions();
        // iterate over partitions, collecting all partitions with the same
        // topic and passing them to f together
        auto it = sparts.cbegin_insertion_order();
        auto end = sparts.cend_insertion_order();
        while (it != end) {
            // from the current position, find the range of partitions which
            // have the same topic, and pass that to the callback
            const auto& current = it->topic_partition;
            auto same_topic_end = std::find_if(
              it, end, [&](const kafka::fetch_session_partition& part) {
                  const auto no_id = model::topic_id{};
                  const auto& next = part.topic_partition;
                  if (
                    current.get_topic_id() == no_id
                    || next.get_topic_id() == no_id) {
                      return current.get_topic() != next.get_topic();
                  }
                  return next.get_topic_id() != current.get_topic_id();
              });
            f(current.get_topic(), std::ranges::subrange(it, same_topic_end));
            it = same_topic_end;
        }
    }
}
} // namespace

class simple_fetch_planner final : public fetch_planner::impl {
    fetch_plan create_plan(op_context& octx) final {
        fetch_plan plan(ss::smp::count);
        auto resp_it = octx.response_begin();
        auto bytes_left_in_plan = octx.bytes_left;

        plan.reserve_from_partition_count(octx.fetch_partition_count());

        const auto client_address = octx.rctx.connection()->local_address();

        /**
         * group fetch requests by shard
         */
        for_each_fetch_partition(
          octx,
          [&resp_it, &octx, &plan, &bytes_left_in_plan, &client_address](
            const model::topic& topic, const auto& partitions) {
              // First, we check all the failure conditions which depend only on
              // the topic, and not the partition.

              const bool over_min_bytes = octx.over_min_bytes();
              const auto& metadata_cache = octx.rctx.metadata_cache();
              model::topic_namespace_view tn_view{
                model::kafka_namespace, topic};

              auto fail_all_partitions = [&](error_code ec) {
                  for (const auto& fp : partitions) {
                      resp_it->set(
                        make_partition_response_error(
                          fp.topic_partition.get_partition(), ec),
                        {});
                      ++resp_it;
                  }
              };

              // An empty topic name means that lookup by id failed during
              // creation of the op_context.
              if (unlikely(topic().empty())) {
                  return fail_all_partitions(error_code::unknown_topic_id);
              }

              /**
               * If not authorized do not include into a plan.
               * We audit successful messages only on the initial fetch.
               */
              if (
                unlikely(!octx.rctx.authorized(
                  security::acl_operation::read,
                  topic,
                  audit_on_success{octx.initial_fetch}))) {
                  return fail_all_partitions(
                    error_code::topic_authorization_failed);
              }

              /**
               * in sanction mode (without an enterprise license), the audit
               * log topic is not consumable
               */
              if (
                unlikely(
                  topic == model::kafka_audit_logging_topic
                  && octx.rctx.feature_table().local().should_sanction())) {
                  thread_local static ss::logger::rate_limit rate(1s);
                  vloglr(
                    klog,
                    ss::log_level::warn,
                    rate,
                    "{}",
                    features::enterprise_error_message::audit_log_fetch());
                  return fail_all_partitions(error_code::unknown_server_error);
              }

              if (unlikely(metadata_cache.should_reject_reads(tn_view))) {
                  return fail_all_partitions(
                    error_code::invalid_topic_exception);
              }

              const auto& topic_md = metadata_cache.get_topic_metadata_ref(
                model::topic_namespace_view{model::kafka_namespace, topic});

              if (!topic_md) {
                  return fail_all_partitions(
                    error_code::unknown_topic_or_partition);
              }

              const auto& topic_cfg = topic_md->get().get_configuration();
              // Max batch size or `message.max.bytes` is a user configurable
              // topic property that defines the max size of a batch that can be
              // produced to any partition in the topic.
              //
              // It is used later in the fetch path to establish the minimum
              // number of memory semaphore units that need to be allocated
              // before reading a single batch from any of the topic's
              // partitions.
              const auto max_batch_size
                = topic_cfg.properties.batch_max_bytes.value_or(
                  metadata_cache.get_default_batch_max_bytes());

              for (const kafka::fetch_session_partition& fp : partitions) {
                  // if this is not an initial fetch we are allowed to skip
                  // partitions that already have an error or we have enough
                  // data
                  if (!octx.initial_fetch) {
                      if (
                        resp_it->has_error()
                        || (over_min_bytes && !resp_it->empty())) {
                          ++resp_it;
                          continue;
                      }
                  }

                  const auto& kitp = fp.topic_partition;
                  const auto partition_id = kitp.get_partition();
                  model::ktp_with_hash ktp{kitp.get_topic(), partition_id};

                  if (
                    unlikely(
                      metadata_cache.is_disabled(tn_view, partition_id))) {
                      resp_it->set(
                        make_partition_response_error(
                          partition_id, error_code::replica_not_available),
                        {});
                      ++resp_it;
                      continue;
                  }

                  auto shard = octx.rctx.shards().shard_for(ktp);
                  if (unlikely(!shard)) {
                      // there is given partition in topic metadata, return
                      // unknown_topic_or_partition error

                      /**
                       * no shard is found on current node, but topic exists in
                       * cluster metadata, this mean that the partition was
                       * moved but consumer has not updated its metadata yet. we
                       * return not_leader_for_partition error to force metadata
                       * update.
                       */
                      auto ec = metadata_cache.contains(kitp)
                                  ? error_code::not_leader_for_partition
                                  : error_code::unknown_topic_or_partition;
                      resp_it->set(
                        make_partition_response_error(partition_id, ec), {});
                      ++resp_it;
                      continue;
                  }

                  auto fetch_md = octx.rctx.get_fetch_metadata_cache().get(ktp);
                  auto max_bytes = std::min(
                    bytes_left_in_plan, size_t(fp.max_bytes));
                  auto avg_batch_size
                    = fetch_md && (fetch_md->avg_bytes_per_batch > 0)
                        ? fetch_md->avg_bytes_per_batch
                        : 1_MiB;
                  /**
                   * If the fetch offest is less than the hwm for the partition
                   * then we try to estimate the number of bytes that can be
                   * read via a moving average in the md cache. If there isn't
                   * enough information in the md cache to estimate this then we
                   * assume the `max_bytes` can be read.
                   */
                  if (fetch_md && fetch_md->high_watermark > fp.fetch_offset) {
                      const auto offset_count
                        = (fetch_md->high_watermark - fp.fetch_offset)() + 1;
                      const auto est_read_size
                        = offset_count * fetch_md->avg_bytes_per_offset;
                      if (est_read_size > 0) {
                          bytes_left_in_plan -= std::min(
                            est_read_size, max_bytes);
                      } else {
                          bytes_left_in_plan -= max_bytes;
                      }
                  }

                  plan.fetches_per_shard[*shard].push_back(
                    std::move(ktp),
                    fetch_config{
                      .start_offset = fp.fetch_offset,
                      .max_offset = model::model_limits<model::offset>::max(),
                      .max_bytes = max_bytes,
                      .max_batch_size = max_batch_size,
                      .avg_batch_size = avg_batch_size,
                      .timeout = octx.deadline.value_or(model::no_timeout),
                      .current_leader_epoch = fp.current_leader_epoch,
                      .isolation_level = octx.request.data.isolation_level,
                      .strict_max_bytes = true,
                      .skip_read = bytes_left_in_plan == 0 && max_bytes == 0,
                      .read_from_follower = octx.request.has_rack_id(),
                      .consumer_rack_id = octx.request.has_rack_id()
                                            ? std::make_optional(
                                                octx.request.data.rack_id)
                                            : std::nullopt,
                      .abort_source = octx.rctx.abort_source(),
                      .client_address = model::client_address_t{client_address},
                    },
                    &(*resp_it));
                  ++resp_it;
              }
          });
        return plan;
    }
};

namespace testing {
kafka::fetch_plan make_simple_fetch_plan(op_context& octx) {
    auto planner = make_fetch_planner<simple_fetch_planner>();
    return planner.create_plan(octx);
}
} // namespace testing

namespace {
ss::future<> do_fetch(op_context& octx) {
    auto planner = make_fetch_planner<simple_fetch_planner>();
    auto fetch_plan = planner.create_plan(octx);

    nonpolling_fetch_plan_executor executor;
    co_await executor.execute_plan(octx, std::move(fetch_plan));
}
} // namespace

namespace testing {
ss::future<> do_fetch(op_context& octx) { return ::kafka::do_fetch(octx); }
} // namespace testing

template<>
ss::future<response_ptr>
fetch_handler::handle(request_context rctx, ss::smp_service_group ssg) {
    return ss::do_with(
      std::make_unique<op_context>(std::move(rctx), ssg),
      [](std::unique_ptr<op_context>& octx_ptr) {
          auto& octx = *octx_ptr;
          log_request(octx.rctx.header(), octx.request);
          // top-level error is used for session-level errors
          if (octx.session_ctx.has_error()) {
              octx.response.data.error_code = octx.session_ctx.error();
              return std::move(octx).send_response();
          }
          if (unlikely(octx.rctx.recovery_mode_enabled())) {
              octx.response.data.error_code = error_code::policy_violation;
              return std::move(octx).send_response();
          }
          octx.response.data.error_code = error_code::none;
          return do_fetch(octx).then([&octx] {
              auto resp_units_deleter = octx.response_memory_units_deleter();

              // NOTE: Audit call doesn't happen until _after_ the fetch
              // is done. This was done for the sake of simplicity and
              // because fetch doesn't alter the state of the broker
              if (!octx.rctx.audit()) {
                  return std::move(octx).send_error_response(
                    error_code::broker_not_available);
              }

              octx.rctx.add_response_resource_deleter(
                std::move(resp_units_deleter));
              return std::move(octx).send_response();
          });
      });
}

void op_context::reset_context() { initial_fetch = false; }

// decode request and initialize budgets
op_context::op_context(request_context&& ctx, ss::smp_service_group ssg)
  : rctx(std::move(ctx))
  , ssg(ssg)
  , response_size(0)
  , response_error(false) {
    /*
     * decode request and prepare the inital response
     */
    request.decode(rctx.reader(), rctx.header().version);
    if (likely(!request.data.topics.empty())) {
        response.data.responses.reserve(request.data.topics.size());
    }

    if (auto delay = request.debounce_delay(); delay) {
        deadline = model::timeout_clock::now() + delay.value();
    }
    fetch_deadline
      = model::timeout_clock::now()
        + config::shard_local_cfg().kafka_fetch_request_timeout_ms();

    if (rctx.header().version() >= api_version{13}) {
        /*
         * Populate topic names
         *
         * Topic names that are not authorized must not be returned, but the
         * response does not serialize them, so this does not need to be undone.
         */
        const auto set_topic = [this](auto& t) {
            auto tp_ns = rctx.metadata_cache().get_name_by_id(t.topic_id);
            if (tp_ns.has_value()) {
                t.topic = std::move(tp_ns->tp);
            } else {
                // An empty topic name will be translated to unknown_topic_id
                // during plan creation
            }
        };
        std::ranges::for_each(request.data.topics, set_topic);
        std::ranges::for_each(request.data.forgotten_topics_data, set_topic);
    }

    /*
     * TODO: max size is multifaceted. it needs to be absolute, but also
     * integrate with other resource contraints that are dynamic within the
     * kafka server itself.
     */
    bytes_left = std::min(
      config::shard_local_cfg().fetch_max_bytes(),
      size_t(request.data.max_bytes));
    session_ctx = rctx.fetch_sessions().maybe_get_session(request);
    create_response_placeholders();
}

// insert and reserve space for a new topic in the response
void op_context::start_response_topic(const fetch_request::topic& topic) {
    response.data.responses.emplace_back(
      fetchable_topic_response{
        .topic = topic.topic, .topic_id = topic.topic_id});
}

void op_context::start_response_partition(const fetch_request::partition& p) {
    response.data.responses.back().partitions.push_back(
      fetch_response::partition_response{
        .partition_index = p.partition,
        .error_code = error_code::none,
        .high_watermark = model::offset(-1),
        .last_stable_offset = model::offset(-1),
        .records = batch_reader()});
}

void op_context::create_response_placeholders() {
    if (session_ctx.is_sessionless() || session_ctx.is_full_fetch()) {
        std::for_each(
          request.cbegin(),
          request.cend(),
          [this](const fetch_request::const_iterator::value_type& v) {
              if (v.new_topic) {
                  start_response_topic(*v.topic);
              }
              start_response_partition(*v.partition);
          });
    } else {
        std::optional<model::kitp_with_hash> last_topic;
        std::for_each(
          session_ctx.session()->partitions().cbegin_insertion_order(),
          session_ctx.session()->partitions().cend_insertion_order(),
          [this, &last_topic](const fetch_session_partition& fp) {
              auto& kitp = fp.topic_partition;
              if (last_topic != kitp) {
                  response.data.responses.emplace_back(
                    fetchable_topic_response{
                      .topic = kitp.get_topic(),
                      .topic_id = kitp.get_topic_id()});
                  last_topic = kitp;
              }
              fetch_response::partition_response p{
                .partition_index = fp.topic_partition.get_partition(),
                .error_code = error_code::none,
                .high_watermark = fp.high_watermark,
                .last_stable_offset = fp.last_stable_offset,
                .records = batch_reader()};

              response.data.responses.back().partitions.push_back(std::move(p));
          });
    }
    for (auto it = response.begin(); it != response.end(); ++it) {
        auto raw = new response_placeholder(it, this); // NOLINT
        iteration_order.push_back(*raw);
    }
}

// Determines if a partition should be included in an incremental fetch
// response per KIP-227.
bool partition_has_changes(
  const fetch_response::partition_response& resp,
  const fetch_session_partition& session_partition) {
    if (resp.records && resp.records->size_bytes() > 0) {
        return true;
    }
    if (session_partition.high_watermark != resp.high_watermark) {
        return true;
    }
    if (session_partition.last_stable_offset != resp.last_stable_offset) {
        return true;
    }
    if (session_partition.start_offset != resp.log_start_offset) {
        return true;
    }
    /**
     * Always include partition in a response if it contains information about
     * the preferred replica
     */
    if (resp.preferred_read_replica != -1) {
        return true;
    }
    if (resp.error_code != error_code::none) {
        // Partitions with errors are always included in the response.
        return true;
    }
    return false;
}

// Updates the fetch session's partition with the response. Called in
// send_response() when committing the response, not during fetch iteration (to
// avoid premature updates on retries).
void update_session_partition(
  const fetch_response::partition_response& resp,
  fetch_session_partition& session_partition) {
    session_partition.high_watermark = model::offset(resp.high_watermark);
    session_partition.last_stable_offset = model::offset(
      resp.last_stable_offset);
    session_partition.start_offset = model::offset(resp.log_start_offset);
    if (resp.error_code != error_code::none) {
        // Set high_watermark to -1 so we re-send this partition once the error
        // clears.
        session_partition.high_watermark = model::offset{-1};
    }
}

ss::future<response_ptr> op_context::send_response() && {
    auto fetched_data_size = uint64_t{0};
    for (const auto& topic : response.data.responses) {
        const bool bytes_to_exclude = std::find(
                                        usage_excluded_topics.cbegin(),
                                        usage_excluded_topics.cend(),
                                        topic.topic)
                                      != usage_excluded_topics.cend();

        for (const auto& part : topic.partitions) {
            if (part.records) {
                auto part_size = part.records->size_bytes();
                fetched_data_size += part_size;

                /// Account for special internal topic bytes for usage
                if (bytes_to_exclude) {
                    response.internal_topic_bytes += part_size;
                }
            }
        }
    }

    rctx.connection()->attributes().fetch_bytes.record(fetched_data_size);

    // Sessionless fetch
    if (session_ctx.is_sessionless()) {
        response.data.session_id = invalid_fetch_session_id;
        return rctx.respond(std::move(response));
    }
    // bellow we handle incremental fetches, set response session id
    response.data.session_id = session_ctx.session()->id();

    auto& session_partitions = session_ctx.session()->partitions();
    auto update_session = [&session_partitions](const auto& resp_it) {
        auto key = model::kitp_view(
          resp_it->partition->topic_id,
          resp_it->partition->topic,
          resp_it->partition_response->partition_index);
        if (
          auto sp_it = session_partitions.find(key);
          sp_it != session_partitions.end()) {
            update_session_partition(
              *resp_it->partition_response, sp_it->second->partition);
        }
    };

    if (session_ctx.is_full_fetch()) {
        for (auto it = response.begin(false); it != response.end(); ++it) {
            update_session(it);
        }
        return rctx.respond(std::move(response));
    }

    fetch_response final_response;
    final_response.data.error_code = response.data.error_code;
    final_response.data.session_id = response.data.session_id;
    final_response.internal_topic_bytes = response.internal_topic_bytes;

    for (auto it = response.begin(true); it != response.end(); ++it) {
        update_session(it);

        if (it->is_new_topic) {
            final_response.data.responses.emplace_back(
              fetchable_topic_response{
                .topic = it->partition->topic,
                .topic_id = it->partition->topic_id});
        }

        fetch_response::partition_response r{
          .partition_index = it->partition_response->partition_index,
          .error_code = it->partition_response->error_code,
          .high_watermark = it->partition_response->high_watermark,
          .last_stable_offset = it->partition_response->last_stable_offset,
          .log_start_offset = it->partition_response->log_start_offset,
          .aborted_transactions = std::move(
            it->partition_response->aborted_transactions),
          .preferred_read_replica
          = it->partition_response->preferred_read_replica,
          .records = std::move(it->partition_response->records)};

        final_response.data.responses.back().partitions.push_back(std::move(r));
    }

    return rctx.respond(std::move(final_response));
}

ss::future<response_ptr> op_context::send_error_response(error_code ec) && {
    fetch_response resp;
    resp.data.error_code = ec;

    if (session_ctx.is_sessionless()) {
        resp.data.session_id = invalid_fetch_session_id;
    } else {
        resp.data.session_id = session_ctx.session()->id();
    }

    return rctx.respond(std::move(resp));
}

ss::deleter op_context::response_memory_units_deleter() {
    chunked_vector<fetch_memory_units> mu;
    for (auto& r : iteration_order) {
        if (r.has_memory_units()) {
            mu.push_back(r.release_memory_units().value());
        }
    }
    return ss::make_object_deleter(std::move(mu));
}

size_t op_context::total_response_memory_units() const {
    size_t res = 0;
    for (const auto& r : iteration_order) {
        res += r.num_memory_units();
    }
    return res;
}

op_context::response_placeholder::response_placeholder(
  fetch_response::iterator it, op_context* ctx)
  : _it(it)
  , _ctx(ctx)
  , _ktp(_it->partition->topic, _it->partition_response->partition_index) {}

void op_context::response_placeholder::set(
  fetch_response::partition_response&& response,
  std::optional<fetch_memory_units>&& response_memory_units) {
    vassert(
      response.partition_index == _it->partition_response->partition_index,
      "Response and current partition ids have to be the same. Current "
      "response {}, update {}",
      _it->partition_response->partition_index,
      response.partition_index);
    dassert(
      (response.records ? response.records->size_bytes() : 0)
        == (response_memory_units ? response_memory_units->num_units() : 0),
      "Response units should equal the number of bytes in the response its "
      "self.");

    replace_or_add_memory_units(std::move(response_memory_units));

    if (response.error_code != error_code::none) {
        _ctx->response_error = true;
    }

    if (response.preferred_read_replica != -1) {
        _ctx->contains_preferred_replica = true;
    }

    auto& current_resp_data = _it->partition_response->records;
    if (current_resp_data) {
        auto sz = current_resp_data->size_bytes();
        _ctx->response_size -= sz;
        _ctx->bytes_left += sz;
    }

    if (response.records) {
        auto sz = response.records->size_bytes();
        _ctx->response_size += sz;
        _ctx->bytes_left -= std::min(_ctx->bytes_left, sz);
    }
    *_it->partition_response = std::move(response);

    // if we are not sessionless update session cache
    if (!_ctx->session_ctx.is_sessionless()) {
        auto& session_partitions = _ctx->session_ctx.session()->partitions();
        auto key = model::kitp_view(
          _it->partition->topic_id,
          _it->partition->topic,
          _it->partition_response->partition_index);

        if (
          auto it = session_partitions.find(key);
          it != session_partitions.end()) {
            auto has_to_be_included = partition_has_changes(
              *_it->partition_response, it->second->partition);
            /**
             * From KIP-227
             *
             * In order to solve the starvation problem, the server must
             * rotate the order in which it returns partition information.
             * The server does this by maintaining a linked list of all
             * partitions in the fetch session.  When data is returned for a
             * partition, that partition is moved to the end of the list.
             * This ensures that we eventually return data about all
             * partitions for which data is available.
             *
             */
            if (
              _it->partition_response->records
              && _it->partition_response->records->size_bytes() > 0) {
                // move both session partition and response placeholder to the
                // end of fetch queue
                session_partitions.move_to_end(it);
                move_to_end();
            }
            _it->partition_response->has_to_be_included = has_to_be_included;
        }
    }
}

rack_aware_replica_selector::rack_aware_replica_selector(
  const cluster::metadata_cache& md_cache)
  : _md_cache(md_cache) {}

std::optional<model::node_id> rack_aware_replica_selector::select_replica(
  const consumer_info& c_info, const partition_info& p_info) const {
    if (!c_info.rack_id.has_value()) {
        return select_leader_replica{}.select_replica(c_info, p_info);
    }
    if (p_info.replicas.empty()) {
        return std::nullopt;
    }

    std::vector<replica_info> rack_replicas;
    model::offset highest_hw;
    for (auto& replica : p_info.replicas) {
        // filter out replicas which are not responsive
        if (!replica.is_alive) {
            continue;
        }

        const auto node_it = _md_cache.nodes().find(replica.id);
        /**
         * Skip nodes which are in maintenance mode or we do not have
         * information about them
         */
        if (
          node_it == _md_cache.nodes().end()
          || node_it->second.state.get_maintenance_state()
               == model::maintenance_state::active) {
            continue;
        }

        if (
          node_it->second.broker.rack() == c_info.rack_id
          && replica.log_end_offset >= c_info.fetch_offset) {
            /**
             * Select replica with highest high watermark in requested rack. If
             * there is more than one use random choice to break the tie.
             */
            if (replica.high_watermark >= highest_hw) {
                if (replica.high_watermark > highest_hw) {
                    highest_hw = replica.high_watermark;
                    rack_replicas.clear();
                }
                rack_replicas.push_back(replica);
            }
        }
    }

    if (rack_replicas.empty()) {
        return std::nullopt;
    }
    // if there are multiple replicas with the same high watermark in
    // requested rack, return random one
    return random_generators::random_choice(rack_replicas).id;
}

std::optional<ss::scheduling_group>
fetch_scheduling_group_provider(const connection_context& conn_ctx) {
    return conn_ctx.server().fetch_scheduling_group();
}

std::ostream& operator<<(std::ostream& o, const consumer_info& ci) {
    fmt::print(o, "rack_id: {}, fetch_offset: {}", ci.rack_id, ci.fetch_offset);
    return o;
}
} // namespace kafka
