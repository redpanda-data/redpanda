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
#include "cluster/metadata_cache.h"
#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "config/configuration.h"
#include "container/fragmented_vector.h"
#include "kafka/latency_probe.h"
#include "kafka/protocol/batch_consumer.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/fetch.h"
#include "kafka/read_distribution_probe.h"
#include "kafka/server/fetch_session.h"
#include "kafka/server/fwd.h"
#include "kafka/server/handlers/details/leader_epoch.h"
#include "kafka/server/handlers/fetch/fetch_plan_executor.h"
#include "kafka/server/handlers/fetch/fetch_planner.h"
#include "kafka/server/handlers/fetch/replica_selector.h"
#include "kafka/server/partition_proxy.h"
#include "kafka/server/replicated_partition.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/record_utils.h"
#include "model/timeout_clock.h"
#include "net/connection.h"
#include "random/generators.h"
#include "resource_mgmt/io_priority.h"
#include "ssx/semaphore.h"
#include "storage/parser_utils.h"
#include "utils/to_string.h"

#include <seastar/core/do_with.hh>
#include <seastar/core/future.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/util/log.hh>

#include <boost/range/irange.hpp>
#include <fmt/ostream.h>

#include <chrono>
#include <exception>
#include <ranges>
#include <string_view>

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
      .records = batch_reader(),
    };
}

/**
 * Low-level handler for reading from an ntp. Runs on ntp's home core.
 */
static ss::future<read_result> read_from_partition(
  kafka::partition_proxy part,
  fetch_config config,
  bool foreign_read,
  std::optional<model::timeout_clock::time_point> deadline) {
    auto lso = part.last_stable_offset();
    if (unlikely(!lso)) {
        co_return read_result(lso.error());
    }
    auto hw = part.high_watermark();
    auto start_o = part.start_offset();
    // if we have no data read, return fast
    if (
      hw < config.start_offset || config.skip_read
      || config.start_offset > config.max_offset) {
        co_return read_result(start_o, hw, lso.value());
    }

    storage::log_reader_config reader_config(
      config.start_offset,
      config.max_offset,
      0,
      config.max_bytes,
      kafka_read_priority(),
      std::nullopt,
      std::nullopt,
      config.abort_source.has_value()
        ? config.abort_source.value().get().local()
        : storage::opt_abort_source_t{},
      config.client_address);

    reader_config.strict_max_bytes = config.strict_max_bytes;
    auto rdr = co_await part.make_reader(reader_config);
    std::exception_ptr e;
    std::unique_ptr<iobuf> data;
    std::vector<cluster::tx::tx_range> aborted_transactions;
    std::optional<std::chrono::milliseconds> delta_from_tip_ms;
    try {
        auto result = co_await rdr.reader.consume(
          kafka_batch_serializer(), deadline ? *deadline : model::no_timeout);
        data = std::make_unique<iobuf>(std::move(result.data));
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
                  part.high_watermark());
            }
        }

    } catch (...) {
        e = std::current_exception();
    }

    co_await std::move(rdr.reader).release()->finally();

    if (e) {
        std::rethrow_exception(e);
    }

    if (foreign_read) {
        co_return read_result(
          ss::make_foreign<read_result::data_t>(std::move(data)),
          start_o,
          hw,
          lso.value(),
          delta_from_tip_ms,
          std::move(aborted_transactions));
    }

    co_return read_result(
      std::move(data),
      start_o,
      hw,
      lso.value(),
      delta_from_tip_ms,
      std::move(aborted_transactions));
}

read_result::memory_units_t::memory_units_t(
  ssx::semaphore& memory_sem, ssx::semaphore& memory_fetch_sem) noexcept
  : kafka(ss::consume_units(memory_sem, 0))
  , fetch(ss::consume_units(memory_fetch_sem, 0)) {}

read_result::memory_units_t::~memory_units_t() noexcept {
    if (shard == ss::this_shard_id() || !has_units()) {
        return;
    }
    auto f = ss::smp::submit_to(
      shard, [uk = std::move(kafka), uf = std::move(fetch)]() mutable noexcept {
          uk.return_all();
          uf.return_all();
      });
    if (!f.available()) {
        ss::engine().run_in_background(std::move(f));
    }
}

void read_result::memory_units_t::adopt(memory_units_t&& o) {
    // Adopts assert internally that the units are from the same semaphore.
    // So there is no need to assert that they are from the same shard here.
    kafka.adopt(std::move(o.kafka));
    fetch.adopt(std::move(o.fetch));
}

/**
 * Consume proper amounts of units from memory semaphores and return them as
 * semaphore_units. Fetch semaphore units returned are the indication of
 * available resources: if none, there is no memory for the operation;
 * if less than \p max_bytes, the fetch should be capped to that size.
 *
 * \param max_bytes The limit of how much data is going to be fetched
 * \param obligatory_batch_read Set to true for the first ntp in the fetch
 *   fetch request, at least one batch must be fetched for that ntp. Also it
 *   is assumed that a batch size has already been consumed from kafka
 *   memory semaphore for it.
 */
static read_result::memory_units_t reserve_memory_units(
  ssx::semaphore& memory_sem,
  ssx::semaphore& memory_fetch_sem,
  const size_t max_bytes,
  const bool obligatory_batch_read) {
    read_result::memory_units_t memory_units;
    const size_t memory_kafka_now = memory_sem.current();
    const size_t memory_fetch = memory_fetch_sem.current();
    const size_t batch_size_estimate
      = config::shard_local_cfg().kafka_memory_batch_size_estimate_for_fetch();

    if (obligatory_batch_read) {
        // cap what we want at what we have, but no further down than a single
        // batch size - with \ref obligatory_batch_read, it must be fetched
        // regardless
        const size_t fetch_size = std::max(
          batch_size_estimate,
          std::min({max_bytes, memory_kafka_now, memory_fetch}));
        memory_units.fetch = ss::consume_units(memory_fetch_sem, fetch_size);
        memory_units.kafka = ss::consume_units(memory_sem, fetch_size);
    } else {
        // max_bytes is how much we prepare to read from this ntp, but no less
        // than one full batch
        const size_t requested_fetch_size = std::max(
          max_bytes, batch_size_estimate);
        // cap what we want at what we have
        const size_t fetch_size = std::min(
          {requested_fetch_size, memory_kafka_now, memory_fetch});
        // only reserve memory if we have space for at least one batch,
        // otherwise this ntp will be skipped
        if (fetch_size >= batch_size_estimate) {
            memory_units.fetch = ss::consume_units(
              memory_fetch_sem, fetch_size);
            memory_units.kafka = ss::consume_units(memory_sem, fetch_size);
        }
    }

    return memory_units;
}

/**
 * Make the \p units hold exactly \p target_bytes, by consuming more units
 * from \p sem or by returning extra units back.
 */
static void adjust_semaphore_units(
  ssx::semaphore& sem, ssx::semaphore_units& units, const size_t target_bytes) {
    if (target_bytes < units.count()) {
        units.return_units(units.count() - target_bytes);
    }
    if (target_bytes > units.count()) {
        units.adopt(ss::consume_units(sem, target_bytes - units.count()));
    }
}

/**
 * Memory units have been reserved before the read op based on an estimation.
 * Now when we know how much data has actually been read, return any extra
 * amount.
 */
static void adjust_memory_units(
  ssx::semaphore& memory_sem,
  ssx::semaphore& memory_fetch_sem,
  read_result::memory_units_t& memory_units,
  const size_t read_bytes) {
    adjust_semaphore_units(memory_sem, memory_units.kafka, read_bytes);
    adjust_semaphore_units(memory_fetch_sem, memory_units.fetch, read_bytes);
}

/**
 * Entry point for reading from an ntp. This is executed on NTP home core and
 * build error responses if anything goes wrong.
 */
static ss::future<read_result> do_read_from_ntp(
  cluster::partition_manager& cluster_pm,
  const replica_selector& replica_selector,
  const ntp_fetch_config& ntp_config,
  bool foreign_read,
  std::optional<model::timeout_clock::time_point> deadline,
  const bool obligatory_batch_read,
  ssx::semaphore& memory_sem,
  ssx::semaphore& memory_fetch_sem) {
    auto cfg = ntp_config.cfg;

    // control available memory
    read_result::memory_units_t memory_units(memory_sem, memory_fetch_sem);
    if (!cfg.skip_read) {
        memory_units = reserve_memory_units(
          memory_sem, memory_fetch_sem, cfg.max_bytes, obligatory_batch_read);
        if (!memory_units.fetch) {
            cfg.skip_read = true;
        } else if (cfg.max_bytes > memory_units.fetch.count()) {
            cfg.max_bytes = memory_units.fetch.count();
        }
    }

    /*
     * lookup the ntp's partition
     */
    auto kafka_partition = make_partition_proxy(ntp_config.ktp(), cluster_pm);
    if (unlikely(!kafka_partition)) {
        co_return read_result(error_code::unknown_topic_or_partition);
    }
    if (!cfg.read_from_follower && !kafka_partition->is_leader()) {
        co_return read_result(error_code::not_leader_for_partition);
    }

    /**
     * validate leader epoch. for more details see KIP-320
     */
    auto leader_epoch_err = details::check_leader_epoch(
      cfg.current_leader_epoch, *kafka_partition);
    if (leader_epoch_err != error_code::none) {
        co_return read_result(leader_epoch_err);
    }
    auto offset_ec = co_await kafka_partition->validate_fetch_offset(
      cfg.start_offset,
      cfg.read_from_follower,
      default_fetch_timeout + model::timeout_clock::now());

    if (config::shard_local_cfg().enable_transactions.value()) {
        if (cfg.isolation_level == model::isolation_level::read_committed) {
            auto maybe_lso = kafka_partition->last_stable_offset();
            if (unlikely(!maybe_lso)) {
                // partition is still bootstrapping
                co_return read_result(maybe_lso.error());
            }
            cfg.max_offset = model::prev_offset(maybe_lso.value());
        }
    }

    if (offset_ec != error_code::none) {
        co_return read_result(
          offset_ec,
          kafka_partition->start_offset(),
          kafka_partition->high_watermark());
    }
    if (
      config::shard_local_cfg().enable_rack_awareness.value()
      && cfg.consumer_rack_id && kafka_partition->is_leader()) {
        auto p_info_res = kafka_partition->get_partition_info();
        if (p_info_res.has_error()) {
            // TODO: add mapping here
            co_return read_result(error_code::not_leader_for_partition);
        }
        auto p_info = std::move(p_info_res.value());

        auto lso = kafka_partition->last_stable_offset();
        if (unlikely(!lso)) {
            co_return read_result(lso.error());
        }
        auto preferred_replica = replica_selector.select_replica(
          consumer_info{
            .fetch_offset = cfg.start_offset, .rack_id = cfg.consumer_rack_id},
          p_info);
        if (preferred_replica && preferred_replica.value() != p_info.leader) {
            vlog(
              klog.trace,
              "Consumer in rack: {}, preferred replica id: {}",
              *cfg.consumer_rack_id,
              preferred_replica.value());
            co_return read_result(
              kafka_partition->start_offset(),
              kafka_partition->high_watermark(),
              lso.value(),
              preferred_replica);
        }
    }
    read_result result = co_await read_from_partition(
      std::move(*kafka_partition), std::move(cfg), foreign_read, deadline);

    adjust_memory_units(
      memory_sem, memory_fetch_sem, memory_units, result.data_size_bytes());
    result.memory_units = std::move(memory_units);
    co_return result;
}

namespace testing {

ss::future<read_result> read_from_ntp(
  cluster::partition_manager& cluster_pm,
  const replica_selector& replica_selector,
  const model::ktp& ktp,
  fetch_config config,
  bool foreign_read,
  std::optional<model::timeout_clock::time_point> deadline,
  const bool obligatory_batch_read,
  ssx::semaphore& memory_sem,
  ssx::semaphore& memory_fetch_sem) {
    return ss::do_with(
      ntp_fetch_config{ktp, std::move(config)}, [&](auto& ntp_config) {
          return do_read_from_ntp(
            cluster_pm,
            replica_selector,
            ntp_config,
            foreign_read,
            deadline,
            obligatory_batch_read,
            memory_sem,
            memory_fetch_sem);
      });
}

read_result::memory_units_t reserve_memory_units(
  ssx::semaphore& memory_sem,
  ssx::semaphore& memory_fetch_sem,
  const size_t max_bytes,
  const bool obligatory_batch_read) {
    return kafka::reserve_memory_units(
      memory_sem, memory_fetch_sem, max_bytes, obligatory_batch_read);
}

} // namespace testing

static void fill_fetch_responses(
  op_context& octx,
  std::vector<read_result> results,
  const std::vector<op_context::response_placeholder_ptr>& responses,
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

    // Used to aggregate semaphore_units from results.
    std::optional<read_result::memory_units_t> total_memory_units;

    for (auto idx : range) {
        auto& res = results[idx];
        const auto& resp_it = responses[idx];

        // error case
        if (unlikely(res.error != error_code::none)) {
            resp_it->set(
              make_partition_response_error(res.partition, res.error));
            continue;
        }

        /**
         * Cache fetch metadata
         */
        octx.rctx.get_fetch_metadata_cache().insert_or_assign(
          {resp_it->topic(), resp_it->partition_id()},
          res.start_offset,
          res.high_watermark,
          res.last_stable_offset);
        /**
         * Over response budget, we will just waste this read, it will cause
         * data to be stored in the cache so next read is fast
         */
        fetch_response::partition_response resp;
        resp.partition_index = res.partition;
        resp.error_code = error_code::none;
        resp.log_start_offset = res.start_offset;
        resp.high_watermark = res.high_watermark;
        resp.last_stable_offset = res.last_stable_offset;
        if (res.preferred_replica) {
            resp.preferred_read_replica = *res.preferred_replica;
        }

        // Aggregate memory_units from all results together to avoid
        // making more than one cross-shard function call to free them.
        //
        // Only aggregate non-empty memory_units.
        if (res.memory_units.has_units()) {
            if (unlikely(!total_memory_units)) {
                // Move the first set of semaphore_units to get a copy of the
                // semaphore pointer and the shard results originates from.
                total_memory_units = std::move(res.memory_units);
            } else {
                total_memory_units->adopt(std::move(res.memory_units));
            }
        }

        /**
         * According to KIP-74 we have to return first batch even if it would
         * violate max_bytes fetch parameter
         */
        if (
          res.has_data()
          && (octx.bytes_left >= res.data_size_bytes() || octx.response_size == 0)) {
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
                resp.aborted = std::move(aborted);
            }
            resp.records = batch_reader(std::move(res).release_data());
        } else {
            // TODO: add probe to measure how much of read data is discarded
            resp.records = batch_reader();
        }

        resp_it->set(std::move(resp));

        if (record_latency) {
            std::chrono::microseconds fetch_latency
              = std::chrono::duration_cast<std::chrono::microseconds>(
                op_context::latency_clock::now() - start_time);
            octx.rctx.probe().record_fetch_latency(fetch_latency);
        }
    }
}

template<typename r, typename v>
concept range_of = std::ranges::range<r>
                   && std::same_as<std::ranges::range_value_t<r>, v>;

static ss::future<std::vector<read_result>> fetch_ntps_in_parallel(
  cluster::partition_manager& cluster_pm,
  const replica_selector& replica_selector,
  range_of<ntp_fetch_config> auto& ntp_fetch_configs,
  read_distribution_probe& read_probe,
  bool foreign_read,
  std::optional<model::timeout_clock::time_point> deadline,
  const size_t bytes_left,
  ssx::semaphore& memory_sem,
  ssx::semaphore& memory_fetch_sem) {
    size_t total_max_bytes = 0;
    for (const auto& c : ntp_fetch_configs) {
        total_max_bytes += c.cfg.max_bytes;
    }

    // bytes_left comes from the fetch plan and also accounts for the max_bytes
    // field in the fetch request
    const size_t max_bytes_per_fetch = std::min<size_t>(
      config::shard_local_cfg().kafka_max_bytes_per_fetch(), bytes_left);
    if (total_max_bytes > max_bytes_per_fetch) {
        auto per_partition = max_bytes_per_fetch / ntp_fetch_configs.size();
        vlog(
          klog.debug,
          "Fetch requested very large response ({}), clamping each partition's "
          "max_bytes to {} bytes",
          total_max_bytes,
          per_partition);

        for (auto& c : ntp_fetch_configs) {
            c.cfg.max_bytes = per_partition;
        }
    }

    const auto first_p_id = ntp_fetch_configs.front().ktp().get_partition();
    auto results = co_await ssx::unsafe_parallel_transform(
      ntp_fetch_configs.begin(),
      ntp_fetch_configs.end(),
      [&cluster_pm,
       &replica_selector,
       deadline,
       foreign_read,
       first_p_id,
       &memory_sem,
       &memory_fetch_sem](const ntp_fetch_config& ntp_cfg) {
          auto p_id = ntp_cfg.ktp().get_partition();
          return do_read_from_ntp(
                   cluster_pm,
                   replica_selector,
                   ntp_cfg,
                   foreign_read,
                   deadline,
                   first_p_id == p_id,
                   memory_sem,
                   memory_fetch_sem)
            .then([p_id](read_result res) {
                res.partition = p_id;
                return res;
            });
      });

    size_t total_size = 0;
    for (const auto& r : results) {
        total_size += r.data_size_bytes();
        if (r.delta_from_tip_ms.has_value()) {
            read_probe.add_read_event_delta_from_tip(
              r.delta_from_tip_ms.value());
        }
    }
    vlog(
      klog.trace,
      "fetch_ntps_in_parallel: for {} partitions returning {} total bytes",
      results.size(),
      total_size);
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

/**
 * Top-level handler for fetching from single shard. The result is
 * unwrapped and any errors from the storage sub-system are translated
 * into kafka specific response codes. On failure or success the
 * partition response is finalized and placed into its position in the
 * response message.
 */
static ss::future<>
handle_shard_fetch(ss::shard_id shard, op_context& octx, shard_fetch fetch) {
    // if over budget skip the fetch.
    if (octx.bytes_left <= 0) {
        return ss::now();
    }
    // no requests for this shard, do nothing
    if (fetch.empty()) {
        return ss::now();
    }

    const bool foreign_read = shard != ss::this_shard_id();

    // dispatch to remote core
    return octx.rctx.partition_manager()
      .invoke_on(
        shard,
        octx.ssg,
        [foreign_read, configs = std::move(fetch.requests), &octx](
          cluster::partition_manager& mgr) mutable {
            // &octx is captured only to immediately use its accessors here so
            // that there is a list of all objects accessed next to `invoke_on`.
            // This is meant to help avoiding unintended cross shard access
            return ss::do_with(
              std::move(configs), [foreign_read, &octx, &mgr](auto& configs) {
                  return fetch_ntps_in_parallel(
                    mgr,
                    octx.rctx.server().local().get_replica_selector(),
                    configs,
                    octx.rctx.server().local().read_probe(),
                    foreign_read,
                    octx.deadline,
                    octx.bytes_left,
                    octx.rctx.server().local().memory(),
                    octx.rctx.server().local().memory_fetch_sem());
              });
        })
      .then([responses = std::move(fetch.responses),
             start_time = fetch.start_time,
             &octx](std::vector<read_result> results) mutable {
          fill_fetch_responses(octx, std::move(results), responses, start_time);
      });
}

class parallel_fetch_plan_executor final : public fetch_plan_executor::impl {
    ss::future<> execute_plan(op_context& octx, fetch_plan plan) final {
        std::vector<ss::future<>> fetches;
        fetches.reserve(ss::smp::count);

        // start fetching from random shard to make sure that we fetch data from
        // all the partition even if we reach fetch message size limit
        const ss::shard_id start_shard_idx = random_generators::get_int(
          ss::smp::count - 1);
        for (size_t i = 0; i < ss::smp::count; ++i) {
            auto shard = (start_shard_idx + i) % ss::smp::count;

            fetches.push_back(handle_shard_fetch(
              shard, octx, std::move(plan.fetches_per_shard[shard])));
        }

        return ss::when_all_succeed(fetches.begin(), fetches.end());
    }
};

class fetch_worker {
public:
    // Passed from the coordinator shard to fetch workers.
    // Contains either references to objects local to the fetch worker shard or
    // copies of data from the coordinator shard.
    struct shard_local_fetch_context {
        // True if the results of this fetch will be read on a foreign shard.
        bool foreign_read;
        size_t bytes_left;
        // Specifies the minimum number of bytes this sub-fetch should read
        // before returning.
        size_t min_bytes;
        // If set then the sub-fetch should return by the specified time_point .
        std::optional<model::timeout_clock::time_point> deadline;
        // The fetch sub-requests of partitions local to the shard this worker
        // is running on.
        std::vector<ntp_fetch_config>& requests;

        // References to services local to the shard this worker is running on.
        // They are protected from deletion by the coordinator.
        server& srv;
        cluster::partition_manager& mgr;
        ss::abort_source& as;
    };

    explicit fetch_worker(shard_local_fetch_context ctx)
      : _ctx(std::move(ctx)) {}

    struct worker_result {
        std::vector<read_result> read_results;
        // The total amount of bytes read across all results in `read_results`.
        size_t total_size;
        // The time it took for the first `fetch_ntps_in_parallel` to complete
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
        std::vector<read_result> results;
        size_t total_size;
    };

    ss::future<query_results>
    query_requests(const range_of<size_t> auto& request_indexes) {
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
        std::vector<model::offset> last_visible_indexes(request_indexes.size());
        std::vector<std::tuple<size_t, model::partition_id>> errored_partitions;
        size_t total_size{0};
        bool has_error{false};

        for (size_t i = 0; i < request_indexes.size(); i++) {
            const auto& req = _ctx.requests[request_indexes[i]];
            auto part = _ctx.mgr.get(req.ktp());
            if (!part) {
                errored_partitions.emplace_back(i, req.ktp().get_partition());
                continue;
            }
            auto consensus = part->raft();
            if (!consensus) {
                errored_partitions.emplace_back(i, req.ktp().get_partition());
                continue;
            }
            last_visible_indexes[i] = consensus->last_visible_index();
        }

        auto requests = request_indexes
                        | std::views::transform(
                          [this](size_t i) -> ntp_fetch_config& {
                              return _ctx.requests[i];
                          });

        // A read_result needs to be returned for every partition. Hence,
        // the function can't return before calling
        // `fetch_ntps_in_parallel`.
        std::vector<read_result> results = co_await fetch_ntps_in_parallel(
          _ctx.mgr,
          _ctx.srv.get_replica_selector(),
          requests,
          _ctx.srv.read_probe(),
          _ctx.foreign_read,
          _ctx.deadline,
          _ctx.bytes_left,
          _ctx.srv.memory(),
          _ctx.srv.memory_fetch_sem());

        // If we weren't able to read the last_visible_index for a partition
        // before calling `fetch_ntps_in_parallel` then we need to
        // return with an error for that partition.
        for (auto [i, partition] : errored_partitions) {
            results[i] = read_result(error_code::not_leader_for_partition);
            results[i].partition = partition;
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
    std::optional<size_t>
    register_waiters(const range_of<size_t> auto& request_indexes) {
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
        std::vector<size_t> request_indexes;

        std::vector<read_result> results;
        size_t total_size{0};

        for (;;) {
            if (!first_run) {
                request_indexes = std::move(_request_indexes);
                _request_indexes.clear();

                // All `_request_indexes` have been read. Reset counter
                _completed_waiter_count.consume(
                  _completed_waiter_count.current());
            }

            query_results q_results;
            if (first_run) {
                auto start_time = op_context::latency_clock::now();

                q_results = co_await query_requests(
                  std::ranges::iota_view{0ul, _ctx.requests.size()});

                results = std::move(q_results.results);
                total_size = q_results.total_size;

                _last_visible_indexes = std::move(
                  q_results.last_visible_indexes);
                first_run_latency_result
                  = std::chrono::duration_cast<std::chrono::microseconds>(
                    op_context::latency_clock::now() - start_time);
            } else {
                q_results = co_await query_requests(request_indexes);

                // Override the older results of the partitions with the newly
                // queried results.
                for (size_t i = 0; i < request_indexes.size(); i++) {
                    auto r_i = request_indexes[i];
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
                  std::ranges::iota_view{0ul, _ctx.requests.size()});
            } else {
                has_errored_request = register_waiters(request_indexes);
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
    explicit nonpolling_fetch_plan_executor(bool debounce = false)
      : _last_result_size(ss::smp::count, 0)
      , _debounce(debounce)
      , _fetch_timeout{[this] { _has_progress.signal(); }} {}

    /**
     * Executes the supplied `plan` until `octx.should_stop_fetch` returns true.
     */
    ss::future<> execute_plan(op_context& octx, fetch_plan plan) final {
        if (_debounce) {
            co_await ss::sleep(std::min(
              config::shard_local_cfg().fetch_reads_debounce_timeout(),
              octx.request.data.max_wait_ms));
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

        const bool foreign_read = fetch.shard != ss::this_shard_id();

        fetch_worker::worker_result results
          = co_await octx.rctx.partition_manager().invoke_on(
            fetch.shard,
            [this,
             shard = fetch.shard,
             min_fetch_bytes,
             foreign_read,
             &configs = fetch.requests,
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
                  fetch_worker(fetch_worker::shard_local_fetch_context{
                    .foreign_read = foreign_read,
                    .bytes_left = octx.bytes_left,
                    .min_bytes = min_fetch_bytes,
                    .deadline = octx.deadline,
                    .requests = configs,
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
    bool _debounce;
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

template<typename Func>
void op_context::for_each_fetch_partition(Func&& f) const {
    /**
     * Iterate over original request only if it is sessionless or initial
     * full fetch request. For not initial full fetch requests we may
     * leverage the fetch session stored partitions as session was populated
     * during initial pass. Using session stored partitions will account for
     * the partitions already read and move to the end of iteration order
     */
    if (
      session_ctx.is_sessionless()
      || (session_ctx.is_full_fetch() && initial_fetch)) {
        std::for_each(
          request.cbegin(),
          request.cend(),
          [f = std::forward<Func>(f)](
            const fetch_request::const_iterator::value_type& p) {
              f(fetch_session_partition(p.topic->name, *p.partition));
          });
    } else {
        std::for_each(
          session_ctx.session()->partitions().cbegin_insertion_order(),
          session_ctx.session()->partitions().cend_insertion_order(),
          std::forward<Func>(f));
    }
}

class simple_fetch_planner final : public fetch_planner::impl {
    fetch_plan create_plan(op_context& octx) final {
        fetch_plan plan(ss::smp::count);
        auto resp_it = octx.response_begin();
        auto bytes_left_in_plan = octx.bytes_left;

        plan.reserve_from_partition_count(octx.fetch_partition_count());

        const auto client_address = fmt::format(
          "{}:{}",
          octx.rctx.connection()->client_host(),
          octx.rctx.connection()->client_port());

        /**
         * group fetch requests by shard
         */
        octx.for_each_fetch_partition(
          [&resp_it, &octx, &plan, &bytes_left_in_plan, &client_address](
            const fetch_session_partition& fp) {
              // if this is not an initial fetch we are allowed to skip
              // partions that aleready have an error or we have enough data
              if (!octx.initial_fetch) {
                  bool has_enough_data = !resp_it->empty()
                                         && octx.over_min_bytes();

                  if (resp_it->has_error() || has_enough_data) {
                      ++resp_it;
                      return;
                  }
              }

              // We audit successful messages only on the initial fetch
              audit_on_success audit{octx.initial_fetch};

              /**
               * if not authorized do not include into a plan
               */
              if (!octx.rctx.authorized(
                    security::acl_operation::read,
                    fp.topic_partition.get_topic(),
                    audit)) {
                  resp_it->set(make_partition_response_error(
                    fp.topic_partition.get_partition(),
                    error_code::topic_authorization_failed));
                  ++resp_it;
                  return;
              }

              auto& tp = fp.topic_partition;

              if (unlikely(octx.rctx.metadata_cache().is_disabled(
                    tp.as_tn_view(), tp.get_partition()))) {
                  resp_it->set(make_partition_response_error(
                    fp.topic_partition.get_partition(),
                    error_code::replica_not_available));
                  ++resp_it;
                  return;
              }

              auto shard = octx.rctx.shards().shard_for(tp);
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
                  auto ec = octx.rctx.metadata_cache().contains(tp.to_ntp())
                              ? error_code::not_leader_for_partition
                              : error_code::unknown_topic_or_partition;
                  resp_it->set(make_partition_response_error(
                    fp.topic_partition.get_partition(), ec));
                  ++resp_it;
                  return;
              }

              auto fetch_md = octx.rctx.get_fetch_metadata_cache().get(tp);
              auto max_bytes = std::min(
                bytes_left_in_plan, size_t(fp.max_bytes));
              /**
               * If offset is greater, assume that fetch will read max_bytes
               */
              if (fetch_md && fetch_md->high_watermark > fp.fetch_offset) {
                  bytes_left_in_plan -= max_bytes;
              }

              fetch_config config{
                .start_offset = fp.fetch_offset,
                .max_offset = model::model_limits<model::offset>::max(),
                .max_bytes = max_bytes,
                .timeout = octx.deadline.value_or(model::no_timeout),
                .current_leader_epoch = fp.current_leader_epoch,
                .isolation_level = octx.request.data.isolation_level,
                .strict_max_bytes = octx.response_size > 0,
                .skip_read = bytes_left_in_plan == 0 && max_bytes == 0,
                .read_from_follower = octx.request.has_rack_id(),
                .consumer_rack_id = octx.request.has_rack_id()
                                      ? std::make_optional(
                                        octx.request.data.rack_id)
                                      : std::nullopt,
                .abort_source = octx.rctx.abort_source(),
                .client_address = model::client_address_t{client_address},
              };

              plan.fetches_per_shard[*shard].push_back(
                {tp, std::move(config)}, &(*resp_it));
              ++resp_it;
          });
        return plan;
    }
};

/**
 * Process partition fetch requests.
 *
 * Each request is handled serially in the order they appear in the request.
 * There are a couple reasons why we are not **yet** processing these in
 * parallel. First, Kafka expects to some extent that the order of the
 * partitions in the request is an implicit priority on which partitions to
 * read from. This is closely related to the request budget limits specified
 * in terms of maximum bytes and maximum time delay.
 *
 * Once we start processing requests in parallel we'll have to work through
 * various challenges. First, once we dispatch in parallel, we'll need to
 * develop heuristics for dealing with the implicit priority order. We'll
 * also need to develop techniques and heuristics for dealing with budgets
 * since global budgets aren't trivially divisible onto each core when
 * partition requests may produce non-uniform amounts of data.
 *
 * w.r.t. what is needed to parallelize this, there are no data dependencies
 * between partition requests within the fetch request, and so they can be
 * run fully in parallel. The only dependency that exists is that the
 * response must be reassembled such that the responses appear in these
 * order as the partitions in the request.
 */
static ss::future<> fetch_topic_partitions(op_context& octx) {
    auto planner = make_fetch_planner<simple_fetch_planner>();

    auto fetch_plan = planner.create_plan(octx);

    fetch_plan_executor executor
      = make_fetch_plan_executor<parallel_fetch_plan_executor>();
    co_await executor.execute_plan(octx, std::move(fetch_plan));

    if (octx.should_stop_fetch()) {
        co_return;
    }

    octx.reset_context();
    // debounce next read retry
    co_await ss::sleep(std::min(
      config::shard_local_cfg().fetch_reads_debounce_timeout(),
      octx.request.data.max_wait_ms));
}

namespace testing {
kafka::fetch_plan make_simple_fetch_plan(op_context& octx) {
    auto planner = make_fetch_planner<simple_fetch_planner>();
    return planner.create_plan(octx);
}
} // namespace testing

namespace {
ss::future<> do_fetch(op_context& octx) {
    switch (config::shard_local_cfg().fetch_read_strategy) {
    case model::fetch_read_strategy::polling: {
        // first fetch, do not wait
        co_await fetch_topic_partitions(octx).then([&octx] {
            return ss::do_until(
              [&octx] { return octx.should_stop_fetch(); },
              [&octx] { return fetch_topic_partitions(octx); });
        });
    } break;
    case model::fetch_read_strategy::non_polling: {
        auto planner = make_fetch_planner<simple_fetch_planner>();
        auto fetch_plan = planner.create_plan(octx);

        nonpolling_fetch_plan_executor executor;
        co_await executor.execute_plan(octx, std::move(fetch_plan));
    } break;
    case model::fetch_read_strategy::non_polling_with_debounce: {
        auto planner = make_fetch_planner<simple_fetch_planner>();
        auto fetch_plan = planner.create_plan(octx);

        nonpolling_fetch_plan_executor executor{true};
        co_await executor.execute_plan(octx, std::move(fetch_plan));
    } break;
    default: {
        vassert(false, "not implemented");
    } break;
    }
}
} // namespace

template<>
ss::future<response_ptr>
fetch_handler::handle(request_context rctx, ss::smp_service_group ssg) {
    return ss::do_with(
      std::make_unique<op_context>(std::move(rctx), ssg),
      [](std::unique_ptr<op_context>& octx_ptr) {
          auto sg
            = octx_ptr->rctx.connection()->server().fetch_scheduling_group();
          return ss::with_scheduling_group(sg, [&octx_ptr] {
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
                  // NOTE: Audit call doesn't happen until _after_ the fetch
                  // is done. This was done for the sake of simplicity and
                  // because fetch doesn't alter the state of the broker
                  if (!octx.rctx.audit()) {
                      return std::move(octx).send_error_response(
                        error_code::broker_not_available);
                  }
                  return std::move(octx).send_response();
              });
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
        response.data.topics.reserve(request.data.topics.size());
    }

    if (auto delay = request.debounce_delay(); delay) {
        deadline = model::timeout_clock::now() + delay.value();
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
    response.data.topics.emplace_back(
      fetchable_topic_response{.name = topic.name});
}

void op_context::start_response_partition(const fetch_request::partition& p) {
    response.data.topics.back().partitions.push_back(
      fetch_response::partition_response{
        .partition_index = p.partition_index,
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
        model::topic last_topic;
        std::for_each(
          session_ctx.session()->partitions().cbegin_insertion_order(),
          session_ctx.session()->partitions().cend_insertion_order(),
          [this, &last_topic](const fetch_session_partition& fp) {
              auto& topic = fp.topic_partition.get_topic();
              if (last_topic != topic) {
                  response.data.topics.emplace_back(
                    fetchable_topic_response{.name = topic});
                  last_topic = topic;
              }
              fetch_response::partition_response p{
                .partition_index = fp.topic_partition.get_partition(),
                .error_code = error_code::none,
                .high_watermark = fp.high_watermark,
                .last_stable_offset = fp.last_stable_offset,
                .records = batch_reader()};

              response.data.topics.back().partitions.push_back(std::move(p));
          });
    }
    for (auto it = response.begin(); it != response.end(); ++it) {
        auto raw = new response_placeholder(it, this); // NOLINT
        iteration_order.push_back(*raw);
    }
}

bool update_fetch_partition(
  const fetch_response::partition_response& resp,
  fetch_session_partition& partition) {
    bool include = false;
    if (resp.records && resp.records->size_bytes() > 0) {
        // Partitions with new data are always included in the response.
        include = true;
    }
    if (partition.high_watermark != resp.high_watermark) {
        include = true;
        partition.high_watermark = model::offset(resp.high_watermark);
    }
    if (partition.last_stable_offset != resp.last_stable_offset) {
        include = true;
        partition.last_stable_offset = model::offset(resp.last_stable_offset);
    }
    if (partition.start_offset != resp.log_start_offset) {
        include = true;
        partition.start_offset = model::offset(resp.log_start_offset);
    }
    /**
     * Always include partition in a response if it contains information about
     * the preferred replica
     */
    if (resp.preferred_read_replica != -1) {
        include = true;
    }
    if (include) {
        return include;
    }
    if (resp.error_code != error_code::none) {
        // Partitions with errors are always included in the response.
        // We also set the cached highWatermark to an invalid offset, -1.
        // This ensures that when the error goes away, we re-send the
        // partition.
        partition.high_watermark = model::offset{-1};
        include = true;
    }
    return include;
}

ss::future<response_ptr> op_context::send_response() && {
    // Sessionless fetch
    if (session_ctx.is_sessionless()) {
        response.data.session_id = invalid_fetch_session_id;
        return rctx.respond(std::move(response));
    }
    // bellow we handle incremental fetches, set response session id
    response.data.session_id = session_ctx.session()->id();
    if (session_ctx.is_full_fetch()) {
        return rctx.respond(std::move(response));
    }

    fetch_response final_response;
    final_response.data.error_code = response.data.error_code;
    final_response.data.session_id = response.data.session_id;

    /// Account for special internal topic bytes for usage
    for (const auto& topic : response.data.topics) {
        const bool bytes_to_exclude = std::find(
                                        usage_excluded_topics.cbegin(),
                                        usage_excluded_topics.cend(),
                                        topic.name)
                                      != usage_excluded_topics.cend();
        if (bytes_to_exclude) {
            for (const auto& part : topic.partitions) {
                if (part.records) {
                    final_response.internal_topic_bytes
                      += part.records->size_bytes();
                }
            }
        }
    }

    for (auto it = response.begin(true); it != response.end(); ++it) {
        if (it->is_new_topic) {
            final_response.data.topics.emplace_back(
              fetchable_topic_response{.name = it->partition->name});
        }

        fetch_response::partition_response r{
          .partition_index = it->partition_response->partition_index,
          .error_code = it->partition_response->error_code,
          .high_watermark = it->partition_response->high_watermark,
          .last_stable_offset = it->partition_response->last_stable_offset,
          .log_start_offset = it->partition_response->log_start_offset,
          .aborted = std::move(it->partition_response->aborted),
          .preferred_read_replica
          = it->partition_response->preferred_read_replica,
          .records = std::move(it->partition_response->records)};

        final_response.data.topics.back().partitions.push_back(std::move(r));
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

op_context::response_placeholder::response_placeholder(
  fetch_response::iterator it, op_context* ctx)
  : _it(it)
  , _ctx(ctx) {}

void op_context::response_placeholder::set(
  fetch_response::partition_response&& response) {
    vassert(
      response.partition_index == _it->partition_response->partition_index,
      "Response and current partition ids have to be the same. Current "
      "response {}, update {}",
      _it->partition_response->partition_index,
      response.partition_index);

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
        auto key = model::topic_partition_view(
          _it->partition->name, _it->partition_response->partition_index);

        if (auto it = session_partitions.find(key);
            it != session_partitions.end()) {
            auto has_to_be_included = update_fetch_partition(
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

        auto const node_it = _md_cache.nodes().find(replica.id);
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
            if (replica.high_watermark >= highest_hw) {
                highest_hw = replica.high_watermark;
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

std::ostream& operator<<(std::ostream& o, const consumer_info& ci) {
    fmt::print(o, "rack_id: {}, fetch_offset: {}", ci);
    return o;
}
} // namespace kafka
