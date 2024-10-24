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
#pragma once
#include "cluster/rm_stm.h"
#include "container/intrusive_list_helpers.h"
#include "kafka/protocol/fetch.h"
#include "kafka/server/handlers/fetch/replica_selector.h"
#include "kafka/server/handlers/handler.h"
#include "model/fundamental.h"
#include "model/ktp.h"
#include "model/metadata.h"
#include "ssx/abort_source.h"
#include "utils/log_hist.h"

#include <seastar/core/smp.hh>

#include <memory>

namespace kafka {

using fetch_handler = single_stage_handler<fetch_api, 4, 11>;

/*
 * Fetch operation context
 */
struct op_context {
    using latency_clock = log_hist_internal::clock_type;
    using latency_point = latency_clock::time_point;

    class response_placeholder {
    public:
        response_placeholder(fetch_response::iterator, op_context* ctx);

        void set(fetch_response::partition_response&&);

        const model::topic& topic() { return _it->partition->name; }
        model::partition_id partition_id() {
            return _it->partition_response->partition_index;
        }

        bool empty() { return _it->partition_response->records->empty(); }
        bool has_error() {
            return _it->partition_response->error_code != error_code::none;
        }
        void move_to_end() {
            _ctx->iteration_order.erase(
              _ctx->iteration_order.iterator_to(*this));
            _ctx->iteration_order.push_back(*this);
        }
        intrusive_list_hook _hook;

    private:
        fetch_response::iterator _it;
        op_context* _ctx;
    };

    using iteration_order_t
      = intrusive_list<response_placeholder, &response_placeholder::_hook>;
    using response_iterator = iteration_order_t::iterator;
    using response_placeholder_ptr = response_placeholder*;
    void reset_context();

    // decode request and initialize budgets
    op_context(request_context&& ctx, ss::smp_service_group ssg);

    op_context(op_context&&) = delete;
    op_context(const op_context&) = delete;
    op_context& operator=(const op_context&) = delete;
    op_context& operator=(op_context&&) = delete;
    ~op_context() {
        iteration_order.clear_and_dispose([](response_placeholder* ph) {
            delete ph; // NOLINT
        });
    }

    // reserve space for a new topic in the response
    void start_response_topic(const fetch_request::topic& topic);

    // reserve space for new partition in the response
    void start_response_partition(const fetch_request::partition&);

    // create placeholder for response topics and partitions
    void create_response_placeholders();

    bool is_empty_request() const {
        /**
         * If request doesn't have a session or it is a full fetch request, we
         * check only request content.
         */
        if (session_ctx.is_sessionless() || session_ctx.is_full_fetch()) {
            return request.empty();
        }

        /**
         * If session is present both session and request must be empty to claim
         * fetch operation as being empty
         */
        return session_ctx.session()->empty() && request.empty();
    }

    bool should_stop_fetch() const {
        return !request.debounce_delay() || over_min_bytes()
               || is_empty_request() || contains_preferred_replica
               || response_error || rctx.abort_requested()
               || deadline <= model::timeout_clock::now();
    }

    bool over_min_bytes() const {
        return static_cast<int32_t>(response_size) >= request.data.min_bytes;
    }

    ss::future<response_ptr> send_response() &&;

    ss::future<response_ptr> send_error_response(error_code ec) &&;

    response_iterator response_begin() { return iteration_order.begin(); }

    response_iterator response_end() { return iteration_order.end(); }

    /**
     * @brief Get an estimate of the number of partitions in the fetch.
     *
     * This is only an estimate, perhaps useful to pre-size some structures
     * and currently returns 0 for sessionless fetches.
     */
    size_t fetch_partition_count() const;

    template<typename Func>
    void for_each_fetch_partition(Func&& f) const;

    request_context rctx;
    ss::smp_service_group ssg;
    fetch_request request;
    fetch_response response;

    // operation budgets
    size_t bytes_left;
    std::optional<model::timeout_clock::time_point> deadline;

    // size of response
    size_t response_size;
    // does the response contain an error
    bool response_error;

    bool initial_fetch = true;
    fetch_session_ctx session_ctx;
    iteration_order_t iteration_order;
    // for fetches that have preferred replica set we skip read, therefore we
    // need other indicator of finished fetch request.
    bool contains_preferred_replica = false;
};

struct fetch_config {
    model::offset start_offset;
    model::offset max_offset;
    size_t max_bytes;
    model::timeout_clock::time_point timeout;
    kafka::leader_epoch current_leader_epoch;
    model::isolation_level isolation_level;
    bool strict_max_bytes{false};
    bool skip_read{false};
    bool read_from_follower{false};
    std::optional<model::rack_id> consumer_rack_id;
    std::optional<std::reference_wrapper<ssx::sharded_abort_source>>
      abort_source;
    std::optional<model::client_address_t> client_address;

    friend std::ostream& operator<<(std::ostream& o, const fetch_config& cfg) {
        fmt::print(
          o,
          R"({{"start_offset": {}, "max_offset": {}, "isolation_lvl": {}, "max_bytes": {}, "strict_max_bytes": {}, "skip_read": {}, "current_leader_epoch:" {}, "follower_read:" {}, "consumer_rack_id": {}, "abortable": {}, "aborted": {}, "client_address": {}}})",
          cfg.start_offset,
          cfg.max_offset,
          cfg.isolation_level,
          cfg.max_bytes,
          cfg.strict_max_bytes,
          cfg.skip_read,
          cfg.current_leader_epoch,
          cfg.read_from_follower,
          cfg.consumer_rack_id,
          cfg.abort_source.has_value(),
          cfg.abort_source.has_value()
            ? cfg.abort_source.value().get().abort_requested()
            : false,
          cfg.client_address.value_or(model::client_address_t{}));
        return o;
    }
};

struct ntp_fetch_config {
    ntp_fetch_config(model::ktp ktp, fetch_config cfg)
      : _ktp(std::move(ktp))
      , cfg(cfg) {}
    model::ktp _ktp;
    fetch_config cfg;

    const model::ktp& ktp() const { return _ktp; }

    friend std::ostream&
    operator<<(std::ostream& o, const ntp_fetch_config& ntp_fetch) {
        fmt::print(o, R"({{"{}": {}}})", ntp_fetch.ktp(), ntp_fetch.cfg);
        return o;
    }
};

/**
 * Simple type aggregating either data or an error
 */
struct read_result {
    using foreign_data_t = ss::foreign_ptr<std::unique_ptr<iobuf>>;
    using data_t = std::unique_ptr<iobuf>;
    using variant_t = std::variant<data_t, foreign_data_t>;

    /// Holds semaphore units from memory semaphores. Can be passed across
    /// shards, semaphore units will be released in the shard where the instance
    /// of this class has been created.
    struct memory_units_t {
        ssx::semaphore_units kafka;
        ssx::semaphore_units fetch;
        ss::shard_id shard = ss::this_shard_id();

        ~memory_units_t() noexcept;
        memory_units_t() noexcept = default;
        memory_units_t(memory_units_t&&) noexcept = default;
        memory_units_t& operator=(memory_units_t&&) noexcept = default;
        memory_units_t(const memory_units_t&) = delete;
        memory_units_t& operator=(const memory_units_t&) = delete;
        memory_units_t(
          ssx::semaphore& memory_sem,
          ssx::semaphore& memory_fetch_sem) noexcept;

        /*
         * Adopts another memory_units_t. This requires that both
         * memory_units_t are from the same shard.
         */
        void adopt(memory_units_t&& o);

        bool has_units() const {
            return fetch.count() > 0 || kafka.count() > 0;
        }
    };

    explicit read_result(error_code e)
      : error(e) {}

    // special case for offset_out_of_range_error
    read_result(
      error_code e, model::offset start_offset, model::offset high_watermark)
      : start_offset(start_offset)
      , high_watermark(high_watermark)
      , error(e) {}

    read_result(
      variant_t data,
      model::offset start_offset,
      model::offset hw,
      model::offset lso,
      std::optional<std::chrono::milliseconds> delta,
      std::vector<cluster::tx::tx_range> aborted_transactions)
      : data(std::move(data))
      , start_offset(start_offset)
      , high_watermark(hw)
      , last_stable_offset(lso)
      , delta_from_tip_ms(delta)
      , error(error_code::none)
      , aborted_transactions(std::move(aborted_transactions)) {}

    read_result(
      model::offset start_offset,
      model::offset hw,
      model::offset lso,
      std::optional<model::node_id> preferred_replica = std::nullopt)
      : start_offset(start_offset)
      , high_watermark(hw)
      , last_stable_offset(lso)
      , preferred_replica(preferred_replica)
      , error(error_code::none) {}

    bool has_data() const {
        return ss::visit(
          data,
          [](const data_t& d) { return d != nullptr; },
          [](const foreign_data_t& d) { return !d->empty(); });
    }

    const iobuf& get_data() const {
        if (std::holds_alternative<data_t>(data)) {
            return *std::get<data_t>(data);
        } else {
            return *std::get<foreign_data_t>(data);
        }
    }

    size_t data_size_bytes() const {
        return ss::visit(
          data,
          [](const data_t& d) { return d == nullptr ? 0 : d->size_bytes(); },
          [](const foreign_data_t& d) {
              return d->empty() ? 0 : d->size_bytes();
          });
    }

    iobuf release_data() && {
        return ss::visit(
          data,
          [](data_t& d) { return std::move(*d); },
          [](foreign_data_t& d) {
              auto ret = d->copy();
              d.reset();
              return ret;
          });
    }

    variant_t data;
    model::offset start_offset;
    model::offset high_watermark;
    model::offset last_stable_offset;
    std::optional<std::chrono::milliseconds> delta_from_tip_ms;
    std::optional<model::node_id> preferred_replica;
    error_code error;
    model::partition_id partition;
    std::vector<cluster::tx::tx_range> aborted_transactions;
    memory_units_t memory_units;
};
// struct aggregating fetch requests and corresponding response iterators for
// the same shard
struct shard_fetch {
    explicit shard_fetch(op_context::latency_point start_time)
      : start_time{start_time} {}

    void push_back(
      ntp_fetch_config config, op_context::response_placeholder_ptr r_ph) {
        requests.push_back(std::move(config));
        responses.push_back(r_ph);
    }
    bool empty() const;

    void reserve(size_t n) {
        requests.reserve(n);
        responses.reserve(n);
    }

    ss::shard_id shard;
    std::vector<ntp_fetch_config> requests;
    std::vector<op_context::response_placeholder_ptr> responses;
    op_context::latency_point start_time;

    friend std::ostream& operator<<(std::ostream& o, const shard_fetch& sf) {
        fmt::print(o, "{}", sf.requests);
        return o;
    }
};

struct fetch_plan {
    explicit fetch_plan(
      size_t shards,
      op_context::latency_point start_time = op_context::latency_clock::now())
      : fetches_per_shard(shards, shard_fetch(start_time)) {
        for (size_t i = 0; i < fetches_per_shard.size(); i++) {
            fetches_per_shard[i].shard = i;
        }
    }

    std::vector<shard_fetch> fetches_per_shard;

    void reserve_from_partition_count(size_t count) {
        size_t per_partition = count * 3 / (fetches_per_shard.size() * 2);
        for (auto& f : fetches_per_shard) {
            f.reserve(per_partition);
        }
    }

    friend std::ostream& operator<<(std::ostream& o, const fetch_plan& plan) {
        fmt::print(o, "{{[");
        if (!plan.fetches_per_shard.empty()) {
            fmt::print(
              o,
              R"({{"shard": 0, "requests": [{}]}})",
              plan.fetches_per_shard[0]);
            for (size_t i = 1; i < plan.fetches_per_shard.size(); ++i) {
                fmt::print(
                  o,
                  R"(, {{"shard": {}, "requests": [{}]}})",
                  i,
                  plan.fetches_per_shard[i]);
            }
        }
        fmt::print(o, "]}}");
        return o;
    }
};

/*
 * Unit Tests Exposure
 */
namespace testing {

ss::future<read_result> read_from_ntp(
  cluster::partition_manager&,
  const replica_selector&,
  const model::ktp&,
  fetch_config,
  bool,
  std::optional<model::timeout_clock::time_point>,
  bool obligatory_batch_read,
  ssx::semaphore& memory_sem,
  ssx::semaphore& memory_fetch_sem);

/**
 * Create a fetch plan with the simple fetch planner.
 *
 * Exposed for testing/benchmarking only.
 */
kafka::fetch_plan make_simple_fetch_plan(op_context& octx);

read_result::memory_units_t reserve_memory_units(
  ssx::semaphore& memory_sem,
  ssx::semaphore& memory_fetch_sem,
  const size_t max_bytes,
  const bool obligatory_batch_read);

ss::future<> do_fetch(op_context& octx);

} // namespace testing
} // namespace kafka
