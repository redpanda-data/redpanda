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
#include "kafka/protocol/fetch.h"
#include "kafka/server/handlers/handler.h"
#include "kafka/types.h"
#include "utils/intrusive_list_helpers.h"

namespace kafka {

using fetch_handler = single_stage_handler<fetch_api, 4, 11>;

/*
 * Fetch operation context
 */
struct op_context {
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
               || is_empty_request() || response_error
               || deadline <= model::timeout_clock::now();
    }

    bool over_min_bytes() const {
        return static_cast<int32_t>(response_size) >= request.data.min_bytes;
    }

    ss::future<response_ptr> send_response() &&;

    response_iterator response_begin() { return iteration_order.begin(); }

    response_iterator response_end() { return iteration_order.end(); }
    template<typename Func>
    void for_each_fetch_partition(Func&& f) const {
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
                  f(fetch_session_partition{
                    .topic = p.topic->name,
                    .partition = p.partition->partition_index,
                    .max_bytes = p.partition->max_bytes,
                    .fetch_offset = p.partition->fetch_offset,
                  });
              });
        } else {
            std::for_each(
              session_ctx.session()->partitions().cbegin_insertion_order(),
              session_ctx.session()->partitions().cend_insertion_order(),
              std::forward<Func>(f));
        }
    }

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
};

struct fetch_config {
    model::offset start_offset;
    model::offset max_offset;
    model::isolation_level isolation_level;
    size_t max_bytes;
    model::timeout_clock::time_point timeout;
    bool strict_max_bytes{false};
    bool skip_read{false};
    kafka::leader_epoch current_leader_epoch;

    friend std::ostream& operator<<(std::ostream& o, const fetch_config& cfg) {
        fmt::print(
          o,
          R"({{"start_offset": {}, "max_offset": {}, "isolation_lvl": {}, "max_bytes": {}, "strict_max_bytes": {}, "current_leader_epoch:" {}}})",
          cfg.start_offset,
          cfg.max_offset,
          cfg.isolation_level,
          cfg.max_bytes,
          cfg.strict_max_bytes,
          cfg.current_leader_epoch);
        return o;
    }
};

struct ntp_fetch_config {
    ntp_fetch_config(model::ntp n, fetch_config cfg)
      : _ntp(std::move(n))
      , cfg(cfg) {}
    model::ntp _ntp;
    fetch_config cfg;

    const model::ntp& ntp() const { return _ntp; }

    friend std::ostream&
    operator<<(std::ostream& o, const ntp_fetch_config& ntp_fetch) {
        fmt::print(o, R"({{"{}": {}}})", ntp_fetch.ntp(), ntp_fetch.cfg);
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
    explicit read_result(error_code e)
      : error(e) {}

    read_result(
      variant_t data,
      model::offset start_offset,
      model::offset hw,
      model::offset lso,
      std::vector<cluster::rm_stm::tx_range> aborted_transactions)
      : data(std::move(data))
      , start_offset(start_offset)
      , high_watermark(hw)
      , last_stable_offset(lso)
      , error(error_code::none)
      , aborted_transactions(std::move(aborted_transactions)) {}

    read_result(model::offset start_offset, model::offset hw, model::offset lso)
      : start_offset(start_offset)
      , high_watermark(hw)
      , last_stable_offset(lso)
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
    error_code error;
    model::partition_id partition;
    std::vector<cluster::rm_stm::tx_range> aborted_transactions;
};
// struct aggregating fetch requests and corresponding response iterators for
// the same shard
struct shard_fetch {
    void push_back(
      ntp_fetch_config config,
      op_context::response_placeholder_ptr r_ph,
      std::unique_ptr<hdr_hist::measurement> m) {
        requests.push_back(std::move(config));
        responses.push_back(r_ph);
        metrics.push_back(std::move(m));
    }

    bool empty() const {
        vassert(
          requests.size() == responses.size(),
          "there have to be equal number of fetch requests and responsens for "
          "single shard. requests count: {}, response count {}",
          requests.size(),
          responses.size());

        return requests.empty();
    }
    ss::shard_id shard;
    std::vector<ntp_fetch_config> requests;
    std::vector<op_context::response_placeholder_ptr> responses;
    std::vector<std::unique_ptr<hdr_hist::measurement>> metrics;

    friend std::ostream& operator<<(std::ostream& o, const shard_fetch& sf) {
        fmt::print(o, "{}", sf.requests);
        return o;
    }
};

struct fetch_plan {
    explicit fetch_plan(size_t shards)
      : fetches_per_shard(shards) {}

    std::vector<shard_fetch> fetches_per_shard;

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

ss::future<read_result> read_from_ntp(
  cluster::partition_manager&,
  coproc::partition_manager&,
  const model::ntp&,
  fetch_config,
  bool,
  std::optional<model::timeout_clock::time_point>);

} // namespace kafka
