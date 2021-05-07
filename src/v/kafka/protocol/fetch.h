/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/partition.h"
#include "cluster/rm_stm.h"
#include "kafka/protocol/batch_reader.h"
#include "kafka/protocol/schemata/fetch_request.h"
#include "kafka/protocol/schemata/fetch_response.h"
#include "kafka/server/fetch_session.h"
#include "kafka/server/partition_proxy.h"
#include "kafka/server/request_context.h"
#include "kafka/server/response.h"
#include "kafka/types.h"
#include "likely.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timeout_clock.h"
#include "seastarx.h"

#include <seastar/core/future.hh>

namespace kafka {

struct fetch_response;

struct fetch_api final {
    using response_type = fetch_response;

    static constexpr const char* name = "fetch";
    static constexpr api_key key = api_key(1);
};

struct fetch_request final {
    using api_type = fetch_api;
    using partition = fetch_partition;
    using topic = fetch_topic;
    using forgotten_topic = ::kafka::forgotten_topic;

    fetch_request_data data;

    void encode(response_writer& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(request_reader& reader, api_version version) {
        data.decode(reader, version);
    }

    /*
     * For max_wait_time > 0 the request may be debounced in order to collect
     * additional data for the response. Otherwise, no such delay is requested.
     */
    std::optional<std::chrono::milliseconds> debounce_delay() const {
        if (data.max_wait_ms <= std::chrono::milliseconds::zero()) {
            return std::nullopt;
        } else {
            return data.max_wait_ms;
        }
    }

    /**
     * return empty if request doesn't contain any topics or all topics are
     * empty
     */
    bool empty() const {
        return data.topics.empty()
               || std::all_of(
                 data.topics.cbegin(), data.topics.cend(), [](const topic& t) {
                     return t.fetch_partitions.empty();
                 });
    }

    /**
     * Check if this request is a full fetch request initiating a session
     */
    bool is_full_fetch_request() const {
        return data.session_epoch == initial_fetch_session_epoch
               || data.session_epoch == final_fetch_session_epoch;
    }

    /*
     * iterator over request partitions. this adapter iterator is used because
     * the partitions are decoded off the wire directly into a hierarhical
     * representation:
     *
     *       [
     *         topic0 -> [...]
     *         topic1 -> [topic1-part1, topic1-part2, ...]
     *         ...
     *         topicN -> [...]
     *       ]
     *
     * the iterator value is a reference to the current topic and partition.
     */
    class const_iterator {
    public:
        using const_topic_iterator = std::vector<topic>::const_iterator;
        using const_partition_iterator = std::vector<partition>::const_iterator;

        struct value_type {
            bool new_topic;
            const_topic_iterator topic;
            const_partition_iterator partition;
        };

        using difference_type = void;
        using pointer = const value_type*;
        using reference = const value_type&;
        using iterator_category = std::forward_iterator_tag;

        const_iterator(const_topic_iterator begin, const_topic_iterator end)
          : state_({.new_topic = true, .topic = begin})
          , t_end_(end) {
            if (likely(state_.topic != t_end_)) {
                state_.partition = state_.topic->fetch_partitions.cbegin();
                normalize();
            }
        }

        reference operator*() const noexcept { return state_; }

        pointer operator->() const noexcept { return &state_; }

        const_iterator& operator++() {
            state_.partition++;
            state_.new_topic = false;
            normalize();
            return *this;
        }
        const_iterator operator++(int) {
            const_iterator tmp = *this;
            ++(*this);
            return tmp;
        }

        bool operator==(const const_iterator& o) const noexcept {
            if (state_.topic == o.state_.topic) {
                if (state_.topic == t_end_) {
                    return true;
                } else {
                    return state_.partition == o.state_.partition;
                }
            }
            return false;
        }

        bool operator!=(const const_iterator& o) const noexcept {
            return !(*this == o);
        }

    private:
        void normalize() {
            while (state_.partition == state_.topic->fetch_partitions.cend()) {
                state_.topic++;
                state_.new_topic = true;
                if (state_.topic != t_end_) {
                    state_.partition = state_.topic->fetch_partitions.cbegin();
                } else {
                    break;
                }
            }
        }

        value_type state_;
        const_topic_iterator t_end_;
    };

    const_iterator cbegin() const {
        return const_iterator(data.topics.cbegin(), data.topics.cend());
    }

    const_iterator cend() const {
        return const_iterator(data.topics.cend(), data.topics.cend());
    }
};

inline std::ostream& operator<<(std::ostream& os, const fetch_request& r) {
    return os << r.data;
}

struct fetch_response final {
    using api_type = fetch_api;
    using aborted_transaction = kafka::aborted_transaction;
    using partition_response = fetchable_partition_response;
    using partition = fetchable_topic_response;

    fetch_response_data data;

    void encode(response_writer& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(iobuf buf, api_version version) {
        data.decode(std::move(buf), version);
    }

    /*
     * iterator over response partitions. this adapter iterator is used because
     * the partitions are encoded into the vector of partition responses
     *
     *       [
     *         partition0 -> [...]
     *         partition1 -> [partition_resp1, partition_resp2, ...]
     *         ...
     *         partitionN -> [...]
     *       ]
     *
     * the iterator value is a reference to the current partition and
     * partition_response.
     */
    class iterator {
    public:
        using partition_iterator = std::vector<partition>::iterator;
        using partition_response_iterator
          = std::vector<partition_response>::iterator;

        struct value_type {
            partition_iterator partition;
            partition_response_iterator partition_response;
            bool is_new_topic;
        };

        using difference_type = void;
        using pointer = value_type*;
        using reference = value_type&;
        using iterator_category = std::forward_iterator_tag;

        iterator(
          partition_iterator begin,
          partition_iterator end,
          bool enable_filtering = false)
          : state_({.partition = begin})
          , t_end_(end)
          , filter_(enable_filtering) {
            if (likely(state_.partition != t_end_)) {
                state_.partition_response
                  = state_.partition->partitions.begin();
                state_.is_new_topic = true;
                normalize();
            }
        }

        reference operator*() noexcept { return state_; }

        pointer operator->() noexcept { return &state_; }

        iterator& operator++() {
            state_.is_new_topic = false;
            state_.partition_response++;
            normalize();
            return *this;
        }
        iterator operator++(int) {
            iterator tmp = *this;
            ++(*this);
            return tmp;
        }

        bool operator==(const iterator& o) const noexcept {
            if (state_.partition == o.state_.partition) {
                if (state_.partition == t_end_) {
                    return true;
                } else {
                    return state_.partition_response
                           == o.state_.partition_response;
                }
            }
            return false;
        }

        bool operator!=(const iterator& o) const noexcept {
            return !(*this == o);
        }

    private:
        void normalize() {
            while (state_.partition_response
                   == state_.partition->partitions.end()) {
                state_.partition++;
                if (state_.partition != t_end_) {
                    state_.is_new_topic = true;
                    state_.partition_response
                      = state_.partition->partitions.begin();
                } else {
                    break;
                }
            }

            // filter responsens that we do not want to include
            if (
              filter_ && state_.partition != t_end_
              && !state_.partition_response->has_to_be_included) {
                state_.partition_response++;
                normalize();
            }
        }

        value_type state_;
        partition_iterator t_end_;
        bool filter_;
    };

    iterator begin(bool enable_filtering = false) {
        return iterator(
          data.topics.begin(), data.topics.end(), enable_filtering);
    }

    iterator end() { return iterator(data.topics.end(), data.topics.end()); }
};

inline std::ostream& operator<<(std::ostream& os, const fetch_response& r) {
    return os << r.data;
}

/*
 * Fetch operation context
 */
struct op_context {
    class response_iterator {
    public:
        using difference_type = void;
        using pointer = fetch_response::iterator::pointer;
        using reference = fetch_response::iterator::reference;
        using iterator_category = std::forward_iterator_tag;

        response_iterator(fetch_response::iterator, op_context* ctx);

        reference operator*() noexcept { return *_it; }

        pointer operator->() noexcept { return &(*_it); }

        response_iterator& operator++();

        const response_iterator operator++(int);

        bool operator==(const response_iterator& o) const noexcept;

        bool operator!=(const response_iterator& o) const noexcept;

        void set(fetch_response::partition_response&&);

    private:
        fetch_response::iterator _it;
        op_context* _ctx;
    };

    void reset_context();

    // decode request and initialize budgets
    op_context(request_context&& ctx, ss::smp_service_group ssg);

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

    response_iterator response_begin(bool enable_filtering = false) {
        return response_iterator(response.begin(enable_filtering), this);
    }

    response_iterator response_end() {
        return response_iterator(response.end(), this);
    }
    template<typename Func>
    void for_each_fetch_partition(Func&& f) const {
        if (session_ctx.is_full_fetch() || session_ctx.is_sessionless()) {
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
};

struct fetch_config {
    model::offset start_offset;
    model::offset max_offset;
    model::isolation_level isolation_level;
    size_t max_bytes;
    model::timeout_clock::time_point timeout;
    bool strict_max_bytes{false};
};

struct ntp_fetch_config {
    ntp_fetch_config(
      model::ntp ntp,
      fetch_config cfg,
      std::optional<model::ntp> materialized_ntp = std::nullopt)
      : ntp(std::move(ntp))
      , cfg(cfg)
      , materialized_ntp(std::move(materialized_ntp)) {}
    model::ntp ntp;
    fetch_config cfg;
    std::optional<model::ntp> materialized_ntp;

    bool is_materialized() const { return materialized_ntp.has_value(); }
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
    void push_back(ntp_fetch_config config, op_context::response_iterator it) {
        requests.push_back(std::move(config));
        responses.push_back(it);
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

    std::vector<ntp_fetch_config> requests;
    std::vector<op_context::response_iterator> responses;
};

std::optional<partition_proxy> make_partition_proxy(
  const model::materialized_ntp&,
  ss::lw_shared_ptr<cluster::partition>,
  cluster::partition_manager& pm);

ss::future<read_result> read_from_ntp(
  cluster::partition_manager&,
  const model::materialized_ntp&,
  fetch_config,
  bool,
  std::optional<model::timeout_clock::time_point>);

} // namespace kafka
