/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/likely.h"
#include "base/seastarx.h"
#include "container/fragmented_vector.h"
#include "kafka/protocol/batch_reader.h"
#include "kafka/protocol/schemata/fetch_request.h"
#include "kafka/protocol/schemata/fetch_response.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timeout_clock.h"

#include <seastar/core/future.hh>

namespace kafka {

struct fetch_request final {
    using api_type = fetch_api;
    using partition = fetch_partition;
    using topic = fetch_topic;
    using forgotten_topic = ::kafka::forgotten_topic;

    fetch_request_data data;

    void encode(protocol::encoder& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(protocol::decoder& reader, api_version version) {
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
    // Indicates that the consumer has rack_id set, the rack being set express
    // an intent to read from the closest replica, including follower
    bool has_rack_id() const { return data.rack_id != model::rack_id{""}; }

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
        using const_topic_iterator = chunked_vector<topic>::const_iterator;
        using const_partition_iterator
          = chunked_vector<partition>::const_iterator;

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

    friend std::ostream& operator<<(std::ostream& os, const fetch_request& r) {
        return os << r.data;
    }
};

struct fetch_response final {
    using api_type = fetch_api;
    using aborted_transaction = kafka::aborted_transaction;
    using partition_response = fetchable_partition_response;
    using partition = fetchable_topic_response;

    fetch_response_data data;

    // Used for usage/metering to relay this value back to the connection layer
    size_t internal_topic_bytes{0};

    void encode(protocol::encoder& writer, api_version version) {
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
        using partition_iterator = chunked_vector<partition>::iterator;
        using partition_response_iterator
          = small_fragment_vector<partition_response>::iterator;

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

    friend std::ostream& operator<<(std::ostream& os, const fetch_response& r) {
        return os << r.data;
    }
};

} // namespace kafka
