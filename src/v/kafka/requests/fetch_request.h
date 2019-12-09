#pragma once

#include "kafka/requests/request_context.h"
#include "kafka/requests/response.h"
#include "seastarx.h"

#include <seastar/core/future.hh>

namespace kafka {

class fetch_api final {
public:
    static constexpr const char* name = "fetch";
    static constexpr api_key key = api_key(1);
    static constexpr api_version min_supported = api_version(4);
    static constexpr api_version max_supported = api_version(4);

    static future<response_ptr> process(request_context&&, smp_service_group);
};

struct fetch_request final {
    struct partition {
        model::partition_id id;
        model::offset fetch_offset;
        int32_t partition_max_bytes;
    };

    struct topic {
        model::topic name;
        std::vector<partition> partitions;
    };

    model::node_id replica_id;
    std::chrono::milliseconds max_wait_time;
    int32_t min_bytes;
    int32_t max_bytes;      // >= v3
    int8_t isolation_level; // >= v4
    std::vector<topic> topics;

    void encode(response_writer& writer, api_version version);
    void decode(request_context& ctx);

    /*
     * For max_wait_time > 0 the request may be debounced in order to collect
     * additional data for the response. Otherwise, no such delay is requested.
     */
    std::optional<std::chrono::milliseconds> debounce_delay() const {
        if (max_wait_time <= std::chrono::milliseconds::zero()) {
            return std::nullopt;
        } else {
            return max_wait_time;
        }
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
            if (__builtin_expect(state_.topic != t_end_, true)) {
                state_.partition = state_.topic->partitions.cbegin();
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
            while (state_.partition == state_.topic->partitions.cend()) {
                state_.topic++;
                state_.new_topic = true;
                if (state_.topic != t_end_) {
                    state_.partition = state_.topic->partitions.cbegin();
                } else {
                    break;
                }
            }
        }

        value_type state_;
        const_topic_iterator t_end_;
    };

    const_iterator cbegin() const {
        return const_iterator(topics.cbegin(), topics.cend());
    }

    const_iterator cend() const {
        return const_iterator(topics.cend(), topics.cend());
    }
};

std::ostream& operator<<(std::ostream&, const fetch_request&);

struct fetch_response final {
    struct aborted_transaction {
        int64_t producer_id;
        model::offset first_offset;
    };

    struct partition_response {
        model::partition_id id;
        error_code error;
        model::offset high_watermark;
        model::offset last_stable_offset;                      // >= v4
        std::vector<aborted_transaction> aborted_transactions; // >= v4
        std::optional<iobuf> record_set;
    };

    struct partition {
        model::topic name;
        std::vector<partition_response> responses;

        partition(model::topic name)
          : name(std::move(name)) {}
    };

    fetch_response()
      : throttle_time(0) {}

    std::chrono::milliseconds throttle_time; // >= v1
    std::vector<partition> partitions;

    void encode(const request_context& ctx, response& resp);
    void decode(iobuf buf, api_version version);
};

std::ostream& operator<<(std::ostream&, const fetch_response&);

/*
 * Fetch operation context
 */
struct op_context {
    request_context rctx;
    smp_service_group ssg;
    fetch_request request;
    fetch_response response;

    // operation budgets
    size_t bytes_left;
    model::timeout_clock::time_point deadline;

    // size of response
    size_t response_size;
    // does the response contain an error
    bool response_error;

    // decode request and initialize budgets
    op_context(request_context&& ctx, smp_service_group ssg)
      : rctx(std::move(ctx))
      , ssg(ssg)
      , response_size(0)
      , response_error(false) {
        /*
         * decode request and prepare the inital response
         */
        request.decode(rctx);
        if (__builtin_expect(!request.topics.empty(), true)) {
            response.partitions.reserve(request.topics.size());
        }

        if (auto delay = request.debounce_delay(); delay) {
            deadline = model::timeout_clock::now() + delay.value();
        } else {
            deadline = model::no_timeout;
        }

        /*
         * TODO: max size is multifaceted. it needs to be absolute, but also
         * integrate with other resource contraints that are dynamic within the
         * kafka server itself.
         */
        static constexpr size_t MAX_SIZE = 128 << 20;
        bytes_left = std::min(MAX_SIZE, size_t(request.max_bytes));
    }

    // insert and reserve space for a new topic in the response
    void start_response_topic(const fetch_request::topic& topic) {
        auto& p = response.partitions.emplace_back(topic.name);
        p.responses.reserve(topic.partitions.size());
    }

    // add to the response the result of fetching from a partition
    void add_partition_response(fetch_response::partition_response&& r) {
        if (r.record_set) {
            response_size += r.record_set->size_bytes();
            bytes_left -= std::min(bytes_left, r.record_set->size_bytes());
        }
        response.partitions.back().responses.push_back(std::move(r));
    }
};

struct fetch_config {
    model::offset start_offset;
    size_t max_bytes;
    model::timeout_clock::time_point timeout;
};

future<fetch_response::partition_response>
read_from_ntp(op_context& octx, model::ntp ntp, fetch_config config);

} // namespace kafka
