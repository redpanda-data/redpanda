#pragma once

#include "kafka/requests/kafka_batch_adapter.h"
#include "kafka/requests/request_context.h"
#include "kafka/requests/response.h"
#include "seastarx.h"

#include <seastar/core/future.hh>

namespace kafka {

class produce_api final {
public:
    static constexpr const char* name = "produce";
    static constexpr api_key key = api_key(0);
    static constexpr api_version min_supported = api_version(3);
    static constexpr api_version max_supported = api_version(7);

    static future<response_ptr>
    process(request_context&&, seastar::smp_service_group);
};

struct produce_request final {
    struct topic_data {
        model::partition_id id;
        std::optional<iobuf> data;

        // the kafka batch format on the wire is slightly different than what is
        // managed by redpanda. this adapter is setup during request decoding,
        // and is not involved in request encoding.
        kafka_batch_adapter adapter;
    };

    struct topic {
        model::topic name;
        std::vector<topic_data> data;
    };

    std::optional<sstring> transactional_id;
    int16_t acks;
    std::chrono::milliseconds timeout;
    std::vector<topic> topics;

    void encode(const request_context& ctx, response_writer& writer);
    void decode(request_context& ctx);

    /// True if the request contains a batch with a transactional id.
    bool has_transactional = false;

    /// True if the request contains a batch with a producer id.
    bool has_idempotent = false;
};

std::ostream& operator<<(std::ostream&, const produce_request&);

struct produce_response final {
    struct partition {
        model::partition_id id;
        error_code error;
        model::offset base_offset;
        model::timestamp log_append_time;
        model::offset log_start_offset; // >= v5
    };

    struct topic {
        model::topic name;
        std::vector<partition> partitions;
    };

    std::vector<topic> topics;
    std::chrono::milliseconds throttle;

    void encode(const request_context& ctx, response& resp);
};

} // namespace kafka
