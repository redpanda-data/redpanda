#pragma once

#include "kafka/requests/kafka_batch_adapter.h"
#include "kafka/requests/request_context.h"
#include "kafka/requests/response.h"
#include "seastarx.h"

#include <seastar/core/future.hh>

namespace kafka {

/**
 * Support starts at version 3 because this is the first version that supports
 * version 2 of the kafka message format.
 */
class produce_api final {
public:
    static constexpr const char* name = "produce";
    static constexpr api_key key = api_key(0);
    static constexpr api_version min_supported = api_version(3);
    static constexpr api_version max_supported = api_version(7);

    static ss::future<response_ptr>
    process(request_context&&, ss::smp_service_group);
};

struct produce_request final {
    struct partition {
        model::partition_id id;
        std::optional<iobuf> data;

        // the kafka batch format on the wire is slightly different than what is
        // managed by redpanda. this adapter is setup during request decoding,
        // and is not involved in request encoding.
        kafka_batch_adapter adapter;
    };

    struct topic {
        model::topic name;
        std::vector<partition> partitions;
    };

    std::optional<ss::sstring> transactional_id;
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

        explicit partition(model::partition_id id)
          : id(id)
          , base_offset(-1)
          , log_append_time(-1)
          , log_start_offset(-1) {}

        partition(model::partition_id id, error_code error)
          : id(id)
          , error(error)
          , base_offset(-1)
          , log_append_time(-1)
          , log_start_offset(-1) {}
    };

    struct topic {
        model::topic name;
        std::vector<partition> partitions;
    };

    std::vector<topic> topics;
    std::chrono::milliseconds throttle = std::chrono::milliseconds(0);

    void encode(const request_context& ctx, response& resp);
};

} // namespace kafka
