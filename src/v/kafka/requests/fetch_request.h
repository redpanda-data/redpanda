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

    void encode(const request_context& ctx, response_writer& writer);
    void decode(request_context& ctx);
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
        iobuf record_set;
    };

    struct partition {
        model::topic name;
        std::vector<partition_response> responses;
    };

    fetch_response()
      : throttle_time(0) {
    }

    std::chrono::milliseconds throttle_time; // >= v1
    std::vector<partition> partitions;

    void encode(const request_context& ctx, response& resp);
};

std::ostream& operator<<(std::ostream&, const fetch_response&);

} // namespace kafka
