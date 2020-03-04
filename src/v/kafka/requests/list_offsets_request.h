#pragma once
#include "bytes/iobuf.h"
#include "kafka/errors.h"
#include "kafka/requests/fwd.h"
#include "kafka/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timestamp.h"

#include <seastar/core/future.hh>

#include <absl/container/btree_set.h>

namespace kafka {

struct list_offsets_response;

class list_offsets_api final {
public:
    using response_type = list_offsets_response;

    static constexpr const char* name = "list_offsets";
    static constexpr api_key key = api_key(2);
    static constexpr api_version min_supported = api_version(1);
    static constexpr api_version max_supported = api_version(3);

    static ss::future<response_ptr>
    process(request_context&&, ss::smp_service_group);
};

struct list_offsets_request final {
    using api_type = list_offsets_api;

    static constexpr model::timestamp earliest_timestamp{-2};
    static constexpr model::timestamp latest_timestamp{-1};

    struct partition {
        model::partition_id id;
        model::timestamp timestamp;
    };

    struct topic {
        model::topic name;
        std::vector<partition> partitions;
    };

    model::node_id replica_id;
    int8_t isolation_level; // >= v2
    std::vector<topic> topics;

    list_offsets_request() = default;
    list_offsets_request(request_context& ctx) { decode(ctx); }

    void encode(response_writer& writer, api_version version);
    void decode(request_context& ctx);

    absl::btree_set<model::topic_partition> tp_dups;

    bool duplicate_tp(const model::topic& t, model::partition_id id) const {
        model::topic_partition tp{.topic = t, .partition = id};
        return tp_dups.find(tp) != tp_dups.end();
    }
};

struct list_offsets_response final {
    struct partition {
        model::partition_id id;
        error_code error;
        model::timestamp timestamp;
        model::offset offset;

        partition(
          model::partition_id id,
          error_code error,
          model::timestamp timestamp,
          model::offset offset)
          : id(id)
          , error(error)
          , timestamp(timestamp)
          , offset(offset) {}

        partition(
          model::partition_id id,
          model::timestamp timestamp,
          model::offset offset)
          : id(id)
          , error(error_code::none)
          , timestamp(timestamp)
          , offset(offset) {}

        partition(model::partition_id id, error_code error)
          : partition(id, error, model::timestamp(-1), model::offset(-1)) {}
    };

    struct topic {
        model::topic name;
        std::vector<partition> partitions;
    };

    std::chrono::milliseconds throttle_time_ms{0}; // >= v2
    std::vector<topic> topics;

    void encode(const request_context& ctx, response& resp);
    void decode(iobuf buf, api_version version);
};

} // namespace kafka
