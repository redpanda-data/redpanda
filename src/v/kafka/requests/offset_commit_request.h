#pragma once

#include "bytes/iobuf.h"
#include "kafka/errors.h"
#include "kafka/requests/fwd.h"
#include "kafka/requests/schemata/offset_commit_request.h"
#include "kafka/requests/schemata/offset_commit_response.h"
#include "kafka/types.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "seastarx.h"

#include <seastar/core/future.hh>

namespace kafka {

struct offset_commit_response;

struct offset_commit_api final {
    using response_type = offset_commit_response;

    static constexpr const char* name = "offset commit";
    static constexpr api_key key = api_key(8);

    // in version 0 kafka stores offsets in zookeeper. if we ever need to
    // support version 0 then we need to do some code review to see if this has
    // any implications on semantics.
    static constexpr api_version min_supported = api_version(1);
    static constexpr api_version max_supported = api_version(7);

    static ss::future<response_ptr>
    process(request_context&&, ss::smp_service_group);
};

struct offset_commit_request final {
    using api_type = offset_commit_api;

    offset_commit_request_data data;

    // set during request processing after mapping group to ntp
    model::ntp ntp;

    void encode(response_writer& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(request_reader& reader, api_version version) {
        data.decode(reader, version);
    }
};

inline std::ostream&
operator<<(std::ostream& os, const offset_commit_request& r) {
    return os << r.data;
}

struct offset_commit_response final {
    using api_type = offset_commit_api;

    offset_commit_response_data data;

    offset_commit_response() = default;

    offset_commit_response(
      const offset_commit_request& request, error_code error) {
        for (const auto& t : request.data.topics) {
            offset_commit_response_topic tmp{.name = t.name};
            for (const auto& p : t.partitions) {
                tmp.partitions.push_back({
                  .partition_index = p.partition_index,
                  .error_code = error,
                });
            }
            data.topics.push_back(std::move(tmp));
        }
    }

    void encode(const request_context& ctx, response& resp) {
        data.encode(ctx, resp);
    }

    void decode(iobuf buf, api_version version) {
        data.decode(std::move(buf), version);
    }
};

inline std::ostream&
operator<<(std::ostream& os, const offset_commit_response& r) {
    return os << r.data;
}

} // namespace kafka
