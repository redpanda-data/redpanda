#pragma once

#include "bytes/iobuf.h"
#include "kafka/errors.h"
#include "kafka/requests/request_context.h"
#include "kafka/requests/response.h"
#include "kafka/requests/schemata/offset_fetch_request.h"
#include "kafka/requests/schemata/offset_fetch_response.h"
#include "kafka/types.h"
#include "model/fundamental.h"
#include "seastarx.h"

#include <seastar/core/future.hh>

namespace kafka {

struct offset_fetch_response;

class offset_fetch_api final {
public:
    using response_type = offset_fetch_response;

    static constexpr const char* name = "offset fetch";
    static constexpr api_key key = api_key(9);

    // in version 0 kafka stores offsets in zookeeper. if we ever need to
    // support version 0 then we need to do some code review to see if this has
    // any implications on semantics.
    static constexpr api_version min_supported = api_version(1);
    static constexpr api_version max_supported = api_version(4);

    static ss::future<response_ptr>
    process(request_context&&, ss::smp_service_group);
};

struct offset_fetch_request final {
    using api_type = offset_fetch_api;

    offset_fetch_request_data data;

    // set during request processing after mapping group to ntp
    model::ntp ntp;

    void encode(response_writer& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(request_reader& reader, api_version version) {
        data.decode(reader, version);
    }
};

std::ostream& operator<<(std::ostream&, const offset_fetch_request&);

struct offset_fetch_response final {
    using api_type = offset_fetch_api;

    offset_fetch_response_data data;

    offset_fetch_response() = default;

    offset_fetch_response(error_code error) { data.error_code = error; }

    offset_fetch_response(const offset_fetch_request&, error_code error)
      : offset_fetch_response(error) {}

    offset_fetch_response(
      std::optional<std::vector<offset_fetch_request_topic>>& topics) {
        data.error_code = error_code::none;
        if (topics) {
            for (auto& topic : *topics) {
                std::vector<offset_fetch_response_partition> partitions;
                for (auto id : topic.partition_indexes) {
                    offset_fetch_response_partition p = {
                      .partition_index = id,
                      .committed_offset = model::offset(-1),
                      .metadata = "",
                      .error_code = error_code::none,
                    };
                    partitions.push_back(std::move(p));
                }
                offset_fetch_response_topic t = {
                  .name = topic.name,
                  .partitions = std::move(partitions),
                };
                data.topics.push_back(std::move(t));
            }
        }
    }

    void encode(const request_context& ctx, response& resp) {
        data.encode(resp.writer(), ctx.header().version);
    }

    void decode(iobuf buf, api_version version) {
        data.decode(std::move(buf), version);
    }
};

std::ostream& operator<<(std::ostream&, const offset_fetch_response&);

} // namespace kafka
