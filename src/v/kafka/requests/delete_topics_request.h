#pragma once

#include "bytes/iobuf.h"
#include "kafka/errors.h"
#include "kafka/requests/fwd.h"
#include "kafka/requests/schemata/delete_topics_request.h"
#include "kafka/requests/schemata/delete_topics_response.h"
#include "kafka/types.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "seastarx.h"

#include <seastar/core/future.hh>

namespace kafka {

struct delete_topics_response;

struct delete_topics_api final {
    using response_type = delete_topics_response;

    static constexpr const char* name = "delete topics";
    static constexpr api_key key = api_key(20);
    static constexpr api_version min_supported = api_version(0);
    static constexpr api_version max_supported = api_version(3);

    static ss::future<response_ptr>
    process(request_context&&, ss::smp_service_group);
};

struct delete_topics_request final {
    using api_type = delete_topics_api;

    delete_topics_request_data data;

    void encode(response_writer& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(request_reader& reader, api_version version) {
        data.decode(reader, version);
    }
};

static inline std::ostream&
operator<<(std::ostream& os, const delete_topics_request& r) {
    return os << r.data;
}

struct delete_topics_response final {
    using api_type = delete_topics_api;

    delete_topics_response_data data;

    void encode(const request_context& ctx, response& resp) {
        data.encode(ctx, resp);
    }

    void decode(iobuf buf, api_version version) {
        data.decode(std::move(buf), version);
    }
};

static inline std::ostream&
operator<<(std::ostream& os, const delete_topics_response& r) {
    return os << r.data;
}

} // namespace kafka
