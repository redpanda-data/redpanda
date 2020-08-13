#pragma once

#include "kafka/requests/request_context.h"
#include "kafka/requests/response.h"
#include "kafka/requests/schemata/create_topics_request.h"
#include "kafka/requests/schemata/create_topics_response.h"
#include "kafka/requests/topics/types.h"
#include "kafka/requests/topics/validators.h"
#include "seastarx.h"

#include <seastar/core/future.hh>

#include <unordered_map>

namespace kafka {

struct create_topics_response;

class create_topics_api final {
public:
    using response_type = create_topics_response;

    static constexpr const char* name = "create topics";
    static constexpr api_key key = api_key(19);
    static constexpr api_version min_supported = api_version(0);
    static constexpr api_version max_supported = api_version(3);

    static ss::future<response_ptr>
    process(request_context&&, ss::smp_service_group);

private:
    using validators = make_validator_types<
      creatable_topic,
      no_custom_partition_assignment,
      partition_count_must_be_positive,
      replication_factor_must_be_positive,
      replication_factor_must_be_odd,
      unsupported_configuration_entries>;

    static response_ptr
    encode_response(request_context&, std::vector<topic_op_result> errs);
};

struct create_topics_request final {
    using api_type = create_topics_api;

    create_topics_request_data data;

    void encode(response_writer& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(request_reader& reader, api_version version) {
        data.decode(reader, version);
    }
};

inline std::ostream&
operator<<(std::ostream& os, const create_topics_request& r) {
    return os << r.data;
}

struct create_topics_response final {
    struct topic {
        model::topic name;
        error_code error;
        std::optional<ss::sstring> error_message; // >= v1
    };

    std::chrono::milliseconds throttle = std::chrono::milliseconds(0); // >= v2
    std::vector<topic> topics;

    void decode(iobuf buf, api_version version);
};

std::ostream& operator<<(std::ostream&, const create_topics_response&);

} // namespace kafka
