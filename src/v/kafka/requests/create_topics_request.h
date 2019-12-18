#pragma once

#include "kafka/requests/request_context.h"
#include "kafka/requests/response.h"
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

    static future<response_ptr> process(request_context&&, smp_service_group);

private:
    using validators = make_validator_types<
      new_topic_configuration,
      no_custom_partition_assignment,
      partition_count_must_be_positive,
      replication_factor_must_be_positive,
      unsupported_configuration_entries>;

    static response_ptr
    encode_response(request_context&, std::vector<topic_op_result> errs);
};

struct create_topics_request {
    using api_type = create_topics_api;

    static create_topics_request decode(request_context&);

    static new_topic_configuration read_topic_configuration(request_reader&);

    static std::vector<partition_assignment>
    read_partiton_assignments(request_reader&);

    static partition_assignment read_partiton_assignment(request_reader&);

    static model::node_id read_node_id(request_reader&);

    std::vector<new_topic_configuration> topics;
    std::chrono::milliseconds timeout;
    bool validate_only; // >= v1

    void encode(response_writer& writer, api_version version);
};

struct create_topics_response final {
    struct topic {
        model::topic name;
        error_code error;
        std::optional<sstring> error_message; // >= v1
    };

    std::chrono::milliseconds throttle; // >= v2
    std::vector<topic> topics;

    void decode(iobuf buf, api_version version);
};

std::ostream& operator<<(std::ostream&, const create_topics_response&);

} // namespace kafka
