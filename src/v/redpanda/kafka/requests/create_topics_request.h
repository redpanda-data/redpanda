#pragma once

#include "redpanda/kafka/requests/headers.h"
#include "redpanda/kafka/requests/request_context.h"
#include "redpanda/kafka/requests/response.h"
#include "redpanda/kafka/requests/topics/types.h"
#include "redpanda/kafka/requests/topics/validators.h"
#include "seastarx.h"

#include <seastar/core/future.hh>

#include <unordered_map>

namespace kafka {

class create_topics_api final {
public:
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
    static create_topics_request decode(request_context&);

    static new_topic_configuration read_topic_configuration(request_reader&);

    static std::vector<partition_assignment>
    read_partiton_assignments(request_reader&);

    static partition_assignment read_partiton_assignment(request_reader&);

    static model::node_id read_node_id(request_reader&);

    static std::unordered_map<sstring, sstring> read_config(request_reader&);

    std::vector<new_topic_configuration> topics;
    std::chrono::milliseconds timeout;
    bool validate_only;
};

} // namespace kafka
