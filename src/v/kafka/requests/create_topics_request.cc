#include "kafka/requests/create_topics_request.h"

#include "kafka/controller_dispatcher.h"
#include "kafka/default_namespace.h"
#include "kafka/errors.h"
#include "kafka/requests/timeout.h"
#include "kafka/requests/topics/topic_result_utils.h"
#include "kafka/requests/topics/topic_utils.h"
#include "model/metadata.h"

#include <seastar/util/log.hh>

#include <fmt/ostream.h>

#include <string_view>

namespace kafka {

create_topics_request create_topics_request::decode(request_context& ctx) {
    return create_topics_request{
      .topics = ctx.reader().read_array(
        &create_topics_request::read_topic_configuration),
      .timeout = std::chrono::milliseconds(ctx.reader().read_int32()),
      .validate_only = ctx.header().version > api_version(0)
                         ? ctx.reader().read_bool()
                         : false};
} // namespace kafka

new_topic_configuration
create_topics_request::read_topic_configuration(request_reader& r) {
    return new_topic_configuration{
      .topic = model::topic_view(r.read_string_view()),
      .partition_count = r.read_int32(),
      .replication_factor = r.read_int16(),
      .assignments = read_partiton_assignments(r),
      .config_entries = read_config(r)};
}

std::vector<partition_assignment>
create_topics_request::read_partiton_assignments(request_reader& r) {
    return r.read_array(&create_topics_request::read_partiton_assignment);
}

partition_assignment
create_topics_request::read_partiton_assignment(request_reader& r) {
    return partition_assignment{
      .partition = model::partition_id(r.read_int32()),
      .assignments = r.read_array(&create_topics_request::read_node_id)};
}

model::node_id create_topics_request::read_node_id(request_reader& r) {
    return model::node_id(r.read_int32());
}

std::unordered_map<sstring, sstring>
create_topics_request::read_config(request_reader& r) {
    auto len = r.read_int32();
    std::unordered_map<sstring, sstring> res;
    res.reserve(std::max(0, len));
    while (len-- > 0) {
        auto key = r.read_string();
        auto val = r.read_nullable_string();
        if (val) {
            res.emplace(std::move(key), std::move(*val));
        }
    }
    return res;
};

future<response_ptr>
create_topics_api::process(request_context&& ctx, smp_service_group g) {
    auto request = create_topics_request::decode(ctx);
    return do_with(
      std::move(ctx),
      [request = std::move(request)](request_context& ctx) mutable {
          return ctx.cntrl_dispatcher()
            .dispatch_to_controller(
              [](const cluster::controller& c) { return c.is_leader(); })
            .then([&ctx, request = std::move(request)](bool is_leader) mutable {
                std::vector<topic_op_result> results;
                auto begin = request.topics.begin();

                // Only allowed on the raft0 leader
                if (!is_leader) {
                    generate_not_controller_errors(
                      begin, request.topics.end(), std::back_inserter(results));
                    return make_ready_future<std::vector<topic_op_result>>(
                      std::move(results));
                }

                // Duplicated topic names are not accepted
                auto valid_range_end = validate_range_duplicates(
                  begin, request.topics.end(), std::back_inserter(results));

                // Validate with validators
                valid_range_end = validate_requests_range(
                  begin,
                  valid_range_end,
                  std::back_inserter(results),
                  validators{});

                if (request.validate_only) {
                    // We do not actually create the topics, only validate the
                    // request
                    // Generate successes for topics that passed the validation.
                    std::transform(
                      begin,
                      valid_range_end,
                      std::back_inserter(results),
                      [](const new_topic_configuration& t) {
                          return generate_successfull_result(t);
                      });
                    return make_ready_future<std::vector<topic_op_result>>(
                      std::move(results));
                }

                // Create the topics with controller on core 0
                return ctx.cntrl_dispatcher()
                  .dispatch_to_controller(
                    [to_create = to_cluster_type(begin, valid_range_end),
                     timeout = request.timeout](cluster::controller& c) {
                        return c.create_topics(to_create, to_timeout(timeout));
                    })
                  .then([results = std::move(results)](
                          std::vector<cluster::topic_result> c_res) mutable {
                      // Append controller results to validation errors
                      append_cluster_results(c_res, results);
                      return make_ready_future<std::vector<topic_op_result>>(
                        std::move(results));
                  });
            })
            .then([&ctx](std::vector<topic_op_result> errs) {
                // Encode response bytes
                return encode_response(ctx, std::move(errs));
            });
      });
}

response_ptr create_topics_api::encode_response(
  request_context& ctx, std::vector<topic_op_result> errs) {
    // Throttle time for api_version >= 2
    int32_t throttle_time_ms = -1;
    if (ctx.header().version >= api_version(2)) {
        throttle_time_ms = ctx.throttle_delay_ms();
    }
    // Error message for api_version >= 1
    auto include_msg = include_message::no;
    if (ctx.header().version >= api_version(1)) {
        include_msg = include_message::yes;
    }

    return encode_topic_results(errs, throttle_time_ms, include_msg);
}

} // namespace kafka
