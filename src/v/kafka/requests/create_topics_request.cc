#include "kafka/requests/create_topics_request.h"

#include "cluster/topics_frontend.h"
#include "kafka/errors.h"
#include "kafka/requests/fwd.h"
#include "kafka/requests/timeout.h"
#include "kafka/requests/topics/topic_result_utils.h"
#include "kafka/requests/topics/topic_utils.h"
#include "kafka/requests/topics/types.h"
#include "kafka/types.h"
#include "model/metadata.h"
#include "utils/to_string.h"

#include <seastar/core/future.hh>
#include <seastar/util/log.hh>

#include <fmt/ostream.h>

#include <chrono>
#include <string_view>

namespace kafka {

void create_topics_response::decode(iobuf buf, api_version version) {
    request_reader reader(std::move(buf));
    if (version >= api_version(2)) {
        throttle = std::chrono::milliseconds(reader.read_int32());
    }
    topics = reader.read_array([version](request_reader& reader) {
        auto t = topic{
          .name = model::topic(reader.read_string()),
          .error = error_code{reader.read_int16()},
        };
        if (version >= api_version(1)) {
            t.error_message = reader.read_nullable_string();
        }
        return t;
    });
}

static std::ostream&
operator<<(std::ostream& o, const create_topics_response::topic& t) {
    return ss::fmt_print(
      o, "name {} error {} error_msg {}", t.name, t.error, t.error_message);
}

std::ostream& operator<<(std::ostream& o, const create_topics_response& r) {
    return ss::fmt_print(o, "topics {}", r.topics);
}

ss::future<response_ptr> create_topics_api::process(
  request_context&& ctx, [[maybe_unused]] ss::smp_service_group g) {
    kafka::create_topics_request request;
    request.decode(ctx.reader(), ctx.header().version);
    return ss::do_with(
      std::move(ctx),
      [request = std::move(request)](request_context& ctx) mutable {
          std::vector<topic_op_result> results;
          auto begin = request.data.topics.begin();
          // Duplicated topic names are not accepted
          auto valid_range_end = validate_range_duplicates(
            begin, request.data.topics.end(), std::back_inserter(results));

          // Validate with validators
          valid_range_end = validate_requests_range(
            begin, valid_range_end, std::back_inserter(results), validators{});

          if (request.data.validate_only) {
              // We do not actually create the topics, only validate the
              // request
              // Generate successes for topics that passed the
              // validation.
              std::transform(
                begin,
                valid_range_end,
                std::back_inserter(results),
                [](const creatable_topic& t) {
                    return generate_successfull_result(t);
                });
              return ss::make_ready_future<response_ptr>(
                encode_response(ctx, results));
          }

          // Create the topics with controller on core 0
          return ctx.topics_frontend()
            .create_topics(
              to_cluster_type(begin, valid_range_end),
              to_timeout(request.data.timeout_ms))
            .then([&ctx,
                   results = std::move(results),
                   tout = to_timeout(request.data.timeout_ms)](
                    std::vector<cluster::topic_result> c_res) mutable {
                // Append controller results to validation errors
                append_cluster_results(c_res, results);
                return encode_response(ctx, std::move(results));
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
