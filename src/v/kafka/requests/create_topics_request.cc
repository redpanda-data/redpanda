#include "kafka/requests/create_topics_request.h"

#include "cluster/topics_frontend.h"
#include "kafka/errors.h"
#include "kafka/requests/fwd.h"
#include "kafka/requests/timeout.h"
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

ss::future<response_ptr> create_topics_api::process(
  request_context&& ctx, [[maybe_unused]] ss::smp_service_group g) {
    kafka::create_topics_request request;
    request.decode(ctx.reader(), ctx.header().version);
    return ss::do_with(
      std::move(ctx),
      [request = std::move(request)](request_context& ctx) mutable {
          create_topics_response response;
          auto begin = request.data.topics.begin();
          // Duplicated topic names are not accepted
          auto valid_range_end = validate_range_duplicates(
            begin,
            request.data.topics.end(),
            std::back_inserter(response.data.topics));

          // Validate with validators
          valid_range_end = validate_requests_range(
            begin,
            valid_range_end,
            std::back_inserter(response.data.topics),
            validators{});

          if (request.data.validate_only) {
              // We do not actually create the topics, only validate the
              // request
              // Generate successes for topics that passed the
              // validation.
              std::transform(
                begin,
                valid_range_end,
                std::back_inserter(response.data.topics),
                [](const creatable_topic& t) {
                    return generate_successfull_result(t);
                });
              return ctx.respond(std::move(response));
          }

          // Create the topics with controller on core 0
          return ctx.topics_frontend()
            .create_topics(
              to_cluster_type(begin, valid_range_end),
              to_timeout(request.data.timeout_ms))
            .then([&ctx,
                   response = std::move(response),
                   tout = to_timeout(request.data.timeout_ms)](
                    std::vector<cluster::topic_result> c_res) mutable {
                // Append controller results to validation errors
                append_cluster_results(c_res, response.data.topics);
                return ctx.respond(response);
            });
      });
}

} // namespace kafka
