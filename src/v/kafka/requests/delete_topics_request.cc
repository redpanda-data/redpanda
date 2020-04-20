#include "kafka/requests/delete_topics_request.h"

#include "kafka/errors.h"
#include "kafka/requests/request_context.h"
#include "kafka/requests/response.h"
#include "model/metadata.h"

#include <seastar/core/do_with.hh>
#include <seastar/core/smp.hh>
#include <seastar/util/log.hh>

#include <fmt/ostream.h>

#include <string_view>

namespace kafka {

struct delete_topics_ctx {
    request_context rctx;
    delete_topics_request request;
    ss::smp_service_group ssg;

    delete_topics_ctx(
      request_context&& rctx,
      delete_topics_request&& request,
      ss::smp_service_group ssg)
      : rctx(std::move(rctx))
      , request(std::move(request))
      , ssg(ssg) {}
};

ss::future<response_ptr>
delete_topics_api::process(request_context&& ctx, ss::smp_service_group ssg) {
    delete_topics_request request;
    request.decode(ctx.reader(), ctx.header().version);
    vlog(klog.trace, "Handling request {}", request);

    return ss::do_with(
      delete_topics_ctx(std::move(ctx), std::move(request), ssg),
      [](delete_topics_ctx& octx) {
          delete_topics_response resp;
          for (auto& topic : octx.request.data.topic_names) {
              resp.data.responses.push_back({
                .name = topic,
                .error_code = error_code::none,
              });
          }
          resp.data.throttle_time_ms = std::chrono::milliseconds(0);
          return octx.rctx.respond(std::move(resp));
      });
}

} // namespace kafka
