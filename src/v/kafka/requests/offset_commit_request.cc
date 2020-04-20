#include "kafka/requests/offset_commit_request.h"

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

struct offset_commit_ctx {
    request_context rctx;
    offset_commit_request request;
    ss::smp_service_group ssg;

    offset_commit_ctx(
      request_context&& rctx,
      offset_commit_request&& request,
      ss::smp_service_group ssg)
      : rctx(std::move(rctx))
      , request(std::move(request))
      , ssg(ssg) {}
};

ss::future<response_ptr>
offset_commit_api::process(request_context&& ctx, ss::smp_service_group ssg) {
    offset_commit_request request;
    request.decode(ctx.reader(), ctx.header().version);
    klog.trace("Handling request {}", request);

    if (request.data.group_instance_id) {
        return ctx.respond(
          offset_commit_response(request, error_code::unsupported_version));
    }

    return ss::do_with(
      offset_commit_ctx(std::move(ctx), std::move(request), ssg),
      [](offset_commit_ctx& octx) {
          return octx.rctx.groups()
            .offset_commit(std::move(octx.request))
            .then([&octx](offset_commit_response resp) {
                return octx.rctx.respond(std::move(resp));
            });
      });
}

} // namespace kafka
