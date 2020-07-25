#include "kafka/requests/describe_groups_request.h"

#include "cluster/namespace.h"
#include "kafka/errors.h"
#include "kafka/requests/request_context.h"
#include "kafka/requests/response.h"
#include "resource_mgmt/io_priority.h"

namespace kafka {

struct describe_groups_ctx {
    request_context rctx;
    describe_groups_request request;
    describe_groups_response response;
    ss::smp_service_group ssg;

    describe_groups_ctx(
      request_context&& rctx,
      describe_groups_request&& request,
      ss::smp_service_group ssg)
      : rctx(std::move(rctx))
      , request(std::move(request))
      , ssg(ssg) {}
};

ss::future<response_ptr>
describe_groups_api::process(request_context&& ctx, ss::smp_service_group ssg) {
    describe_groups_request request;
    request.decode(ctx.reader(), ctx.header().version);
    klog.trace("Handling request {}", request);

    return ss::do_with(
      describe_groups_ctx(std::move(ctx), std::move(request), ssg),
      [](describe_groups_ctx& octx) {
          return ss::parallel_for_each(
                   octx.request.data.groups.begin(),
                   octx.request.data.groups.end(),
                   [&octx](kafka::group_id id) {
                       return octx.rctx.groups()
                         .describe_group(std::move(id))
                         .then([&octx](described_group g) {
                             octx.response.data.groups.push_back(std::move(g));
                         });
                   })
            .then(
              [&octx] { return octx.rctx.respond(std::move(octx.response)); });
      });
}

} // namespace kafka
