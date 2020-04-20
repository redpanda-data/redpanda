#include "kafka/requests/sync_group_request.h"

#include "kafka/requests/request_context.h"
#include "kafka/requests/response.h"
#include "utils/remote.h"
#include "utils/to_string.h"

#include <seastar/core/print.hh>

namespace kafka {

ss::future<response_ptr> sync_group_api::process(
  request_context&& ctx, [[maybe_unused]] ss::smp_service_group g) {
    return ss::do_with(std::move(ctx), [](request_context& ctx) {
        sync_group_request request;
        request.decode(ctx.reader(), ctx.header().version);
        return ctx.groups()
          .sync_group(std::move(request))
          .then([&ctx](sync_group_response&& reply) {
              return ctx.respond(std::move(reply));
          });
    });
}

} // namespace kafka
