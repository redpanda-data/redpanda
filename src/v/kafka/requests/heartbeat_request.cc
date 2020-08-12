#include "kafka/requests/heartbeat_request.h"

#include "kafka/groups/group_manager.h"
#include "kafka/groups/group_router.h"
#include "kafka/requests/request_context.h"
#include "kafka/requests/response.h"
#include "utils/remote.h"
#include "utils/to_string.h"

#include <seastar/core/print.hh>

namespace kafka {

ss::future<response_ptr> heartbeat_api::process(
  request_context&& ctx, [[maybe_unused]] ss::smp_service_group g) {
    return ss::do_with(std::move(ctx), [](request_context& ctx) {
        heartbeat_request request;
        request.decode(ctx.reader(), ctx.header().version);
        return ctx.groups()
          .heartbeat(std::move(request))
          .then([&ctx](heartbeat_response&& reply) {
              return ctx.respond(std::move(reply));
          });
    });
}

} // namespace kafka
