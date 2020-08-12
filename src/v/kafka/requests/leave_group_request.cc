#include "kafka/requests/leave_group_request.h"

#include "kafka/groups/group_manager.h"
#include "kafka/groups/group_router.h"
#include "kafka/requests/request_context.h"
#include "kafka/requests/response.h"
#include "utils/remote.h"
#include "utils/to_string.h"

#include <seastar/core/print.hh>

namespace kafka {

ss::future<response_ptr> leave_group_api::process(
  request_context&& ctx, [[maybe_unused]] ss::smp_service_group g) {
    return ss::do_with(
      remote(std::move(ctx)), [](remote<request_context>& remote_ctx) {
          auto& ctx = remote_ctx.get();
          leave_group_request request;
          request.decode(ctx.reader(), ctx.header().version);
          return ctx.groups()
            .leave_group(std::move(request))
            .then([&ctx](leave_group_response&& reply) {
                auto resp = std::make_unique<response>();
                reply.encode(ctx, *resp.get());
                return ss::make_ready_future<response_ptr>(std::move(resp));
            });
      });
}

} // namespace kafka
