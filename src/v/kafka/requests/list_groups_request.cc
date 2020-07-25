#include "kafka/requests/list_groups_request.h"

#include "kafka/errors.h"
#include "kafka/requests/request_context.h"
#include "model/metadata.h"

namespace kafka {

ss::future<response_ptr> list_groups_api::process(
  request_context&& ctx, [[maybe_unused]] ss::smp_service_group g) {
    list_groups_request request{};
    request.decode(ctx.reader(), ctx.header().version);
    klog.trace("Handling request {}", request);

    return ss::do_with(std::move(ctx), [](request_context& ctx) mutable {
        return ctx.groups().list_groups().then(
          [&ctx](std::pair<bool, std::vector<listed_group>> g) {
              // group listing is still returned even if some partitions are
              // still in the process of loading/recovering.
              list_groups_response resp;
              resp.data.error_code
                = g.first ? error_code::coordinator_load_in_progress
                          : error_code::none;
              resp.data.groups = std::move(g.second);
              return ctx.respond(std::move(resp));
          });
    });
}

} // namespace kafka
