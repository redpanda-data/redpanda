#include "kafka/requests/alter_configs_request.h"

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

ss::future<response_ptr> alter_configs_api::process(
  request_context&& ctx, [[maybe_unused]] ss::smp_service_group ssg) {
    alter_configs_request request;
    request.decode(ctx.reader(), ctx.header().version);
    klog.trace("Handling request {}", request);

    return ss::do_with(
      std::move(ctx),
      std::move(request),
      [](request_context& ctx, alter_configs_request&) {
          return ctx.respond(alter_configs_response());
      });
}

} // namespace kafka
