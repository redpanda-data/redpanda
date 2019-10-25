#include "redpanda/kafka/requests/list_groups_request.h"

#include "model/metadata.h"
#include "redpanda/kafka/errors/errors.h"

#include <seastar/util/log.hh>

#include <fmt/ostream.h>

#include <string_view>

namespace kafka::requests {

future<response_ptr>
list_groups_api::process(request_context&& ctx, smp_service_group g) {
    auto resp = std::make_unique<response>();
    if (ctx.header().version >= api_version(1)) {
        resp->writer().write(int32_t(0));
    }
    resp->writer().write(error_code::none);
    resp->writer().write_array(
      std::vector<int>{0}, [](int v, response_writer& wr) {
          wr.write("fake_group_id");
          wr.write("fake_protocol_type");
      });
    return make_ready_future<response_ptr>(std::move(resp));
}

} // namespace kafka::requests
