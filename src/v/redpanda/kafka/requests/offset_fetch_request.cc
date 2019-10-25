#include "redpanda/kafka/requests/offset_fetch_request.h"

#include "model/metadata.h"
#include "redpanda/kafka/errors/errors.h"

#include <seastar/util/log.hh>

#include <fmt/ostream.h>

#include <string_view>

namespace kafka {

future<response_ptr>
offset_fetch_api::process(request_context&& ctx, smp_service_group g) {
    // request
    auto group_id = ctx.reader().read_string();
    auto topics = ctx.reader().read_array([](request_reader& r) {
        auto topic = r.read_string();
        auto partitions = r.read_array(
          [](request_reader& r) { return r.read_int32(); });
        return std::make_pair(topic, partitions);
    });

    // response
    auto resp = std::make_unique<response>();

    if (ctx.header().version >= api_version(3)) {
        resp->writer().write(int32_t(0));
    }

    resp->writer().write_array(topics, [](const auto& t, response_writer& wr) {
        wr.write(t.first);
        wr.write_array(
          t.second, [](const int32_t partition, response_writer& wr) {
              std::optional<std::string_view> metadata;
              wr.write(partition);
              wr.write(int64_t(0));
              wr.write(metadata);
              wr.write(int16_t(0));
          });
    });

    if (ctx.header().version >= api_version(2)) {
        resp->writer().write(int16_t(0));
    }

    return make_ready_future<response_ptr>(std::move(resp));
}

} // namespace kafka
