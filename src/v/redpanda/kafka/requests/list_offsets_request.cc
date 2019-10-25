#include "redpanda/kafka/requests/list_offsets_request.h"

#include "model/metadata.h"
#include "redpanda/kafka/errors/errors.h"

#include <seastar/util/log.hh>

#include <fmt/ostream.h>

#include <string_view>

namespace kafka::requests {

struct partition {
    int32_t id;
    int64_t timestamp;
};

struct topic {
    sstring name;
    std::vector<partition> partitions;
};

future<response_ptr>
list_offsets_api::process(request_context&& ctx, smp_service_group g) {
    auto replica_id = ctx.reader().read_int32();
    auto topics = ctx.reader().read_array([&ctx](request_reader& r) {
        auto name = r.read_string();
        auto partitions = r.read_array([&ctx](request_reader& r) {
            auto id = r.read_int32();
            auto timestamp = r.read_int64();
            return partition{id, timestamp};
        });
        return topic{name, partitions};
    });

    auto resp = std::make_unique<response>();

    resp->writer().write_array(
      topics, [&ctx](const auto& topic, response_writer& wr) {
          wr.write(topic.name);
          wr.write_array(
            topic.partitions,
            [&ctx](const auto& partition, response_writer& wr) {
                wr.write(partition.id);
                wr.write(errors::error_code::none);
                wr.write(int64_t(0));
                wr.write(int64_t(0));
            });
      });

    return make_ready_future<response_ptr>(std::move(resp));
}

} // namespace kafka::requests
