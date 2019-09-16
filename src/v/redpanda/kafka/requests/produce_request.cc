#include "redpanda/kafka/requests/produce_request.h"

#include "model/metadata.h"
#include "redpanda/kafka/errors/errors.h"

#include <seastar/util/log.hh>

#include <fmt/ostream.h>

#include <string_view>

namespace kafka::requests {

struct partition_messages {
    int32_t partition;
    bytes_opt messages;
};

struct topic_data {
    sstring topic;
    std::vector<partition_messages> partitions;
};

// Notes
//  - it was observed with the kafka-python client that even though we informed
//  of a maximum supported produce api version of 2, it went ahead and send a
//  request with version 3 anyway.
future<response_ptr>
produce_request::process(request_context& ctx, smp_service_group g) {
    if (
      ctx.header().version < min_supported
      || ctx.header().version > max_supported) {
        return make_exception_future<response_ptr>(
          std::runtime_error(fmt::format(
            "Unsupported version {} for metadata API", ctx.header().version)));
    }

    std::optional<sstring> transactional_id;
    if (ctx.header().version >= api_version(3)) {
        transactional_id = ctx.reader().read_nullable_string();
    }

    auto acks = ctx.reader().read_int16();
    auto timeout = ctx.reader().read_int32();
    auto topics = ctx.reader().read_array([](request_reader& rd) {
        auto topic = rd.read_string();
        auto partitions = rd.read_array([](request_reader& rd) {
            return partition_messages{rd.read_int32(),
                                      rd.read_nullable_bytes()};
        });
        return topic_data{topic, partitions};
    });

    auto resp = std::make_unique<response>();

    // return a fake response for each topic/partition in the request
    resp->writer().write_array(
      topics, [&ctx](const auto& topic, response_writer& wr) {
          wr.write(topic.topic);
          wr.write_array(
            topic.partitions,
            [&ctx](const auto& partition, response_writer& wr) {
                wr.write(partition.partition);
                wr.write(errors::error_code::none);
                wr.write(int64_t(0));
                if (ctx.header().version >= api_version(2)) {
                    wr.write(int64_t(0));
                }
            });
      });

    if (ctx.header().version >= api_version(1)) {
        resp->writer().write(ctx.throttle_delay_ms());
    }

    return make_ready_future<response_ptr>(std::move(resp));
}

} // namespace kafka::requests
