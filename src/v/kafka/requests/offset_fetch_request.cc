#include "kafka/requests/offset_fetch_request.h"

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

void offset_fetch_request::encode(
  response_writer& writer, api_version version) {
    writer.write(group_id());
    writer.write_nullable_array(
      topics, [version](topic& topic, response_writer& writer) {
          writer.write(topic.name);
          writer.write_array(
            topic.partitions,
            [version](model::partition_id id, response_writer& writer) {
                writer.write(id);
            });
      });
}

void offset_fetch_request::decode(request_context& ctx) {
    const auto version = ctx.header().version;
    auto& reader = ctx.reader();

    group_id = kafka::group_id(reader.read_string());
    topics = reader.read_nullable_array([version](request_reader& reader) {
        return topic{
          .name = model::topic(reader.read_string()),
          .partitions = reader.read_array([version](request_reader& reader) {
              return model::partition_id(reader.read_int32());
          }),
        };
    });
}

void offset_fetch_response::encode(const request_context& ctx, response& resp) {
    auto& writer = resp.writer();
    const auto version = ctx.header().version;

    if (version >= api_version(3)) {
        writer.write(int32_t(throttle_time_ms.count()));
    }
    writer.write_array(
      topics, [version](topic& topic, response_writer& writer) {
          writer.write(topic.name);
          writer.write_array(
            topic.partitions,
            [version](partition& partition, response_writer& writer) {
                writer.write(partition.id);
                writer.write(partition.offset);
                writer.write(partition.metadata);
                writer.write(partition.error);
            });
      });
    writer.write(error);
}

void offset_fetch_response::decode(iobuf buf, api_version version) {
    request_reader reader(std::move(buf));

    if (version >= api_version(3)) {
        throttle_time_ms = std::chrono::milliseconds(reader.read_int32());
    }
    topics = reader.read_array([version](request_reader& reader) {
        auto name = model::topic(reader.read_string());
        auto partitions = reader.read_array([version](request_reader& reader) {
            partition p;
            p.id = model::partition_id(reader.read_int32());
            p.offset = model::offset(reader.read_int64());
            p.metadata = reader.read_nullable_string();
            p.error = error_code(reader.read_int16());
            return p;
        });
        return topic{std::move(name), std::move(partitions)};
    });
    error = error_code(reader.read_int16());
}

struct offset_fetch_ctx {
    request_context rctx;
    ss::smp_service_group ssg;
    offset_fetch_request request;

    offset_fetch_ctx(
      request_context&& rctx,
      offset_fetch_request&& request,
      ss::smp_service_group ssg)
      : rctx(std::move(rctx))
      , request(std::move(request))
      , ssg(ssg) {}
};

ss::future<response_ptr>
offset_fetch_api::process(request_context&& ctx, ss::smp_service_group ssg) {
    offset_fetch_request request(ctx);
    return ss::do_with(
      offset_fetch_ctx(std::move(ctx), std::move(request), ssg),
      [](offset_fetch_ctx& octx) {
          return octx.rctx.groups()
            .offset_fetch(std::move(octx.request))
            .then([&octx](offset_fetch_response resp) {
                return octx.rctx.respond(std::move(resp));
            });
      });
}

} // namespace kafka
