#include "kafka/requests/offset_commit_request.h"

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

void offset_commit_request::encode(
  response_writer& writer, api_version version) {
    writer.write(group_id());
    writer.write(generation_id);
    writer.write(member_id());
    if (version >= api_version(7)) {
        writer.write(group_instance_id);
    }
    writer.write_array(
      topics, [version](topic& topic, response_writer& writer) {
          writer.write(topic.name);
          writer.write_array(
            topic.partitions,
            [version](partition& partition, response_writer& writer) {
                writer.write(partition.id);
                writer.write(partition.committed);
                if (version >= api_version(6)) {
                    writer.write(partition.leader_epoch);
                }
                writer.write(partition.metadata);
            });
      });
}

void offset_commit_request::decode(request_context& ctx) {
    const auto version = ctx.header().version;
    auto& reader = ctx.reader();

    group_id = kafka::group_id(reader.read_string());
    generation_id = kafka::generation_id(reader.read_int32());
    member_id = kafka::member_id(reader.read_string());
    if (version >= api_version(7)) {
        auto tmp = reader.read_nullable_string();
        if (tmp) {
            group_instance_id = kafka::group_instance_id(*tmp);
        }
    }
    topics = reader.read_array([version](request_reader& reader) {
        return topic{
          .name = model::topic(reader.read_string()),
          .partitions = reader.read_array([version](request_reader& reader) {
              auto id = model::partition_id(reader.read_int32());
              auto committed = model::offset(reader.read_int64());
              auto p = partition{
                .id = id,
                .committed = committed,
              };
              if (version >= api_version(6)) {
                  p.leader_epoch = reader.read_int32();
              }
              p.metadata = reader.read_nullable_string();
              return p;
          }),
        };
    });
}

void offset_commit_response::encode(
  const request_context& ctx, response& resp) {
    auto& writer = resp.writer();
    const auto version = ctx.header().version;

    writer.write(int32_t(throttle_time_ms.count()));
    writer.write_array(
      topics, [version](topic& topic, response_writer& writer) {
          writer.write(topic.name);
          writer.write_array(
            topic.partitions,
            [version](partition& partition, response_writer& writer) {
                writer.write(partition.id);
                writer.write(partition.error);
            });
      });
}

void offset_commit_response::decode(iobuf buf, api_version version) {
    request_reader reader(std::move(buf));

    throttle_time_ms = std::chrono::milliseconds(reader.read_int32());
    topics = reader.read_array([version](request_reader& reader) {
        auto name = model::topic(reader.read_string());
        auto partitions = reader.read_array([version](request_reader& reader) {
            auto id = model::partition_id(reader.read_int32());
            auto error = error_code(reader.read_int16());
            return partition{id, error};
        });
        return topic{std::move(name), std::move(partitions)};
    });
}

struct offset_commit_ctx {
    request_context rctx;
    ss::smp_service_group ssg;
    offset_commit_request request;

    offset_commit_ctx(
      request_context&& rctx,
      offset_commit_request&& request,
      ss::smp_service_group ssg)
      : rctx(std::move(rctx))
      , request(std::move(request))
      , ssg(ssg) {}
};

ss::future<response_ptr>
offset_commit_api::process(request_context&& ctx, ss::smp_service_group ssg) {
    offset_commit_request request(ctx);

    if (request.group_instance_id) {
        return ctx.respond(
          offset_commit_response(request, error_code::unsupported_version));
    }

    return ss::do_with(
      offset_commit_ctx(std::move(ctx), std::move(request), ssg),
      [](offset_commit_ctx& octx) {
          return octx.rctx.groups()
            .offset_commit(std::move(octx.request))
            .then([&octx](offset_commit_response resp) {
                return octx.rctx.respond(std::move(resp));
            });
      });
}

} // namespace kafka
