#include "redpanda/kafka/requests/join_group_request.h"

#include "model/metadata.h"
#include "redpanda/kafka/errors/errors.h"
#include "redpanda/kafka/requests/request_context.h"
#include "utils/remote.h"

#include <seastar/util/log.hh>

#include <fmt/ostream.h>

#include <string_view>

namespace kafka::requests {

void join_group_request::decode(request_context& ctx) {
    auto& reader = ctx.reader();

    version = ctx.header().version;
    if (ctx.header().client_id) {
        client_id = sstring(*ctx.header().client_id);
    }
    group_id = kafka::group_id(reader.read_string());
    session_timeout = std::chrono::milliseconds(reader.read_int32());
    if (version >= api_version(1)) {
        rebalance_timeout = std::chrono::milliseconds(reader.read_int32());
    } else {
        rebalance_timeout = session_timeout;
    }
    member_id = kafka::member_id(reader.read_string());
    if (version >= api_version(5)) {
        auto id = reader.read_nullable_string();
        if (id) {
            group_instance_id = kafka::group_instance_id(std::move(*id));
        }
    }
    protocol_type = kafka::protocol_type(reader.read_string());
    protocols = reader.read_array([](request_reader& reader) {
        auto name = kafka::protocol_name(reader.read_string());
        auto metadata = reader.read_bytes();
        return protocol_config{name, metadata};
    });
}

std::ostream& operator<<(std::ostream& o, const join_group_request& r) {
    return o;
}

void join_group_response::encode(const request_context& ctx, response& resp) {
    auto& writer = resp.writer();
    auto version = ctx.header().version;

    if (version >= api_version(2)) {
        writer.write(int32_t(throttle_time.count()));
    }
    writer.write(error);
    writer.write(generation_id);
    writer.write(protocol_name());
    writer.write(leader_id());
    writer.write(member_id());
    writer.write_array(
      members,
      [this, version](const member_config& m, response_writer& writer) {
          writer.write(m.member_id());
          if (version >= api_version(2)) {
              writer.write(m.group_instance_id);
          }
          writer.write(bytes_view(m.metadata));
      });
}

future<response_ptr>
join_group_request::process(request_context&& ctx, smp_service_group g) {
    return do_with(
      remote(std::move(ctx)), [g](remote<request_context>& remote_ctx) {
          auto& ctx = remote_ctx.get();
          join_group_request request;
          request.decode(ctx);
          return ctx.groups()
            .join_group(std::move(request))
            .then([&ctx](join_group_response&& reply) {
                auto resp = std::make_unique<response>();
                reply.encode(ctx, *resp.get());
                return make_ready_future<response_ptr>(std::move(resp));
            });
      });
}

} // namespace kafka::requests
