#include "kafka/requests/join_group_request.h"

#include "kafka/requests/request_context.h"
#include "kafka/requests/response.h"
#include "utils/remote.h"
#include "utils/to_string.h"

namespace kafka {

std::ostream& operator<<(std::ostream& o, const member_protocol& p) {
    return ss::fmt_print(o, "{}:{}", p.name, p.metadata.size());
}

void join_group_request::decode(request_context& ctx) {
    auto& reader = ctx.reader();

    version = ctx.header().version;
    if (ctx.header().client_id) {
        client_id = ss::sstring(*ctx.header().client_id);
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
        return member_protocol{name, metadata};
    });
}

void join_group_request::encode(
  const request_context& ctx, response_writer& writer) {
    auto version = ctx.header().version;

    writer.write(group_id());
    writer.write(int32_t(session_timeout.count()));
    if (version >= api_version(1)) {
        writer.write(int32_t(rebalance_timeout.count()));
    }
    writer.write(member_id());
    if (version >= api_version(5)) {
        writer.write(group_instance_id);
    }
    writer.write(protocol_type());
    writer.write_array(
      protocols, [](const member_protocol& p, response_writer& writer) {
          writer.write(p.name());
          writer.write(bytes_view(p.metadata));
      });
}

std::ostream& operator<<(std::ostream& o, const join_group_request& r) {
    return ss::fmt_print(
      o,
      "group={} member={} group_inst={} proto_type={} timeout={}/{} v{} "
      "protocols={}",
      r.group_id,
      r.member_id,
      r.group_instance_id,
      r.protocol_type,
      r.session_timeout,
      r.rebalance_timeout,
      r.version(),
      r.protocols);
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
          if (version >= api_version(5)) {
              writer.write(m.group_instance_id);
          }
          writer.write(bytes_view(m.metadata));
      });
}

std::ostream&
operator<<(std::ostream& o, const join_group_response::member_config& m) {
    return ss::fmt_print(
      o, "{}:{}:{}", m.member_id, m.group_instance_id, m.metadata.size());
}

std::ostream& operator<<(std::ostream& o, const join_group_response& r) {
    return ss::fmt_print(
      o,
      "error={} gen={} proto_name={} leader={} member={} members={}",
      r.error,
      r.generation_id,
      r.protocol_name,
      r.leader_id,
      r.member_id,
      r.members);
}

ss::future<response_ptr>
join_group_api::process(request_context&& ctx, ss::smp_service_group g) {
    join_group_request request(ctx);

    if (request.group_instance_id) {
        return ctx.respond(
          join_group_response(error_code::unsupported_version));
    }

    return ss::do_with(
      remote(std::move(ctx)),
      std::move(request),
      [g](remote<request_context>& remote_ctx, join_group_request& request) {
          auto& ctx = remote_ctx.get();
          return ctx.groups()
            .join_group(std::move(request))
            .then([&ctx](join_group_response&& reply) {
                return ctx.respond(std::move(reply));
            });
      });
}

} // namespace kafka
