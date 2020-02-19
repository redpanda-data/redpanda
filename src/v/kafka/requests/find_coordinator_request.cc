#include "kafka/requests/find_coordinator_request.h"

#include "kafka/errors.h"
#include "model/metadata.h"

#include <seastar/util/log.hh>

#include <fmt/ostream.h>

#include <string_view>

namespace kafka {

void find_coordinator_request::encode(
  response_writer& writer, api_version version) {
    writer.write(key);
    if (version >= api_version(1)) {
        writer.write(int8_t(key_type));
    }
}

void find_coordinator_request::decode(request_context& ctx) {
    const auto version = ctx.header().version;

    key = ctx.reader().read_string();
    if (version >= api_version(1)) {
        key_type = coordinator_type(ctx.reader().read_int8());
    }
}

void find_coordinator_response::encode(
  const request_context& ctx, response& resp) {
    auto& writer = resp.writer();
    const auto version = ctx.header().version;

    if (version >= api_version(1)) {
        writer.write(int32_t(throttle.count()));
    }
    writer.write(error);
    if (version >= api_version(1)) {
        writer.write(error_message);
    }
    writer.write(int32_t(node()));
    writer.write(host);
    writer.write(port);
}

void find_coordinator_response::decode(iobuf buf, api_version version) {
    request_reader reader(std::move(buf));

    if (version >= api_version(1)) {
        throttle = std::chrono::milliseconds(reader.read_int32());
    }
    error = error_code(reader.read_int16());
    if (version >= api_version(1)) {
        error_message = reader.read_nullable_string();
    }
    node = model::node_id(reader.read_int32());
    host = reader.read_string();
    port = reader.read_int32();
}

ss::future<response_ptr>
find_coordinator_api::process(request_context&& ctx, ss::smp_service_group g) {
    find_coordinator_request request(ctx);

    if (request.key_type != coordinator_type::group) {
        return ctx.respond(
          find_coordinator_response(error_code::unsupported_version));
    }

    /*
     * TODO 1: create the group membership internal topic if it doesn't exist
     *
     * if there is a problem creating the topic
     * return ctx.respond(
     *   find_coordinator_response(error_code::coordinator_not_available));
     */

    /*
     * TODO 2: hash the group id to the target partition
     *
     * The set of partition will be available either from the metadata cache or
     * from the response to the create topics rpc handled by the controller.
     */

    /*
     * TODO 3: return the leader for the target partition
     *
     * Assuming that we do not wait for leadership election for each partition
     * after creating the topic return any host and let the client use the
     * normal retry protocol to wait for a leader.  When the leader can't be
     * determined, can return error_code::coordinator_not_available.
     */

    return ctx.respond(
      find_coordinator_response(model::node_id(0), "localhost", 9092));
}

} // namespace kafka
