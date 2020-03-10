#include "kafka/requests/find_coordinator_request.h"

#include "config/configuration.h"
#include "kafka/controller_dispatcher.h"
#include "kafka/errors.h"
#include "kafka/groups/coordinator_ntp_mapper.h"
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

static ss::future<response_ptr>
handle_leader(request_context& ctx, model::node_id leader) {
    auto broker = ctx.metadata_cache().get_broker(leader);
    if (broker) {
        auto b = *broker;
        return ctx.respond(find_coordinator_response(
          b->id(),
          b->kafka_api_address().host(),
          b->kafka_api_address().port()));
    } else {
        return ctx.respond(
          find_coordinator_response(error_code::coordinator_not_available));
    }
}

/*
 * map the ntp to it's leader broker connection information. also wait for the
 * leader to be elected if it isn't yet available immediately, like in the case
 * of creating the internal metadata topic on-demand.
 */
static ss::future<response_ptr>
handle_ntp(request_context& ctx, std::optional<model::ntp> ntp) {
    if (!ntp) {
        return ctx.respond(
          find_coordinator_response(error_code::coordinator_not_available));
    }

    auto timeout = ss::lowres_clock::now()
                   + config::shard_local_cfg().wait_for_leader_timeout_ms();

    return ctx.metadata_cache()
      .get_leader(std::move(*ntp), timeout)
      .then(
        [&ctx](model::node_id leader) { return handle_leader(ctx, leader); })
      .handle_exception([&ctx](std::exception_ptr e) {
          return ctx.respond(
            find_coordinator_response(error_code::coordinator_not_available));
      });
}

/*
 * create the internal metadata topic for group membership
 */
static ss::future<error_code>
create_topic(request_context& ctx, cluster::topic_configuration topic) {
    // route request through controller home core
    return ctx.cntrl_dispatcher().dispatch_to_controller(
      [topic = std::move(topic)](cluster::controller& c) mutable {
          auto timeout = ss::lowres_clock::now()
                         + config::shard_local_cfg().create_topic_timeout_ms();

          return c.autocreate_topics({std::move(topic)}, timeout)
            .then([](std::vector<cluster::topic_result> res) {
                /*
                 * kindly ask client to retry on error
                 */
                vassert(res.size() == 1, "expected exactly one result");
                if (res[0].ec != cluster::errc::success) {
                    return error_code::coordinator_not_available;
                }
                return error_code::none;
            })
            .handle_exception([](std::exception_ptr e) {
                // various errors may returned such as a timeout, or if the
                // controller group doesn't have a leader. client will retry.
                return error_code::coordinator_not_available;
            });
      });
}

ss::future<response_ptr>
find_coordinator_api::process(request_context&& ctx, ss::smp_service_group g) {
    find_coordinator_request request(ctx);

    // other types include txn coordinators which are unsupported
    if (request.key_type != coordinator_type::group) {
        return ctx.respond(
          find_coordinator_response(error_code::unsupported_version));
    }

    return ss::do_with(
      std::move(ctx),
      [request = std::move(request)](request_context& ctx) mutable {
          /*
           * map the group to a target ntp. this may fail because the internal
           * metadata topic doesn't exist. in this case fall through and create
           * the topic on-demand.
           */
          if (auto ntp = ctx.coordinator_mapper().local().ntp_for(
                kafka::group_id(request.key));
              ntp) {
              return handle_ntp(ctx, std::move(ntp));
          }

          // the new internal metadata topic for group membership
          cluster::topic_configuration topic{
            ctx.coordinator_mapper().local().ns(),
            ctx.coordinator_mapper().local().topic(),
            config::shard_local_cfg().group_topic_partitions(),
            config::shard_local_cfg().default_topic_replication()};

          return create_topic(ctx, std::move(topic))
            .then([&ctx, request = std::move(request)](error_code error) {
                /*
                 * if the topic is successfully created then the metadata cache
                 * will be updated and we can retry the group-ntp mapping.
                 */
                if (error == error_code::none) {
                    auto ntp = ctx.coordinator_mapper().local().ntp_for(
                      kafka::group_id(request.key));
                    return handle_ntp(ctx, std::move(ntp));
                }
                return ctx.respond(find_coordinator_response(error));
            });
      });
}

} // namespace kafka
