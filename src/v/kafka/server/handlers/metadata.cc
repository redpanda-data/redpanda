// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/metadata.h"

#include "cluster/metadata_cache.h"
#include "cluster/topics_frontend.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "config/node_config.h"
#include "kafka/cluster_limits.h"
#include "kafka/server/errors.h"
#include "kafka/server/fwd.h"
#include "kafka/server/handlers/details/leader_epoch.h"
#include "kafka/server/handlers/details/security.h"
#include "kafka/server/handlers/topics/topic_utils.h"
#include "kafka/types.h"
#include "likely.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/timeout_clock.h"
#include "utils/to_string.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/thread.hh>

#include <boost/numeric/conversion/cast.hpp>
#include <fmt/ostream.h>

namespace kafka {

static constexpr model::node_id no_leader(-1);
/**
 * We use simple heuristic to tolerate isolation of a node hosting both
 * partition leader and follower.
 *
 * Kafka clients request metadata refresh in case they receive error that is
 * related with stale metadata - f.e. NOT_LEADER. Metadata request can be
 * processed by any broker and there is no general rule for that which
 * broker to choose to refresh metadata from. (f.e. Java kafka client uses the
 * broker with active least loaded connection.) This may lead to the situation
 * in which client will ask for metadata always the same broker. When that
 * broker is isolated from rest of the cluster it will never update its metadata
 * view. This way the client will always receive stale metadata.
 *
 * This behavior may lead to a live lock in an event of network partition. If
 * current partition leader is isolated from the cluster it will keep answering
 * with its id in the leader_id field for that partition (according to policy
 * where we return a former leader - there is no leader for that broker, it is a
 * candidate). Client will retry produce or fetch request and receive NOT_LEADER
 * error, this will force client to request metadata update, broker will respond
 * with the same metadata and the whole cycle will loop indefinitely.
 *
 * In order to break the loop and force client to make progress we use following
 * heuristics:
 *
 * 1) when current leader is unknown, return former leader (Kafka behavior)
 *
 * 2) when current leader is unknown and previous leader is equal to current
 *    node id select random replica_id as a leader (indicate leader isolation)
 *
 * With those heuristics we will always force the client to communicate with the
 * nodes that may not be partitioned.
 */
std::optional<cluster::leader_term> get_leader_term(
  model::topic_namespace_view tp_ns,
  model::partition_id p_id,
  const cluster::metadata_cache& md_cache,
  const std::vector<model::node_id>& replicas) {
    auto leader_term = md_cache.get_leader_term(tp_ns, p_id);
    if (!leader_term) {
        return std::nullopt;
    }
    if (!leader_term->leader.has_value()) {
        const auto previous = md_cache.get_previous_leader_id(tp_ns, p_id);
        leader_term->leader = previous;

        if (previous == config::node().node_id()) {
            auto idx = fast_prng_source() % replicas.size();
            leader_term->leader = replicas[idx];
        }
    }

    return leader_term;
}

namespace {
bool is_internal(const model::topic_namespace& tp_ns) {
    return tp_ns == model::kafka_consumer_offsets_nt;
}

} // namespace

metadata_response::topic make_topic_response_from_topic_metadata(
  const cluster::metadata_cache& md_cache, cluster::topic_metadata&& tp_md) {
    metadata_response::topic tp;
    tp.error_code = error_code::none;
    auto tp_ns = tp_md.get_configuration().tp_ns;
    tp.name = std::move(tp_md.get_configuration().tp_ns.tp);

    tp.is_internal = is_internal(tp_ns);
    std::transform(
      tp_md.get_assignments().begin(),
      tp_md.get_assignments().end(),
      std::back_inserter(tp.partitions),
      [tp_ns = std::move(tp_ns),
       &md_cache](cluster::partition_assignment& p_md) {
          std::vector<model::node_id> replicas{};
          replicas.reserve(p_md.replicas.size());
          std::transform(
            std::cbegin(p_md.replicas),
            std::cend(p_md.replicas),
            std::back_inserter(replicas),
            [](const model::broker_shard& bs) { return bs.node_id; });
          metadata_response::partition p;
          p.error_code = error_code::none;
          p.partition_index = p_md.id;
          p.leader_id = no_leader;
          auto lt = get_leader_term(tp_ns, p_md.id, md_cache, replicas);
          if (lt) {
              p.leader_id = lt->leader.value_or(no_leader);
              p.leader_epoch = leader_epoch_from_term(lt->term);
          }
          p.replica_nodes = std::move(replicas);
          p.isr_nodes = p.replica_nodes;
          p.offline_replicas = {};
          return p;
      });
    return tp;
}

static ss::future<metadata_response::topic>
create_topic(request_context& ctx, model::topic&& topic) {
    // default topic configuration
    cluster::topic_configuration cfg{
      model::kafka_namespace,
      topic,
      config::shard_local_cfg().default_topic_partitions(),
      config::shard_local_cfg().default_topic_replication()};
    auto tout = config::shard_local_cfg().create_topic_timeout_ms();
    return ctx.topics_frontend()
      .autocreate_topics({std::move(cfg)}, tout)
      .then([&ctx, &md_cache = ctx.metadata_cache(), tout](
              std::vector<cluster::topic_result> res) {
          vassert(res.size() == 1, "expected single result");

          // error, neither success nor topic exists
          if (!(res[0].ec == cluster::errc::success
                || res[0].ec == cluster::errc::topic_already_exists)) {
              metadata_response::topic t;
              t.name = std::move(res[0].tp_ns.tp);
              t.error_code = map_topic_error_code(res[0].ec);
              return ss::make_ready_future<metadata_response::topic>(t);
          }
          auto tp_md = md_cache.get_topic_metadata(res[0].tp_ns);

          if (!tp_md) {
              metadata_response::topic t;
              t.name = std::move(res[0].tp_ns.tp);
              t.error_code = error_code::invalid_topic_exception;
              return ss::make_ready_future<metadata_response::topic>(t);
          }

          return wait_for_topics(
                   res,
                   ctx.controller_api(),
                   tout + model::timeout_clock::now())
            .then([&ctx, tp_md = std::move(tp_md)]() mutable {
                return make_topic_response_from_topic_metadata(
                  ctx.metadata_cache(), std::move(tp_md.value()));
            });
      })
      .handle_exception([topic = std::move(topic)](
                          [[maybe_unused]] std::exception_ptr e) mutable {
          metadata_response::topic t;
          t.name = std::move(topic);
          t.error_code = error_code::request_timed_out;
          return t;
      });
}

metadata_response::topic
make_error_topic_response(model::topic tp, error_code ec) {
    return metadata_response::topic{.error_code = ec, .name = std::move(tp)};
}

static metadata_response::topic make_topic_response(
  request_context& ctx, metadata_request& rq, cluster::topic_metadata md) {
    int32_t auth_operations = 0;
    /**
     * if requested include topic authorized operations
     */
    if (rq.data.include_topic_authorized_operations) {
        auth_operations = details::to_bit_field(
          details::authorized_operations(ctx, md.get_configuration().tp_ns.tp));
    }

    auto res = make_topic_response_from_topic_metadata(
      ctx.metadata_cache(), std::move(md));
    res.topic_authorized_operations = auth_operations;
    return res;
}

static ss::future<std::vector<metadata_response::topic>>
get_topic_metadata(request_context& ctx, metadata_request& request) {
    std::vector<metadata_response::topic> res;

    // request can be served from whatever happens to be in the cache
    if (request.list_all_topics) {
        auto& topics_md = ctx.metadata_cache().all_topics_metadata();

        for (const auto& [tp_ns, md] : topics_md) {
            // only serve topics from the kafka namespace
            if (tp_ns.ns != model::kafka_namespace) {
                continue;
            }
            /*
             * quiet authz failures. this isn't checking for a specifically
             * requested topic, but rather checking visibility of all topics.
             */
            if (!ctx.authorized(
                  security::acl_operation::describe,
                  tp_ns.tp,
                  authz_quiet{true})) {
                continue;
            }
            res.push_back(make_topic_response(ctx, request, md));
        }

        return ss::make_ready_future<std::vector<metadata_response::topic>>(
          std::move(res));
    }

    std::vector<ss::future<metadata_response::topic>> new_topics;

    for (auto& topic : *request.data.topics) {
        /**
         * Authorize source topic in case if we deal with materialized one
         */
        if (!ctx.authorized(security::acl_operation::describe, topic.name)) {
            // not authorized, return authorization error
            res.push_back(make_error_topic_response(
              std::move(topic.name), error_code::topic_authorization_failed));
            continue;
        }
        if (auto md = ctx.metadata_cache().get_topic_metadata(
              model::topic_namespace_view(model::kafka_namespace, topic.name));
            md) {
            auto src_topic_response = make_topic_response(
              ctx, request, std::move(*md));
            src_topic_response.name = std::move(topic.name);
            res.push_back(std::move(src_topic_response));
            continue;
        }

        if (
          !config::shard_local_cfg().auto_create_topics_enabled
          || !request.data.allow_auto_topic_creation) {
            res.push_back(make_error_topic_response(
              std::move(topic.name), error_code::unknown_topic_or_partition));
            continue;
        }
        /**
         * check if authorized to create
         */
        if (!ctx.authorized(security::acl_operation::create, topic.name)) {
            res.push_back(make_error_topic_response(
              std::move(topic.name), error_code::topic_authorization_failed));
            continue;
        }
        new_topics.push_back(create_topic(ctx, std::move(topic.name)));
    }

    return ss::when_all_succeed(new_topics.begin(), new_topics.end())
      .then([res = std::move(res)](
              std::vector<metadata_response::topic> topics) mutable {
          res.insert(res.end(), topics.begin(), topics.end());
          return res;
      });
}

/**
 * During configuration changes, it may not be possible to identify
 * the correct listener on a broker based on our local listener's
 * name alone (e.g. if the names of listeners differ between nodes'
 * configuration.
 *
 * Attempt to guess the right listener on a peer by port, falling back to
 * picking the first listener if that doesn't work.
 *
 * Assumption: that peer metadata contains at least one suitable address
 * that is accessible to the client making this request.  A redpanda
 * cluster for which this is not true is in an invalid configuration
 * and cannot serve Kafka on any listener that does not have an equivalent
 * listener on other nodes, because Kafka clients have to be able to
 * connect to all brokers.
 *
 * @return pointer to the best guess at which listener on a peer should
 *         be used in kafka metadata responses.
 */
static const model::broker_endpoint*
guess_peer_listener(request_context& ctx, cluster::broker_ptr broker) {
    // Peer has no listener with name matching the name of the
    // listener serving this Kafka request.  This can happen during
    // configuration changes
    // (https://github.com/redpanda-data/redpanda/issues/3588)
    //
    // Use a fallback matching to find the best peer address we can.
    vlog(
      klog.warn,
      "Broker {} has no listener named '{}', falling "
      "back to guessing peer listener",
      broker->id(),
      ctx.listener());

    // Look up port for the listener in use for this request
    const auto& my_listeners = config::node().advertised_kafka_api();
    int16_t my_port = 0;
    for (const auto& l : my_listeners) {
        if (l.name == ctx.listener()) {
            my_port = l.address.port();

            // Looking up the address for myself?  Take the whole
            // listener.  This is the path where what's in node_config
            // is not yet consistent with what's in members_table,
            // because a node configuration update didn't propagate
            // via raft0 yet
            if (broker->id() == config::node().node_id()) {
                return &l;
            }
        }
    }

    if (my_port == 0) {
        // Should never happen: if we're listening with a given
        // name, that name must have been in config.
        vlog(
          klog.error,
          "Request on listener '{}' but not found in node_config",
          ctx.listener());
        return nullptr;
    }

    // Fallback 1: Try to match by port
    for (const auto& listener : broker->kafka_advertised_listeners()) {
        // filter broker listeners by active connection
        if (listener.address.port() == my_port) {
            return &listener;
        }
    }

    // Fallback 2: no name or port match, return first listener from
    // peer.
    if (!broker->kafka_advertised_listeners().empty()) {
        return &broker->kafka_advertised_listeners()[0];
    } else {
        // A broker with no kafka listeners, there is no way to
        // include it in our response
        return nullptr;
    }
}

template<>
ss::future<response_ptr> metadata_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group g) {
    metadata_response reply;
    auto alive_brokers = co_await ctx.metadata_cache().all_alive_brokers();
    for (const auto& broker : alive_brokers) {
        const model::broker_endpoint* peer_listener = nullptr;
        for (const auto& listener : broker->kafka_advertised_listeners()) {
            // filter broker listeners by active connection
            if (listener.name == ctx.listener()) {
                peer_listener = &listener;
                break;
            }
        }

        if (peer_listener == nullptr) {
            peer_listener = guess_peer_listener(ctx, broker);
        }

        if (peer_listener) {
            reply.data.brokers.push_back(metadata_response::broker{
              .node_id = broker->id(),
              .host = peer_listener->address.host(),
              .port = peer_listener->address.port(),
              .rack = broker->rack()});
        }
    }

    const auto cluster_id = config::shard_local_cfg().cluster_id();
    if (cluster_id.has_value()) {
        reply.data.cluster_id = ssx::sformat("redpanda.{}", cluster_id.value());
    } else {
        // Include a "redpanda." cluster ID even if we didn't initialize
        // cluster_id yet, so that callers can identify which Kafka
        // implementation they're talking to.
        reply.data.cluster_id = "redpanda.initializing";
    }

    auto leader_id = ctx.metadata_cache().get_controller_leader_id();
    reply.data.controller_id = leader_id.value_or(model::node_id(-1));

    metadata_request request;
    request.decode(ctx.reader(), ctx.header().version);

    reply.data.topics = co_await get_topic_metadata(ctx, request);

    if (
      request.data.include_cluster_authorized_operations
      && ctx.authorized(
        security::acl_operation::describe, security::default_cluster_name)) {
        reply.data.cluster_authorized_operations = details::to_bit_field(
          details::authorized_operations(ctx, security::default_cluster_name));
    }

    co_return co_await ctx.respond(std::move(reply));
}

size_t metadata_memory_estimator(size_t, connection_context&) {
    // We cannot make a precise estimate of the size of a metadata response by
    // examining only the size of the request (nor even by examining the entire
    // request) since the response depends on the number of partitions in the
    // cluster. Instead, we return a conservative estimate based on the soft
    // limit of total cluster-wide partition counts, mutiplied by an empirical
    // "bytes per partition".
    //
    // The bytes per partition is roughly split evenly between the number of
    // bytes needed to encode the response (about 100 bytes per partition,
    // assuming 3 replicas) as well as the input structure to the encoder (i.e.,
    // the in-memory representation of that same metadata).
    //
    // This estimate is additionally inexact in the sense that more a higher
    // replication factor would increase the per-partition size without bound.
    // An additional inaccuracy is that we don't consider topics but lump all
    // partitions together: this works for moderate to large partition counts,
    // but it likely that a cluster with (for example) many topics with only 1
    // partition each might produce a larger metadata response than this
    // calculation accounts for.

    // Empirical highwater memory allocated per partition while processing a
    // metadata response.
    constexpr size_t bytes_per_partition = 200;

    return max_clusterwide_partitions * bytes_per_partition;
}
} // namespace kafka
