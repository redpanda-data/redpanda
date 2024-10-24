// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/metadata.h"

#include "base/likely.h"
#include "cluster/metadata_cache.h"
#include "cluster/topics_frontend.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "config/node_config.h"
#include "container/fragmented_vector.h"
#include "kafka/protocol/schemata/metadata_response.h"
#include "kafka/server/errors.h"
#include "kafka/server/fwd.h"
#include "kafka/server/handlers/details/isolated_node_utils.h"
#include "kafka/server/handlers/details/leader_epoch.h"
#include "kafka/server/handlers/details/security.h"
#include "kafka/server/handlers/topics/topic_utils.h"
#include "kafka/server/response.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/timeout_clock.h"
#include "utils/to_string.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/thread.hh>

#include <boost/numeric/conversion/cast.hpp>
#include <fmt/ostream.h>

#include <iterator>
#include <type_traits>

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
    /**
     * If current broker do not yet have any information about leadership we
     * fallback to leader guesstimating. We return first replica from the
     * replica set and term 0. (This is the same logic that has been a part of
     * cluster::topic_dispatcher before)
     */
    if (!leader_term) {
        leader_term.emplace(replicas[0], model::term_id(0));
        return leader_term;
    }
    if (!leader_term->leader.has_value()) {
        const auto previous = md_cache.get_previous_leader_id(tp_ns, p_id);
        leader_term->leader = previous;

        if (previous == *config::node().node_id()) {
            auto idx = fast_prng_source() % replicas.size();
            leader_term->leader = replicas[idx];
        }
    }

    return leader_term;
}

namespace {
bool is_internal(model::topic_namespace_view tp_ns) {
    return tp_ns == model::kafka_consumer_offsets_nt;
}

} // namespace

metadata_response::topic make_topic_response_from_topic_metadata(
  const cluster::metadata_cache& md_cache,
  const cluster::topic_metadata& tp_md,
  const is_node_isolated_or_decommissioned is_node_isolated,
  bool recovery_mode_enabled) {
    metadata_response::topic tp;
    tp.error_code = error_code::none;
    model::topic_namespace_view tp_ns = tp_md.get_configuration().tp_ns;
    tp.name = tp_md.get_configuration().tp_ns.tp;

    tp.is_internal = is_internal(tp_ns);

    const bool is_user_topic = model::is_user_topic(tp_ns);
    const auto* disabled_set = md_cache.get_topic_disabled_set(tp_ns);

    for (const auto& [_, p_as] : tp_md.get_assignments()) {
        std::vector<model::node_id> replicas{};
        replicas.reserve(p_as.replicas.size());
        // current replica set
        std::transform(
          std::cbegin(p_as.replicas),
          std::cend(p_as.replicas),
          std::back_inserter(replicas),
          [](const model::broker_shard& bs) { return bs.node_id; });
        metadata_response::partition p;
        p.error_code = error_code::none;
        if (recovery_mode_enabled && is_user_topic) {
            p.error_code = error_code::policy_violation;
        } else if (disabled_set && disabled_set->is_disabled(p_as.id)) {
            p.error_code = error_code::replica_not_available;
        }
        p.partition_index = p_as.id;
        p.leader_id = no_leader;
        auto lt = get_leader_term(tp_ns, p_as.id, md_cache, replicas);
        if (lt && !is_node_isolated && p.error_code == error_code::none) {
            p.leader_id = lt->leader.value_or(no_leader);
            p.leader_epoch = leader_epoch_from_term(lt->term);
        }
        if (is_node_isolated && p.error_code == error_code::none) {
            auto replicas_for_sfuffle = replicas;
            std::shuffle(
              replicas_for_sfuffle.begin(),
              replicas_for_sfuffle.end(),
              std::default_random_engine());
            for (const auto& replica : replicas_for_sfuffle) {
                if (replica != config::node().node_id()) {
                    p.leader_id = replica;
                    break;
                }
            }
        }
        p.replica_nodes = std::move(replicas);
        p.isr_nodes = p.replica_nodes;
        p.offline_replicas = {};
        tp.partitions.push_back(std::move(p));
    }

    return tp;
}

static ss::future<metadata_response::topic> create_topic(
  request_context& ctx,
  model::topic&& topic,
  const is_node_isolated_or_decommissioned is_node_isolated) {
    if (is_node_isolated) {
        vlog(
          klog.info,
          "Can not autocreate topic({}) in metadata request, because node is "
          "isolated",
          topic);
        metadata_response::topic t;
        t.name = std::move(topic);
        t.error_code = error_code::broker_not_available;
        return ss::make_ready_future<metadata_response::topic>(std::move(t));
    }
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
              return ss::make_ready_future<metadata_response::topic>(
                std::move(t));
          }
          auto tp_md = md_cache.get_topic_metadata(res[0].tp_ns);

          if (!tp_md) {
              metadata_response::topic t;
              t.name = std::move(res[0].tp_ns.tp);
              t.error_code = error_code::invalid_topic_exception;
              return ss::make_ready_future<metadata_response::topic>(
                std::move(t));
          }

          return wait_for_topics(
                   md_cache,
                   res,
                   ctx.controller_api(),
                   tout + model::timeout_clock::now())
            .then([&ctx, tp_md = std::move(tp_md)]() mutable {
                return make_topic_response_from_topic_metadata(
                  ctx.metadata_cache(),
                  tp_md.value(),
                  is_node_isolated_or_decommissioned::no,
                  ctx.recovery_mode_enabled());
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
  request_context& ctx,
  metadata_request& rq,
  const cluster::topic_metadata& md,
  const is_node_isolated_or_decommissioned is_node_isolated) {
    auto res = make_topic_response_from_topic_metadata(
      ctx.metadata_cache(), md, is_node_isolated, ctx.recovery_mode_enabled());

    /**
     * if requested include topic authorized operations
     */
    if (rq.data.include_topic_authorized_operations) {
        res.topic_authorized_operations = details::to_bit_field(
          details::authorized_operations(ctx, md.get_configuration().tp_ns.tp));
    }

    return res;
}

static ss::future<small_fragment_vector<metadata_response::topic>>
get_topic_metadata(
  request_context& ctx,
  metadata_request& request,
  const is_node_isolated_or_decommissioned is_node_isolated) {
    small_fragment_vector<metadata_response::topic> res;

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
            res.push_back(
              make_topic_response(ctx, request, md.metadata, is_node_isolated));
        }

        return ss::make_ready_future<
          small_fragment_vector<metadata_response::topic>>(std::move(res));
    }

    std::vector<model::topic> topics_to_be_created;
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
              ctx, request, *md, is_node_isolated);
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
        topics_to_be_created.emplace_back(std::move(topic.name));
    }

    if (!ctx.audit()) {
        std::for_each(res.begin(), res.end(), [](metadata_response::topic& t) {
            t.error_code = error_code::broker_not_available;
        });

        std::transform(
          topics_to_be_created.begin(),
          topics_to_be_created.end(),
          std::back_inserter(res),
          [](model::topic& t) {
              return metadata_response::topic{
                .error_code = error_code::broker_not_available,
                .name = std::move(t)};
          });

        return ss::make_ready_future<
          small_fragment_vector<metadata_response::topic>>(std::move(res));
    }

    std::for_each(
      topics_to_be_created.begin(),
      topics_to_be_created.end(),
      [&new_topics, &ctx, is_node_isolated](model::topic& t) {
          new_topics.emplace_back(
            create_topic(ctx, std::move(t), is_node_isolated));
      });

    return ss::when_all_succeed(new_topics.begin(), new_topics.end())
      .then([res = std::move(res)](
              std::vector<metadata_response::topic> topics) mutable {
          std::move(topics.begin(), topics.end(), std::back_inserter(res));
          return std::move(res);
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
static const std::optional<model::broker_endpoint>
guess_peer_listener(request_context& ctx, const cluster::node_metadata& nm) {
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
      nm.broker.id(),
      ctx.listener());

    // Look up port for the listener in use for this request
    const auto my_listeners = config::node().advertised_kafka_api();
    int16_t my_port = 0;
    for (const auto& l : my_listeners) {
        if (l.name == ctx.listener()) {
            my_port = l.address.port();

            // Looking up the address for myself?  Take the whole
            // listener.  This is the path where what's in node_config
            // is not yet consistent with what's in members_table,
            // because a node configuration update didn't propagate
            // via raft0 yet
            if (nm.broker.id() == *config::node().node_id()) {
                return l;
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
        return std::nullopt;
    }

    // Fallback 1: Try to match by port
    for (const auto& listener : nm.broker.kafka_advertised_listeners()) {
        // filter broker listeners by active connection
        if (listener.address.port() == my_port) {
            return listener;
        }
    }

    // Fallback 2: no name or port match, return first listener from
    // peer.
    if (!nm.broker.kafka_advertised_listeners().empty()) {
        return nm.broker.kafka_advertised_listeners()[0];
    } else {
        // A broker with no kafka listeners, there is no way to
        // include it in our response
        return std::nullopt;
    }
}

// If node isolated or decomissioned it can not handle kafka requests from
// client, so in this case we need to signal client comunicate with another
// broker. For this we need to exclude isolated node from brokers list and
// return -1 for controller_id, after it client will send metadata request to
// another broker and will comunicate with it
static ss::future<metadata_response> fill_info_about_brokers_and_controller_id(
  request_context& ctx, is_node_isolated_or_decommissioned isolated_flag) {
    metadata_response reply;

    std::vector<cluster::node_metadata> alive_brokers;
    if (isolated_flag) {
        alive_brokers = ctx.metadata_cache().all_nodes();
    } else {
        alive_brokers = co_await ctx.metadata_cache().alive_nodes();
    }

    for (const auto& nm : alive_brokers) {
        if (isolated_flag && nm.broker.id() == config::node().node_id()) {
            continue;
        }

        std::optional<model::broker_endpoint> peer_listener;
        for (const auto& listener : nm.broker.kafka_advertised_listeners()) {
            // filter broker listeners by active connection
            if (listener.name == ctx.listener()) {
                peer_listener = listener;
                break;
            }
        }

        if (!peer_listener) {
            peer_listener = guess_peer_listener(ctx, nm);
        }

        if (peer_listener) {
            reply.data.brokers.push_back(metadata_response::broker{
              .node_id = nm.broker.id(),
              .host = peer_listener->address.host(),
              .port = peer_listener->address.port(),
              .rack = nm.broker.rack()});
        }
    }

    if (isolated_flag) {
        reply.data.controller_id = model::node_id(-1);
    } else {
        auto leader_id = ctx.metadata_cache().get_controller_leader_id();
        reply.data.controller_id = leader_id.value_or(model::node_id(-1));
    }

    co_return reply;
}

template<>
ss::future<response_ptr> metadata_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group g) {
    auto isolated_or_decommissioned = node_isolated_or_decommissioned(ctx);

    auto reply = co_await fill_info_about_brokers_and_controller_id(
      ctx, isolated_or_decommissioned);

    const auto cluster_id = config::shard_local_cfg().cluster_id();
    if (cluster_id.has_value()) {
        reply.data.cluster_id = ssx::sformat("redpanda.{}", cluster_id.value());
    } else {
        // Include a "redpanda." cluster ID even if we didn't initialize
        // cluster_id yet, so that callers can identify which Kafka
        // implementation they're talking to.
        reply.data.cluster_id = "redpanda.initializing";
    }

    metadata_request request;
    request.decode(ctx.reader(), ctx.header().version);
    log_request(ctx.header(), request);

    reply.data.topics = co_await get_topic_metadata(
      ctx, request, isolated_or_decommissioned);

    if (
      request.data.include_cluster_authorized_operations
      && ctx.authorized(
        security::acl_operation::describe, security::default_cluster_name)) {
        reply.data.cluster_authorized_operations = details::to_bit_field(
          details::authorized_operations(ctx, security::default_cluster_name));
    }

    co_return co_await ctx.respond(std::move(reply));
}

size_t
metadata_memory_estimator(size_t request_size, connection_context& conn_ctx) {
    // We cannot make a precise estimate of the size of a metadata response by
    // examining only the size of the request (nor even by examining the entire
    // request) since the response depends on the number of partitions in the
    // cluster. Instead, we return a conservative estimate based on the current
    // number of topics & partitions in the cluster.

    // Essentially we need to estimate the size taken by a "maximum size"
    // metadata_response_data response. The maximum size is when metadata for
    // all topics is returned, which is also a common case in practice. This
    // involves calculating the size for each topic's portion of the response,
    // since the size varies both based on the number of partitions and the
    // replica count.

    // We start with a base estimate of 10K and then proceed to ignore
    // everything other than the topic/partition part of the response, since
    // that's what takes space in large responses and we assume the remaining
    // part of the response (the broker list being the second largest part) will
    // fit in this 10000k slush fund.
    size_t size_estimate = 10000;

    auto& md_cache = conn_ctx.server().metadata_cache();

    // The size will vary with the number of brokers, though this effect is
    // probably small if there are large numbers of partitions

    // This covers the variable part of the broker response, i.e., the broker
    // hostname + rack We just hope these are less than this amount, because we
    // don't want to execute the relatively complex logic to guess the listener
    // just for the size estimate.
    constexpr size_t extra_bytes_per_broker = 200;
    size_estimate
      += md_cache.node_count()
         * (sizeof(metadata_response_broker) + extra_bytes_per_broker);

    for (auto& [tp_ns, topic_metadata] : md_cache.all_topics_metadata()) {
        // metadata_response_topic
        size_estimate += sizeof(kafka::metadata_response_topic);
        size_estimate += tp_ns.tp().size();

        using partition = kafka::metadata_response_partition;

        // Base number of bytes needed to represent each partition, ignoring the
        // variable part attributable to the replica count, we just take as the
        // size of the partition response structure.
        constexpr size_t bytes_per_partition = sizeof(partition);

        // Then, we need the number of additional bytes per replica, per
        // partition, associated with storing the replica list in
        // metadata_response_partition::replicas/isr_nodes, which we take to
        // be the size of the elements in those lists (4 bytes each).
        constexpr size_t bytes_per_replica = sizeof(partition::replica_nodes[0])
                                             + sizeof(partition::isr_nodes[0]);

        // The actual partition and replica count for this topic.
        int32_t pcount = topic_metadata.get_configuration().partition_count;
        cluster::replication_factor rcount
          = topic_metadata.get_replication_factor();

        size_estimate += pcount
                         * (bytes_per_partition + bytes_per_replica * rcount);
    }

    // Finally, we double the estimate, because the highwater mark for memory
    // use comes when the in-memory structures (metadata_response_data and
    // subobjects) exist on the heap and they are encoded into the reponse,
    // which will also exist on the heap. The calculation above handles the
    // first size, and the encoded response ends up being very similar in size,
    // so we double the estimate to account for both.
    size_estimate *= 2;

    // We still add on the default_estimate to handle the size of the request
    // itself and miscellaneous other procesing (this is a small adjustment,
    // generally ~8000 bytes). Finally, we add max_frag_bytes to account for the
    // worse-cast overshoot during vector re-allocation.
    return default_memory_estimate(request_size) + size_estimate
           + large_fragment_vector<
             metadata_response_partition>::max_frag_bytes();
}
} // namespace kafka
