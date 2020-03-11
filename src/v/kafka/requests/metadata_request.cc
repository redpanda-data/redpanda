#include "kafka/requests/metadata_request.h"

#include "cluster/metadata_cache.h"
#include "cluster/types.h"
#include "kafka/controller_dispatcher.h"
#include "kafka/errors.h"
#include "likely.h"
#include "model/metadata.h"
#include "utils/to_string.h"

#include <seastar/core/future-util.hh>
#include <seastar/core/thread.hh>

#include <fmt/ostream.h>

namespace kafka {

void metadata_request::decode(request_context& ctx) {
    auto version = ctx.header().version;
    auto& reader = ctx.reader();
    // For metadata request version 0 this array will always be present
    topics = reader.read_nullable_array(
      [](request_reader& r) { return model::topic(r.read_string()); });

    if (version >= api_version(4)) {
        allow_auto_topic_creation = reader.read_bool();
    }
    if (version >= api_version(8)) {
        include_cluster_authorized_operations = reader.read_bool();
        include_topic_authorized_operations = reader.read_bool();
    }

    if (ctx.header().version > api_version(0)) {
        list_all_topics = !topics;
    } else {
        // For metadata API version 0, empty array requests all topics
        list_all_topics = topics->empty();
    }
}

void metadata_request::encode(response_writer& writer, api_version version) {
    writer.write_nullable_array(
      topics, [](const model::topic tp, response_writer& writer) {
          writer.write(tp());
      });
    if (version >= api_version(4)) {
        writer.write(allow_auto_topic_creation);
    }
    if (version >= api_version(8)) {
        writer.write(include_cluster_authorized_operations);
        writer.write(include_topic_authorized_operations);
    }
}

std::ostream& operator<<(std::ostream& o, const metadata_request& r) {
    return ss::fmt_print(
      o,
      "topics {} auto_creation {} inc_cluster_aut_ops {} inc_topic_aut_ops {}",
      r.topics,
      r.allow_auto_topic_creation,
      r.include_cluster_authorized_operations,
      r.include_topic_authorized_operations);
}

void metadata_response::encode(const request_context& ctx, response& resp) {
    auto& writer = resp.writer();
    auto version = ctx.header().version;

    if (version >= api_version(3)) {
        writer.write(int32_t(throttle_time.count()));
    }
    // brokers
    writer.write_array(
      brokers, [version](const broker& b, response_writer& rw) {
          rw.write(b.node_id);
          rw.write(b.host);
          rw.write(b.port);
          if (version >= api_version(1)) {
              rw.write(b.rack);
          }
      });
    // cluster id
    if (version >= api_version(2)) {
        writer.write(cluster_id);
    }
    // controller id
    if (version >= api_version(1)) {
        writer.write(controller_id);
    }
    writer.write_array(topics, [version](const topic& tp, response_writer& rw) {
        tp.encode(version, rw);
    });
    if (version >= api_version(8)) {
        writer.write(cluster_authorized_operations);
    }
}

void metadata_response::topic::encode(
  api_version version, response_writer& rw) const {
    rw.write(err_code);
    rw.write(name);
    if (version >= api_version(1)) {
        rw.write(is_internal);
    }
    rw.write_array(
      partitions, [version](const partition& p, response_writer& rw) {
          p.encode(version, rw);
      });
    if (version >= api_version(8)) {
        rw.write(topic_authorized_operations);
    }
}

void metadata_response::partition::encode(
  api_version version, response_writer& rw) const {
    rw.write(err_code);
    rw.write(index);
    rw.write(leader);
    if (version >= api_version(7)) {
        rw.write(leader_epoch);
    }
    rw.write_array(
      replica_nodes,
      [](const model::node_id& n, response_writer& rw) { rw.write(n); });
    // isr nodes
    rw.write_array(
      replica_nodes,
      [](const model::node_id& n, response_writer& rw) { rw.write(n); });

    if (version >= api_version(5)) {
        rw.write_array(
          offline_replicas,
          [](const model::node_id& n, response_writer& rw) { rw.write(n); });
    }
}

void metadata_response::decode(iobuf buf, api_version version) {
    request_reader reader(std::move(buf));

    if (version >= api_version(3)) {
        throttle_time = std::chrono::milliseconds(reader.read_int32());
    }

    brokers = reader.read_array([version](request_reader& reader) {
        auto b = broker{
          .node_id = model::node_id(reader.read_int32()),
          .host = reader.read_string(),
          .port = reader.read_int32(),
        };
        if (version >= api_version(1)) {
            b.rack = reader.read_nullable_string();
        }
        return b;
    });

    if (version >= api_version(2)) {
        cluster_id = reader.read_nullable_string();
    }

    if (version >= api_version(1)) {
        controller_id = model::node_id(reader.read_int32());
    }

    topics = reader.read_array([version](request_reader& reader) {
        auto t = topic{
          .err_code = error_code(reader.read_int16()),
          .name = model::topic(reader.read_string()),
        };
        if (version >= api_version(1)) {
            t.is_internal = reader.read_bool();
        }
        t.partitions = reader.read_array([version](request_reader& reader) {
            auto p = partition{
              .err_code = error_code(reader.read_int16()),
              .index = model::partition_id(reader.read_int32()),
              .leader = model::node_id(reader.read_int32()),
            };
            if (version >= api_version(7)) {
                p.leader_epoch = reader.read_int32();
            }
            p.replica_nodes = reader.read_array([](request_reader& reader) {
                return model::node_id(reader.read_int32());
            });
            if (version >= api_version(5)) {
                p.offline_replicas = reader.read_array(
                  [](request_reader& reader) {
                      return model::node_id(reader.read_int32());
                  });
            }
            return p;
        });
        if (version >= api_version(8)) {
            t.topic_authorized_operations = reader.read_int32();
        }
        return t;
    });

    if (version >= api_version(8)) {
        cluster_authorized_operations = model::node_id(reader.read_int32());
    }
}

metadata_response::topic metadata_response::topic::make_from_topic_metadata(
  model::topic_metadata&& tp_md) {
    metadata_response::topic tp;
    tp.err_code = error_code::none;
    tp.name = std::move(tp_md.tp);
    tp.is_internal = false; // no internal topics yet
    std::transform(
      tp_md.partitions.begin(),
      tp_md.partitions.end(),
      std::back_inserter(tp.partitions),
      [](model::partition_metadata& p_md) {
          std::vector<model::node_id> replicas{};
          replicas.reserve(p_md.replicas.size());
          std::transform(
            std::cbegin(p_md.replicas),
            std::cend(p_md.replicas),
            std::back_inserter(replicas),
            [](const model::broker_shard& bs) { return bs.node_id; });
          metadata_response::partition p;
          p.err_code = error_code::none;
          p.index = p_md.id;
          p.leader = p_md.leader_node.value_or(model::node_id(-1));
          p.leader_epoch = 0;
          p.replica_nodes = std::move(replicas);
          p.offline_replicas = {};
          return p;
      });
    return tp;
}

std::ostream& operator<<(std::ostream& o, const metadata_response::broker& b) {
    return fmt_print(
      o, "id {} hostname {} port {} rack", b.node_id(), b.host, b.port, b.rack);
}

std::ostream&
operator<<(std::ostream& o, const metadata_response::partition& p) {
    return ss::fmt_print(
      o,
      "err_code {} idx {} leader {} leader_epoch {} replicas {} offline {}",
      p.err_code,
      p.index,
      p.leader(),
      p.leader_epoch,
      p.replica_nodes,
      p.offline_replicas);
}

std::ostream& operator<<(std::ostream& o, const metadata_response::topic& tp) {
    return fmt_print(
      o,
      "err_code {} name {} is_internal {} partitions {} tp_aut_ops {}",
      tp.err_code,
      tp.name(),
      tp.is_internal,
      tp.partitions,
      tp.topic_authorized_operations);
}

std::ostream& operator<<(std::ostream& o, const metadata_response& resp) {
    return fmt_print(
      o,
      "throttle_time {} brokers {} cluster_id {} controller_id {} topics {} "
      "cluster_aut_ops {}",
      resp.throttle_time,
      resp.brokers,
      resp.cluster_id,
      resp.controller_id,
      resp.topics,
      resp.cluster_authorized_operations);
}

static ss::future<metadata_response::topic>
create_topic(request_context& ctx, model::topic&& topic) {
    return ctx.cntrl_dispatcher().dispatch_to_controller(
      [topic = std::move(topic)](cluster::controller& c) mutable {
          auto timeout = ss::lowres_clock::now()
                         + config::shard_local_cfg().create_topic_timeout_ms();

          // default topic configuration
          cluster::topic_configuration cfg{
            cluster::kafka_namespace,
            topic,
            config::shard_local_cfg().default_topic_partitions(),
            config::shard_local_cfg().default_topic_replication()};

          return c.autocreate_topics({std::move(cfg)}, timeout)
            .then([](std::vector<cluster::topic_result> res) {
                vassert(res.size() == 1, "expected single result");
                metadata_response::topic t;
                t.name = std::move(res[0].topic);
                if (
                  res[0].ec == cluster::errc::success
                  || res[0].ec == cluster::errc::topic_already_exists) {
                    /*
                     * currently kafka instructs the client to retry. we may be
                     * able to be more optimistic here and return some of the
                     * metadata immediately after topic creation.
                     */
                    t.err_code = error_code::leader_not_available;
                } else {
                    t.err_code = map_topic_error_code(res[0].ec);
                }
                return t;
            })
            .handle_exception(
              [topic = std::move(topic)](std::exception_ptr e) mutable {
                  metadata_response::topic t;
                  t.name = std::move(topic);
                  t.err_code = error_code::request_timed_out;
                  return t;
              });
      });
}

static ss::future<std::vector<metadata_response::topic>>
get_topic_metadata(request_context& ctx, metadata_request& request) {
    std::vector<metadata_response::topic> res;

    // request can be served from whatever happens to be in the cache
    if (request.list_all_topics) {
        auto topics = ctx.metadata_cache().all_topics_metadata();
        std::transform(
          topics.begin(),
          topics.end(),
          std::back_inserter(res),
          [](model::topic_metadata& t_md) {
              return metadata_response::topic::make_from_topic_metadata(
                std::move(t_md));
          });
        return ss::make_ready_future<std::vector<metadata_response::topic>>(
          std::move(res));
    }

    std::vector<ss::future<metadata_response::topic>> new_topics;

    for (auto& topic : *request.topics) {
        if (auto md = ctx.metadata_cache().get_topic_metadata(topic); md) {
            res.push_back(metadata_response::topic::make_from_topic_metadata(
              std::move(*md)));
            continue;
        }

        if (!request.allow_auto_topic_creation) {
            metadata_response::topic t;
            t.name = std::move(topic);
            t.err_code = error_code::unknown_topic_or_partition;
            res.push_back(std::move(t));
            continue;
        }

        new_topics.push_back(create_topic(ctx, std::move(topic)));
    }

    return ss::when_all_succeed(new_topics.begin(), new_topics.end())
      .then([res = std::move(res)](
              std::vector<metadata_response::topic> topics) mutable {
          res.insert(res.end(), topics.begin(), topics.end());
          return res;
      });
}

ss::future<response_ptr>
metadata_api::process(request_context&& ctx, ss::smp_service_group g) {
    return ss::do_with(
      std::move(ctx),
      metadata_response{},
      [g](request_context& ctx, metadata_response& reply) {
          auto brokers = ctx.metadata_cache().all_brokers();
          std::transform(
            brokers.begin(),
            brokers.end(),
            std::back_inserter(reply.brokers),
            [](cluster::broker_ptr b) {
                return metadata_response::broker{
                  .node_id = b->id(),
                  .host = b->kafka_api_address().host(),
                  .port = b->kafka_api_address().port(),
                  .rack = b->rack()};
            });

          // FIXME:  #95 Cluster Id
          reply.cluster_id = std::nullopt;

          return ctx.cntrl_dispatcher()
            .dispatch_to_controller([&reply](cluster::controller& cntrl) {
                auto leader_id = cntrl.get_leader_id();
                reply.controller_id = leader_id.value_or(model::node_id(-1));
            })
            .then([&ctx, &reply]() {
                metadata_request request;
                request.decode(ctx);
                return get_topic_metadata(ctx, request)
                  .then([&reply](std::vector<metadata_response::topic> topics) {
                      reply.topics = std::move(topics);
                  });
            })
            .then([&ctx, &reply] { return ctx.respond(std::move(reply)); });
      });
}

} // namespace kafka
