#include "kafka/requests/metadata_request.h"

#include "cluster/metadata_cache.h"
#include "kafka/controller_dispatcher.h"
#include "kafka/errors.h"
#include "model/metadata.h"
#include "utils/to_string.h"

#include <seastar/core/thread.hh>

#include <fmt/ostream.h>

namespace kafka {

void metadata_request::decode(request_context& ctx) {
    auto version = ctx.header().version;
    auto& reader = ctx.reader();
    // For metadata request version 0 this array will always be present
    topics = reader.read_nullable_array(
      [](request_reader& r) { return model::topic(r.read_string()); });

    allow_auto_topic_creation = version >= api_version(4) ? reader.read_bool()
                                                          : false;
    if (version >= api_version(8)) {
        include_cluster_authorized_operations = reader.read_bool();
        include_topic_authorized_operations = reader.read_bool();
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
    return fmt_print(
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
          p.leader = p_md.leader_node;
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
    return fmt_print(
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

future<response_ptr>
metadata_api::process(request_context&& ctx, smp_service_group g) {
    return do_with(std::move(ctx), [g](request_context& ctx) {
        metadata_request request;
        request.decode(ctx);
        auto f = ctx.cntrl_dispatcher().dispatch_to_controller(
          [](cluster::controller& cntrl) { return cntrl.get_leader_id(); });

        metadata_response reply;
        // FIXME: fill with brokers list when available in cache
        reply.brokers.push_back({.node_id = model::node_id(1),
                                 .host = "localhost",
                                 .port = 9092,
                                 .rack = std::nullopt});
        // FIXME:  #95 Cluster Id
        reply.cluster_id = std::nullopt;

        // list all topics if topics array is not present
        bool list_all_topics = !request.topics;

        if (__builtin_expect(ctx.header().version == api_version(0), false)) {
            // For metadata API version 0, empty array requests all topics
            if (request.topics->empty()) {
                list_all_topics = true;
            }
        }

        if (list_all_topics) {
            // need to return all topics for empty request list
            auto topics = ctx.metadata_cache().all_topics_metadata();
            std::transform(
              topics.begin(),
              topics.end(),
              std::back_inserter(reply.topics),
              [](model::topic_metadata& t_md) {
                  return metadata_response::topic::make_from_topic_metadata(
                    std::move(t_md));
              });
        } else {
            // ask cache for each topic separatelly
            std::transform(
              std::cbegin(*request.topics),
              std::cend(*request.topics),
              std::back_inserter(reply.topics),
              [&ctx, &request](const model::topic& tp) {
                  auto opt = ctx.metadata_cache().get_topic_metadata(tp);
                  if (opt) {
                      return metadata_response::topic::make_from_topic_metadata(
                        std::move(*opt));
                  }
                  if (request.allow_auto_topic_creation) {
                      // we do not yet support creation of topics with Metadata
                      // API
                      kreq_log.warn(
                        "Topic autocreation with Metadata API is "
                        "not yet supported. Topic {} will not be created",
                        tp());
                      // TODO: Dispatch topic creation request to leader
                      //       controller
                  }
                  metadata_response::topic r_tp{};
                  r_tp.name = std::move(tp);
                  r_tp.err_code = error_code::unknown_topic_or_partition;
                  return r_tp;
              });
        }

        return f.then(
          [&ctx, reply = std::move(reply)](model::node_id leader_id) mutable {
              reply.controller_id = std::move(leader_id);
              response resp;
              reply.encode(ctx, resp);
              return make_ready_future<response_ptr>(
                std::make_unique<response>(std::move(resp)));
          });
    });
}

} // namespace kafka
