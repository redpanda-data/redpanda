#include "handlers.h"

#include "pandaproxy/json/requests/produce.h"
#include "pandaproxy/json/rjson_util.h"
#include "storage/record_batch_builder.h"

#include <seastar/core/future.hh>
#include <seastar/http/reply.hh>

#include <absl/container/flat_hash_map.h>

#include <chrono>

namespace ppj = pandaproxy::json;

namespace pandaproxy {

ss::future<server::reply_t>
get_topics_names(server::request_t rq, server::reply_t rp) {
    rq.req.reset();
    kafka::metadata_request req;
    req.list_all_topics = true;
    return rq.ctx.client.dispatch(std::move(req))
      .then([rp = std::move(rp)](
              kafka::metadata_request::api_type::response_type res) mutable {
          std::vector<model::topic_view> names;
          names.reserve(res.topics.size());

          std::transform(
            res.topics.begin(),
            res.topics.end(),
            std::back_inserter(names),
            [](const kafka::metadata_response::topic& e) {
                return model::topic_view(e.name);
            });

          auto json_rslt = ppj::rjson_serialize(names);
          rp.rep->write_body("json", json_rslt);
          return std::move(rp);
      });
}

ss::future<server::reply_t>
post_topics_name(server::request_t rq, server::reply_t rp) {
    auto raw_records = ppj::rjson_parse(
      rq.req->content.data(),
      ppj::produce_request_handler(serialization_format::binary_v2));

    absl::flat_hash_map<model::partition_id, storage::record_batch_builder>
      partition_builders;

    for (auto& r : raw_records) {
        auto it = partition_builders
                    .try_emplace(
                      r.id, model::record_batch_type(3), model::offset(0))
                    .first;
        it->second.add_raw_kv(
          std::move(r.key).value_or(iobuf{}),
          std::move(r.value).value_or(iobuf{}));
    }

    std::vector<kafka::produce_request::partition> partitions;
    partitions.reserve(partition_builders.size());
    for (auto& pb : partition_builders) {
        partitions.emplace_back(kafka::produce_request::partition{
          .id = pb.first,
          .data = {},
          .adapter = kafka::kafka_batch_adapter{
            .v2_format = true,
            .valid_crc = true,
            .batch = std::move(pb.second).build()}});
    }

    std::vector<kafka::produce_request::topic> topics;
    topics.emplace_back(kafka::produce_request::topic{
      .name = model::topic(rq.req->param["topic_name"]),
      .partitions = std::move(partitions)});

    std::optional<ss::sstring> t_id;
    int16_t acks = -1;
    auto req = kafka::produce_request(t_id, acks, std::move(topics));

    return rq.ctx.client.dispatch(std::move(req))
      .then([rp = std::move(rp)](
              kafka::produce_request::api_type::response_type res) mutable {
          if (res.topics.size() != 1) {
              // TODO: better error
              rp.rep->set_status(
                ss::httpd::reply::status_type::internal_server_error);
              return std::move(rp);
          }

          auto json_rslt = ppj::rjson_serialize(res.topics[0]);
          rp.rep->write_body("json", json_rslt);
          return std::move(rp);
      });
}

} // namespace pandaproxy