// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "handlers.h"

#include "kafka/protocol/fetch.h"
#include "kafka/types.h"
#include "model/fundamental.h"
#include "pandaproxy/configuration.h"
#include "pandaproxy/json/requests/create_consumer.h"
#include "pandaproxy/json/requests/fetch.h"
#include "pandaproxy/json/requests/produce.h"
#include "pandaproxy/json/rjson_util.h"
#include "pandaproxy/reply.h"
#include "raft/types.h"
#include "ssx/future-util.h"
#include "storage/record_batch_builder.h"

#include <seastar/core/future.hh>

#include <absl/container/flat_hash_map.h>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>

#include <algorithm>
#include <chrono>

namespace ppj = pandaproxy::json;

namespace pandaproxy {

ppj::serialization_format parse_serialization_format(std::string_view accept) {
    std::vector<std::string_view> none = {
      "", "*/*", "application/json", "application/vnd.kafka.v2+json"};

    std::vector<ss::sstring> results;
    boost::split(
      results, accept, boost::is_any_of(",; "), boost::token_compress_on);

    if (std::any_of(results.begin(), results.end(), [](std::string_view v) {
            return v == "application/vnd.kafka.binary.v2+json";
        })) {
        return ppj::serialization_format::binary_v2;
    }

    if (std::any_of(
          results.begin(), results.end(), [&none](std::string_view lhs) {
              return std::any_of(
                none.begin(), none.end(), [lhs](std::string_view rhs) {
                    return lhs == rhs;
                });
          })) {
        return ppj::serialization_format::none;
    }

    return ppj::serialization_format::unsupported;
}

ss::future<server::reply_t>
get_topics_names(server::request_t rq, server::reply_t rp) {
    rq.req.reset();
    auto make_list_topics_req = []() {
        return kafka::metadata_request{.list_all_topics = true};
    };
    return rq.ctx.client.dispatch(make_list_topics_req)
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
get_topics_records(server::request_t rq, server::reply_t rp) {
    auto fmt = parse_serialization_format(rq.req->get_header("Accept"));
    if (fmt == ppj::serialization_format::unsupported) {
        rp.rep = unprocessable_entity("Unsupported serialization format");
        return ss::make_ready_future<server::reply_t>(std::move(rp));
    }

    model::topic_partition tp{
      model::topic(rq.req->param["topic_name"]),
      model::partition_id{boost::lexical_cast<model::partition_id::type>(
        rq.req->param["partition_id"])}};
    model::offset offset{boost::lexical_cast<model::offset::type>(
      rq.req->get_query_param("offset"))};
    std::chrono::milliseconds timeout{
      boost::lexical_cast<std::chrono::milliseconds::rep>(
        rq.req->get_query_param("timeout"))};
    int32_t max_bytes{
      boost::lexical_cast<int32_t>(rq.req->get_query_param("max_bytes"))};

    rq.req.reset();
    return rq.ctx.client
      .fetch_partition(std::move(tp), offset, max_bytes, timeout)
      .then([fmt, rp = std::move(rp)](kafka::fetch_response res) mutable {
          rapidjson::StringBuffer str_buf;
          rapidjson::Writer<rapidjson::StringBuffer> w(str_buf);

          ppj::rjson_serialize_fmt(fmt)(w, std::move(res));

          // TODO Ben: Prevent this linearization
          ss::sstring json_rslt = str_buf.GetString();
          rp.rep->write_body("json", json_rslt);
          return std::move(rp);
      });
}

ss::future<server::reply_t>
post_topics_name(server::request_t rq, server::reply_t rp) {
    auto fmt = parse_serialization_format(rq.req->get_header("Accept"));
    if (fmt == ppj::serialization_format::unsupported) {
        rp.rep = unprocessable_entity("Unsupported serialization format");
        return ss::make_ready_future<server::reply_t>(std::move(rp));
    }

    auto raw_records = ppj::rjson_parse(
      rq.req->content.data(), ppj::produce_request_handler(fmt));

    absl::flat_hash_map<model::partition_id, storage::record_batch_builder>
      partition_builders;

    for (auto& r : raw_records) {
        auto it = partition_builders
                    .try_emplace(r.id, raft::data_batch_type, model::offset(0))
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

    auto topic = model::topic(rq.req->param["topic_name"]);
    return ssx::parallel_transform(
             std::move(partitions),
             [topic,
              rq{std::move(rq)}](kafka::produce_request::partition p) mutable {
                 return rq.ctx.client.produce_record_batch(
                   model::topic_partition(topic, p.id),
                   std::move(*p.adapter.batch));
             })
      .then([topic, rp{std::move(rp)}](auto responses) mutable {
          std::vector<kafka::produce_response::topic> topics;
          topics.push_back(kafka::produce_response::topic{
            .name{std::move(topic)}, .partitions{std::move(responses)}});

          auto res = kafka::produce_response{
            .topics{std::move(topics)},
            .throttle{std::chrono::milliseconds{0}}};

          auto json_rslt = ppj::rjson_serialize(res.topics[0]);
          rp.rep->write_body("json", json_rslt);
          return std::move(rp);
      });
}

ss::future<server::reply_t>
create_consumer(server::request_t rq, server::reply_t rp) {
    auto req_data = ppj::rjson_parse(
      rq.req->content.data(), ppj::create_consumer_request_handler());
    auto group_id = kafka::group_id(rq.req->param["group_name"]);

    return rq.ctx.client.create_consumer(group_id).then(
      [group_id, rp{std::move(rp)}](kafka::member_id m_id) mutable {
          auto adv_addr = shard_local_cfg().advertised_pandaproxy_api();
          json::create_consumer_response res{
            .instance_id = m_id,
            .base_uri = fmt::format(
              "http://{}:{}/consumers/{}",
              adv_addr.host(),
              adv_addr.port(),
              m_id())};
          auto json_rslt = ppj::rjson_serialize(res);
          rp.rep->write_body("json", json_rslt);
          return std::move(rp);
      });
}

} // namespace pandaproxy
