// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "handlers.h"

#include "kafka/client/exceptions.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/fetch.h"
#include "kafka/protocol/leave_group.h"
#include "kafka/protocol/offset_commit.h"
#include "kafka/protocol/offset_fetch.h"
#include "kafka/protocol/schemata/offset_commit_request.h"
#include "kafka/types.h"
#include "model/fundamental.h"
#include "pandaproxy/configuration.h"
#include "pandaproxy/json/exceptions.h"
#include "pandaproxy/json/requests/create_consumer.h"
#include "pandaproxy/json/requests/fetch.h"
#include "pandaproxy/json/requests/offset_commit.h"
#include "pandaproxy/json/requests/offset_fetch.h"
#include "pandaproxy/json/requests/partition_offsets.h"
#include "pandaproxy/json/requests/partitions.h"
#include "pandaproxy/json/requests/produce.h"
#include "pandaproxy/json/requests/subscribe_consumer.h"
#include "pandaproxy/json/rjson_util.h"
#include "pandaproxy/parsing/httpd.h"
#include "pandaproxy/reply.h"
#include "raft/types.h"
#include "ssx/future-util.h"
#include "storage/record_batch_builder.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/std-coroutine.hh>
#include <seastar/http/reply.hh>

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

    auto tp{model::topic_partition{
      parse::request_param<model::topic>(*rq.req, "topic_name"),
      parse::request_param<model::partition_id>(*rq.req, "partition_id")}};
    auto offset{parse::query_param<model::offset>(*rq.req, "offset")};
    auto timeout{
      parse::query_param<std::chrono::milliseconds>(*rq.req, "timeout")};
    int32_t max_bytes{parse::query_param<int32_t>(*rq.req, "max_bytes")};

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
      [group_id, rq{std::move(rq)}, rp{std::move(rp)}](
        kafka::member_id m_id) mutable {
          auto adv_addr = rq.ctx.config.advertised_pandaproxy_api();
          json::create_consumer_response res{
            .instance_id = m_id,
            .base_uri = fmt::format(
              "http://{}:{}/consumers/{}/instances/{}",
              adv_addr.host(),
              adv_addr.port(),
              group_id(),
              m_id())};
          auto json_rslt = ppj::rjson_serialize(res);
          rp.rep->write_body("json", json_rslt);
          return std::move(rp);
      });
}

ss::future<server::reply_t>
remove_consumer(server::request_t rq, server::reply_t rp) {
    auto group_id = kafka::group_id(rq.req->param["group_name"]);
    auto member_id = kafka::member_id(rq.req->param["instance"]);

    co_await rq.ctx.client.remove_consumer(group_id, member_id);
    rp.rep->set_status(ss::httpd::reply::status_type::no_content);
    co_return rp;
}

ss::future<server::reply_t>
subscribe_consumer(server::request_t rq, server::reply_t rp) {
    auto req_data = ppj::rjson_parse(
      rq.req->content.data(), ppj::subscribe_consumer_request_handler());
    auto group_id = kafka::group_id(rq.req->param["group_name"]);
    auto member_id = kafka::member_id(rq.req->param["instance"]);

    return rq.ctx.client
      .subscribe_consumer(group_id, member_id, std::move(req_data.topics))
      .then([rp{std::move(rp)}]() mutable {
          // nothing to do!
          return std::move(rp);
      });
}

ss::future<server::reply_t>
consumer_fetch(server::request_t rq, server::reply_t rp) {
    auto fmt = parse_serialization_format(rq.req->get_header("Accept"));
    if (fmt == ppj::serialization_format::unsupported) {
        rp.rep = unprocessable_entity("Unsupported serialization format");
        return ss::make_ready_future<server::reply_t>(std::move(rp));
    }

    std::chrono::milliseconds timeout{
      boost::lexical_cast<std::chrono::milliseconds::rep>(
        rq.req->get_query_param("timeout"))};
    int32_t max_bytes{
      boost::lexical_cast<int32_t>(rq.req->get_query_param("max_bytes"))};
    auto group_id = kafka::group_id(rq.req->param["group_name"]);
    auto member_id = kafka::member_id(rq.req->param["instance"]);

    return rq.ctx.client.consumer_fetch(group_id, member_id, timeout, max_bytes)
      .then([fmt, rp{std::move(rp)}](kafka::fetch_response res) mutable {
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
get_consumer_offsets(server::request_t rq, server::reply_t rp) {
    auto group_id = kafka::group_id(rq.req->param["group_name"]);
    auto member_id = kafka::member_id(rq.req->param["instance"]);

    auto req_data = ppj::partitions_request_to_offset_request(ppj::rjson_parse(
      rq.req->content.data(), ppj::partitions_request_handler()));

    auto res = co_await rq.ctx.client.consumer_offset_fetch(
      group_id, member_id, std::move(req_data));
    rapidjson::StringBuffer str_buf;
    rapidjson::Writer<rapidjson::StringBuffer> w(str_buf);
    ppj::rjson_serialize(w, res);
    ss::sstring json_rslt = str_buf.GetString();
    rp.rep->write_body("json", json_rslt);
    co_return rp;
}

ss::future<server::reply_t>
post_consumer_offsets(server::request_t rq, server::reply_t rp) {
    auto group_id = kafka::group_id(rq.req->param["group_name"]);
    auto member_id = kafka::member_id(rq.req->param["instance"]);

    // If the request is empty, commit all offsets
    auto req_data = rq.req->content.length() == 0
                      ? std::vector<kafka::offset_commit_request_topic>()
                      : ppj::partition_offsets_request_to_offset_commit_request(
                        ppj::rjson_parse(
                          rq.req->content.data(),
                          ppj::partition_offsets_request_handler()));

    auto res = co_await rq.ctx.client.consumer_offset_commit(
      group_id, member_id, std::move(req_data));
    rp.rep->set_status(ss::httpd::reply::status_type::no_content);
    co_return rp;
}

} // namespace pandaproxy
