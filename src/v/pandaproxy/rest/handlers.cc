// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "handlers.h"

#include "base/vlog.h"
#include "kafka/client/exceptions.h"
#include "kafka/protocol/fetch.h"
#include "kafka/protocol/schemata/offset_commit_request.h"
#include "model/fundamental.h"
#include "pandaproxy/json/requests/brokers.h"
#include "pandaproxy/json/requests/create_consumer.h"
#include "pandaproxy/json/requests/error_reply.h"
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
#include "ssx/sformat.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/http/reply.hh>

#include <absl/container/flat_hash_map.h>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>

#include <algorithm>
#include <chrono>
#include <iterator>
#include <system_error>

namespace ppj = pandaproxy::json;

namespace pandaproxy::rest {

namespace {

using server = proxy::server;

} // namespace

ss::future<server::reply_t>
get_brokers(server::request_t rq, server::reply_t rp) {
    auto res_fmt = parse::accept_header(
      *rq.req,
      {json::serialization_format::v2, json::serialization_format::none});
    rq.req.reset();
    vlog(plog.debug, "get_brokers");

    auto make_metadata_req = []() {
        return kafka::metadata_request{.list_all_topics = false};
    };

    co_return co_await rq.dispatch(make_metadata_req)
      .then([res_fmt, rp = std::move(rp)](
              kafka::metadata_request::api_type::response_type res) mutable {
          json::get_brokers_res brokers;
          brokers.ids.reserve(res.data.brokers.size());

          std::transform(
            res.data.brokers.begin(),
            res.data.brokers.end(),
            std::back_inserter(brokers.ids),
            [](const kafka::metadata_response::broker& b) {
                return b.node_id;
            });

          rp.rep->write_body("json", ppj::rjson_serialize(brokers));

          rp.mime_type = res_fmt;
          return std::move(rp);
      });
}

ss::future<server::reply_t>
get_topics_names(server::request_t rq, server::reply_t rp) {
    auto res_fmt = parse::accept_header(
      *rq.req,
      {json::serialization_format::v2, json::serialization_format::none});
    rq.req.reset();
    vlog(plog.debug, "get_topics_names");

    auto make_list_topics_req = []() {
        return kafka::metadata_request{.list_all_topics = true};
    };

    co_return co_await rq.dispatch(make_list_topics_req)
      .then([res_fmt, rp = std::move(rp)](
              kafka::metadata_request::api_type::response_type res) mutable {
          std::vector<model::topic_view> names;
          names.reserve(res.data.topics.size());
          for (auto& topic : res.data.topics) {
              if (!topic.is_internal) {
                  names.emplace_back(topic.name);
              }
          }

          rp.rep->write_body("json", ppj::rjson_serialize(names));
          rp.mime_type = res_fmt;
          return std::move(rp);
      });
}

ss::future<server::reply_t>
get_topics_records(server::request_t rq, server::reply_t rp) {
    auto res_fmt = parse::accept_header(
      *rq.req,
      {json::serialization_format::binary_v2,
       json::serialization_format::json_v2});

    auto tp{model::topic_partition{
      parse::request_param<model::topic>(*rq.req, "topic_name"),
      parse::request_param<model::partition_id>(*rq.req, "partition_id")}};
    auto offset{parse::query_param<model::offset>(*rq.req, "offset")};
    auto timeout{
      parse::query_param<std::chrono::milliseconds>(*rq.req, "timeout")};
    int32_t max_bytes{parse::query_param<int32_t>(*rq.req, "max_bytes")};

    rq.req.reset();
    vlog(
      plog.debug,
      "get_topics_records: tp: {}, offset: {}, timeout: {}, max_bytes: {}",
      tp,
      offset,
      timeout,
      max_bytes);

    co_return co_await rq
      .dispatch([offset, timeout, max_bytes, res_fmt, tp{std::move(tp)}](
                  kafka::client::client& client) mutable {
          return client
            .fetch_partition(std::move(tp), offset, max_bytes, timeout)
            .then([res_fmt](kafka::fetch_response res) {
                ::json::chunked_buffer buf;
                ::json::iobuf_writer<::json::chunked_buffer> w(buf);

                ppj::rjson_serialize_fmt(res_fmt)(w, std::move(res));
                return buf;
            });
      })
      .then([res_fmt, rp = std::move(rp)](auto buf) mutable {
          rp.rep->write_body(
            "json", json::as_body_writer(std::move(buf).as_iobuf()));
          rp.mime_type = res_fmt;
          return std::move(rp);
      });
}

ss::future<server::reply_t>
post_topics_name(server::request_t rq, server::reply_t rp) {
    auto req_fmt = parse::content_type_header(
      *rq.req,
      {json::serialization_format::binary_v2,
       json::serialization_format::json_v2});
    auto res_fmt = parse::accept_header(
      *rq.req,
      {json::serialization_format::v2, json::serialization_format::none});

    auto topic = parse::request_param<model::topic>(*rq.req, "topic_name");

    vlog(plog.debug, "get_topics_name: topic: {}", topic);

    auto records = co_await ppj::rjson_parse(
      std::move(rq.req), ppj::produce_request_handler(req_fmt));
    co_return co_await rq.dispatch(
      [records{std::move(records)}, topic, res_fmt, rp{std::move(rp)}](
        kafka::client::client& client) mutable {
          return client.produce_records(topic, std::move(records))
            .then([rp{std::move(rp)},
                   res_fmt](kafka::produce_response res) mutable {
                rp.rep->write_body(
                  "json", ppj::rjson_serialize(res.data.responses[0]));
                rp.mime_type = res_fmt;
                return std::move(rp);
            });
      });
}

static ss::sstring make_consumer_uri_base(
  const server::request_t& request, const kafka::group_id& group_id) {
    auto& addr = request.ctx.advertised_listeners[request.req->listener_idx];
    return ssx::sformat(
      "{}://{}:{}/consumers/{}/instances/",
      request.req->get_protocol_name(),
      addr.host(),
      addr.port(),
      group_id());
}

ss::future<server::reply_t>
create_consumer(server::request_t rq, server::reply_t rp) {
    parse::content_type_header(*rq.req, {json::serialization_format::v2});
    auto res_fmt = parse::accept_header(
      *rq.req,
      {json::serialization_format::v2, json::serialization_format::none});

    auto group_id = parse::request_param<kafka::group_id>(
      *rq.req, "group_name");

    auto base_uri = make_consumer_uri_base(rq, group_id);

    auto req_data = co_await ppj::rjson_parse(
      std::move(rq.req), ppj::create_consumer_request_handler());

    validate_no_control(
      req_data.name(), parse::pp_parsing_error{req_data.name()});

    if (req_data.format != "binary" && req_data.format != "json") {
        throw parse::error(
          parse::error_code::invalid_param,
          "format must be 'binary' or 'json'");
    }
    if (req_data.auto_offset_reset != "earliest") {
        throw parse::error(
          parse::error_code::invalid_param, "auto.offset must be earliest");
    }
    if (req_data.auto_commit_enable != "false") {
        throw parse::error(
          parse::error_code::invalid_param, "auto.commit must be false");
    }

    co_return co_await rq.dispatch(
      group_id,
      [group_id,
       base_uri{std::move(base_uri)},
       res_fmt,
       req_data{std::move(req_data)},
       rp{std::move(rp)}](kafka::client::client& client) mutable {
          vlog(
            plog.debug,
            "create_consumer: group_id: {}, name: {}, min_bytes: {}, timeout: "
            "{}, auto_offset_reset: {}, auto_commit_enable: {}",
            group_id,
            req_data.name,
            req_data.fetch_min_bytes,
            req_data.consumer_request_timeout_ms,
            req_data.auto_offset_reset,
            req_data.auto_commit_enable);

          // clang-tidy 16.0.4 is reporting an erroneous 'use-after-move' error
          // when calling `then` after `get_consumer`.
          auto f = client.get_consumer(group_id, req_data.name);
          return f
            .then([group_id](const kafka::client::shared_consumer_t&) mutable {
                auto ec = make_error_condition(
                  reply_error_code::consumer_already_exists);
                server::reply_t rp;
                rp.rep = errored_body(ec, ec.message());
                return ss::make_ready_future<server::reply_t>(std::move(rp));
            })
            .handle_exception_type(
              [group_id,
               base_uri{std::move(base_uri)},
               res_fmt,
               req_data{std::move(req_data)},
               rp{std::move(rp)},
               &client](const kafka::client::consumer_error&) mutable {
                  return client.create_consumer(group_id, req_data.name)
                    .then([rp{std::move(rp)}, base_uri, res_fmt](
                            kafka::member_id name) mutable {
                        json::create_consumer_response res{
                          .instance_id = name, .base_uri = base_uri + name};
                        rp.rep->write_body("json", ppj::rjson_serialize(res));
                        rp.mime_type = res_fmt;
                        return std::move(rp);
                    });
              });
      });
}

ss::future<server::reply_t>
remove_consumer(server::request_t rq, server::reply_t rp) {
    parse::content_type_header(*rq.req, {json::serialization_format::v2});
    parse::accept_header(
      *rq.req,
      {json::serialization_format::v2, json::serialization_format::none});

    auto group_id = parse::request_param<kafka::group_id>(
      *rq.req, "group_name");
    auto member_id = parse::request_param<kafka::member_id>(
      *rq.req, "instance");

    co_return co_await rq.dispatch(
      group_id,
      [group_id, member_id{std::move(member_id)}, rp{std::move(rp)}](
        kafka::client::client& client) mutable {
          vlog(
            plog.debug,
            "remove_consumer: group_id: {}, member_id: {}",
            group_id,
            member_id);

          return client.remove_consumer(group_id, member_id)
            .then([rp{std::move(rp)}]() mutable {
                rp.rep->set_status(ss::http::reply::status_type::no_content);
                return std::move(rp);
            });
      });
}

ss::future<server::reply_t>
subscribe_consumer(server::request_t rq, server::reply_t rp) {
    parse::content_type_header(*rq.req, {json::serialization_format::v2});
    auto res_fmt = parse::accept_header(
      *rq.req,
      {json::serialization_format::v2, json::serialization_format::none});

    auto group_id = parse::request_param<kafka::group_id>(
      *rq.req, "group_name");
    auto member_id = parse::request_param<kafka::member_id>(
      *rq.req, "instance");

    auto req_data = co_await ppj::rjson_parse(
      std::move(rq.req), ppj::subscribe_consumer_request_handler());
    std::for_each(
      req_data.topics.begin(),
      req_data.topics.end(),
      [](const auto& topic_name) {
          validate_no_control(
            topic_name(), parse::pp_parsing_error{topic_name()});
      });

    co_return co_await rq.dispatch(
      group_id,
      [group_id,
       member_id{std::move(member_id)},
       req_data{std::move(req_data)},
       res_fmt,
       rp{std::move(rp)}](kafka::client::client& client) mutable {
          vlog(
            plog.debug,
            "subscribe_consumer: group_id: {}, member_id: {}, topics: {}",
            group_id,
            member_id,
            req_data.topics);

          return client
            .subscribe_consumer(group_id, member_id, std::move(req_data.topics))
            .then([res_fmt, rp{std::move(rp)}]() mutable {
                rp.mime_type = res_fmt;
                rp.rep->set_status(ss::http::reply::status_type::no_content);
                return std::move(rp);
            });
      });
}

ss::future<server::reply_t>
consumer_fetch(server::request_t rq, server::reply_t rp) {
    auto res_fmt = parse::accept_header(
      *rq.req,
      {json::serialization_format::binary_v2,
       json::serialization_format::json_v2});

    auto group_id{parse::request_param<kafka::group_id>(*rq.req, "group_name")};
    auto name{parse::request_param<kafka::member_id>(*rq.req, "instance")};
    auto timeout{parse::query_param<std::optional<std::chrono::milliseconds>>(
      *rq.req, "timeout")};
    auto max_bytes{
      parse::query_param<std::optional<int32_t>>(*rq.req, "max_bytes")};

    co_return co_await rq.dispatch(
      group_id,
      [group_id,
       name{std::move(name)},
       timeout,
       max_bytes,
       res_fmt,
       rp{std::move(rp)}](kafka::client::client& client) mutable {
          vlog(
            plog.debug,
            "consumer_fetch: group_id: {}, name: {}, timeout: {}, max_bytes: "
            "{}",
            group_id,
            name,
            timeout,
            max_bytes);

          return client.consumer_fetch(group_id, name, timeout, max_bytes)
            .then([res_fmt, rp{std::move(rp)}](auto res) mutable {
                ::json::chunked_buffer buf;
                ::json::iobuf_writer<::json::chunked_buffer> w(buf);

                ppj::rjson_serialize_fmt(res_fmt)(w, std::move(res));

                rp.rep->write_body(
                  "json", json::as_body_writer(std::move(buf).as_iobuf()));
                rp.mime_type = res_fmt;
                return std::move(rp);
            });
      });
}

ss::future<server::reply_t>
get_consumer_offsets(server::request_t rq, server::reply_t rp) {
    parse::content_type_header(*rq.req, {json::serialization_format::v2});
    auto res_fmt = parse::accept_header(
      *rq.req,
      {json::serialization_format::v2, json::serialization_format::none});

    auto group_id{parse::request_param<kafka::group_id>(*rq.req, "group_name")};
    auto member_id{parse::request_param<kafka::member_id>(*rq.req, "instance")};

    auto req_data = ppj::partitions_request_to_offset_request(
      co_await ppj::rjson_parse(
        std::move(rq.req), ppj::partitions_request_handler()));

    std::for_each(req_data.begin(), req_data.end(), [](const auto& r) {
        validate_no_control(r.name(), parse::pp_parsing_error{r.name()});
    });

    co_return co_await rq.dispatch(
      group_id,
      [group_id,
       member_id{std::move(member_id)},
       req_data{std::move(req_data)},
       res_fmt,
       rp{std::move(rp)}](kafka::client::client& client) mutable {
          vlog(
            plog.debug,
            "get_consumer_offsets: group_id: {}, member_id: {}, offsets: {}",
            group_id,
            member_id,
            req_data);

          return client
            .consumer_offset_fetch(group_id, member_id, std::move(req_data))
            .then([rp{std::move(rp)}, res_fmt](auto res) mutable {
                rp.rep->write_body("json", ppj::rjson_serialize(res));
                rp.mime_type = res_fmt;
                return std::move(rp);
            });
      });
}

ss::future<server::reply_t>
post_consumer_offsets(server::request_t rq, server::reply_t rp) {
    parse::content_type_header(*rq.req, {json::serialization_format::v2});
    parse::accept_header(
      *rq.req,
      {json::serialization_format::v2, json::serialization_format::none});

    auto group_id{parse::request_param<kafka::group_id>(*rq.req, "group_name")};
    auto member_id{parse::request_param<kafka::member_id>(*rq.req, "instance")};

    // If the request is empty, commit all offsets
    auto req_data = rq.req->content_length == 0
                      ? std::vector<kafka::offset_commit_request_topic>()
                      : ppj::partition_offsets_request_to_offset_commit_request(
                          co_await ppj::rjson_parse(
                            std::move(rq.req),
                            ppj::partition_offsets_request_handler()));

    std::for_each(req_data.begin(), req_data.end(), [](const auto& r) {
        validate_no_control(r.name(), parse::pp_parsing_error{r.name()});
    });

    co_return co_await rq.dispatch(
      group_id,
      [group_id,
       member_id{std::move(member_id)},
       req_data{std::move(req_data)},
       rp{std::move(rp)}](kafka::client::client& client) mutable {
          vlog(
            plog.debug,
            "post_consumer_offsets: group_id: {}, member_id: {}, offsets: {}",
            group_id,
            member_id,
            req_data);

          return client
            .consumer_offset_commit(group_id, member_id, std::move(req_data))
            .then([rp{std::move(rp)}](auto) mutable {
                rp.rep->set_status(ss::http::reply::status_type::no_content);
                return std::move(rp);
            });
      });
}

ss::future<proxy::server::reply_t>
status_ready(proxy::server::request_t rq, proxy::server::reply_t rp) {
    auto make_metadata_req = []() {
        return kafka::metadata_request{.list_all_topics = false};
    };

    auto res = co_await rq.dispatch(make_metadata_req);
    rp.rep->set_status(ss::http::reply::status_type::ok);
    co_return rp;
}

} // namespace pandaproxy::rest
