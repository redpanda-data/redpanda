// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "raft/kvelldb/httpkvrsm.h"

#include "json/document.h"
#include "json/stringbuffer.h"
#include "json/writer.h"

#include <seastar/http/function_handlers.hh>
#include <seastar/http/json_path.hh>

#include <absl/container/flat_hash_map.h>

namespace raft::kvelldb {

bool try_parse_json_hash(
  ss::sstring json,
  std::vector<ss::sstring> keys,
  absl::flat_hash_map<ss::sstring, ss::sstring>& target) {
    rapidjson::Document body;

    if (body.Parse(json.c_str()).HasParseError()) {
        return false;
    }

    for (auto& key : keys) {
        if (!body.HasMember(key)) {
            return false;
        }

        rapidjson::Value& value = body[key];
        target.emplace(key, value.GetString());
    }

    return true;
}

ss::sstring reject_as_json() {
    json::StringBuffer buffer;
    json::Writer<json::StringBuffer> writer(buffer);

    writer.StartObject();
    writer.Key("status");
    writer.String("fail");
    writer.EndObject();

    ss::sstring serialized{buffer.GetString()};

    return serialized;
}

ss::sstring cmd_result_as_json(raft::kvelldb::kvrsm::cmd_result result) {
    raft::kvelldb::errc status;

    switch (static_cast<raft::errc>(result.raft_status.value())) {
    case raft::errc::not_leader:
        status = raft::kvelldb::errc::unknown_command;
        break;
    case raft::errc::success:
        switch (static_cast<raft::kvelldb::errc>(result.rsm_status.value())) {
        case raft::kvelldb::errc::unknown_command:
        case raft::kvelldb::errc::success:
        case raft::kvelldb::errc::not_found:
        case raft::kvelldb::errc::conflict:
            status = static_cast<raft::kvelldb::errc>(
              result.rsm_status.value());
            break;
        default:
            status = raft::kvelldb::errc::timeout;
            break;
        }
        break;
    default:
        status = raft::kvelldb::errc::timeout;
        break;
    }

    json::StringBuffer buffer;
    json::Writer<json::StringBuffer> writer(buffer);

    writer.StartObject();
    writer.Key("status");
    if (status == raft::kvelldb::errc::unknown_command) {
        writer.String("fail");
    } else if (
      status == raft::kvelldb::errc::success
      || status == raft::kvelldb::errc::conflict) {
        writer.String("ok");
        writer.Key("hasData");
        writer.Bool(true);
        writer.Key("value");
        writer.String(result.value);
        writer.Key("writeID");
        writer.String(result.write_id);
    } else if (status == raft::kvelldb::errc::not_found) {
        writer.String("ok");
        writer.Key("hasData");
        writer.Bool(false);
    } else {
        writer.String("unknown");
    }
    writer.Key("metrics");
    writer.StartObject();
    writer.Key("replicated_us");
    writer.Int64(result.replicated_us.count());
    writer.Key("executed_us");
    writer.Int64(result.executed_us.count());
    writer.EndObject();
    writer.EndObject();

    ss::sstring serialized{buffer.GetString()};

    return serialized;
}

std::chrono::seconds timeout(
  std::unordered_map<ss::sstring, ss::sstring>& query_parameters,
  int32_t default_timeout_seconds) {
    if (query_parameters.find("timeout") != query_parameters.end()) {
        ss::sstring timeout_s = query_parameters["timeout"];
        return std::chrono::seconds(std::stoi(timeout_s));
    } else {
        return std::chrono::seconds(default_timeout_seconds);
    }
}

httpkvrsm::httpkvrsm(
  ss::lw_shared_ptr<raft::kvelldb::kvrsm> kvrsm,
  const ss::sstring& server_name,
  ss::socket_address addr)
  : _kvrsm(kvrsm)
  , _server(server_name)
  , _addr(addr) {}

ss::future<> httpkvrsm::start() {
    auto read_handler = [this](
                          std::unique_ptr<ss::httpd::request> req,
                          std::unique_ptr<ss::httpd::reply> rep) {
        return this->read(std::move(req), std::move(rep));
    };

    ss::path_description read_route{
      "/read", ss::operation_type::GET, "read", {}, {}};
    ss::httpd::function_handler* read_ptr = new ss::httpd::function_handler(
      read_handler, "json");
    read_route.set(_server._routes, read_ptr);

    auto write_handler = [this](
                           std::unique_ptr<ss::httpd::request> req,
                           std::unique_ptr<ss::httpd::reply> rep) {
        return this->write(std::move(req), std::move(rep));
    };

    ss::path_description write_route{
      "/write", ss::operation_type::POST, "write", {}, {}};
    ss::httpd::function_handler* write_ptr = new ss::httpd::function_handler(
      write_handler, "json");
    write_route.set(_server._routes, write_ptr);

    auto cas_handler = [this](
                         std::unique_ptr<ss::httpd::request> req,
                         std::unique_ptr<ss::httpd::reply> rep) {
        return this->cas(std::move(req), std::move(rep));
    };

    ss::path_description cas_route{
      "/cas", ss::operation_type::POST, "cas", {}, {}};
    ss::httpd::function_handler* cas_ptr = new ss::httpd::function_handler(
      cas_handler, "json");
    cas_route.set(_server._routes, cas_ptr);

    return _server.listen(_addr);
}

ss::future<std::unique_ptr<ss::httpd::reply>> httpkvrsm::read(
  std::unique_ptr<ss::httpd::request> req,
  std::unique_ptr<ss::httpd::reply> rep) {
    if (req->query_parameters.find("key") == req->query_parameters.end()) {
        auto body = reject_as_json();
        rep->set_status(ss::httpd::reply::status_type::bad_request);
        rep->write_body("json", body);
        return ss::make_ready_future<std::unique_ptr<ss::httpd::reply>>(
          std::move(rep));
    }

    auto result_f = this->_kvrsm->get_and_wait(
      req->query_parameters["key"],
      model::timeout_clock::now() + timeout(req->query_parameters, 5));

    return result_f.then(
      [rep = std::move(rep)](raft::kvelldb::kvrsm::cmd_result result) mutable {
          auto body = cmd_result_as_json(result);
          rep->set_status(ss::httpd::reply::status_type::ok);
          rep->write_body("json", body);
          return ss::make_ready_future<std::unique_ptr<ss::httpd::reply>>(
            std::move(rep));
      });
}

ss::future<std::unique_ptr<ss::httpd::reply>> httpkvrsm::write(
  std::unique_ptr<ss::httpd::request> req,
  std::unique_ptr<ss::httpd::reply> rep) {
    absl::flat_hash_map<ss::sstring, ss::sstring> content;

    if (!try_parse_json_hash(
          req->content, {"key", "value", "writeID"}, content)) {
        auto body = reject_as_json();
        rep->set_status(ss::httpd::reply::status_type::bad_request);
        rep->write_body("json", body);
        return ss::make_ready_future<std::unique_ptr<ss::httpd::reply>>(
          std::move(rep));
    }

    auto result_f = this->_kvrsm->set_and_wait(
      content["key"],
      content["value"],
      content["writeID"],
      model::timeout_clock::now() + timeout(req->query_parameters, 5));

    return result_f.then(
      [rep = std::move(rep)](raft::kvelldb::kvrsm::cmd_result result) mutable {
          auto body = cmd_result_as_json(result);
          rep->set_status(ss::httpd::reply::status_type::ok);
          rep->write_body("json", body);
          return ss::make_ready_future<std::unique_ptr<ss::httpd::reply>>(
            std::move(rep));
      });
}

ss::future<std::unique_ptr<ss::httpd::reply>> httpkvrsm::cas(
  std::unique_ptr<ss::httpd::request> req,
  std::unique_ptr<ss::httpd::reply> rep) {
    absl::flat_hash_map<ss::sstring, ss::sstring> content;

    if (!try_parse_json_hash(
          req->content, {"key", "value", "writeID", "prevWriteID"}, content)) {
        auto body = reject_as_json();
        rep->set_status(ss::httpd::reply::status_type::bad_request);
        rep->write_body("json", body);
        return ss::make_ready_future<std::unique_ptr<ss::httpd::reply>>(
          std::move(rep));
    }

    auto result_f = this->_kvrsm->cas_and_wait(
      content["key"],
      content["prevWriteID"],
      content["value"],
      content["writeID"],
      model::timeout_clock::now() + timeout(req->query_parameters, 5));

    return result_f.then(
      [rep = std::move(rep)](raft::kvelldb::kvrsm::cmd_result result) mutable {
          auto body = cmd_result_as_json(result);
          rep->set_status(ss::httpd::reply::status_type::ok);
          rep->write_body("json", body);
          return ss::make_ready_future<std::unique_ptr<ss::httpd::reply>>(
            std::move(rep));
      });
}

} // namespace raft::kvelldb