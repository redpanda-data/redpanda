/* Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "transform/rpc/serde.h"

#include "model/record.h"
#include "utils/to_string.h"

#include <seastar/core/chunked_fifo.hh>

#include <fmt/format.h>

namespace transform::rpc {
transformed_topic_data::transformed_topic_data(
  model::topic_partition tp, model::record_batch b)
  : tp(std::move(tp)) {
    batches.reserve(1);
    batches.push_back(std::move(b));
}

transformed_topic_data::transformed_topic_data(
  model::topic_partition tp, ss::chunked_fifo<model::record_batch> b)
  : tp(std::move(tp))
  , batches(std::move(b)) {}

transformed_topic_data transformed_topic_data::share() {
    ss::chunked_fifo<model::record_batch> shared;
    shared.reserve(batches.size());
    for (auto& batch : batches) {
        shared.push_back(batch.share());
    }
    return {tp, std::move(shared)};
}

produce_request produce_request::share() {
    ss::chunked_fifo<transformed_topic_data> shared;
    shared.reserve(topic_data.size());
    for (auto& data : topic_data) {
        shared.push_back(data.share());
    }
    return {std::move(shared), timeout};
}

std::ostream& operator<<(std::ostream& os, const offset_commit_request& req) {
    fmt::print(
      os, "{{ kvs: {}, coordinator: {} }}", req.kvs.size(), req.coordinator);
    return os;
}

std::ostream& operator<<(std::ostream& os, const offset_commit_response& resp) {
    fmt::print(os, "{{ errc: {} }}", resp.errc);
    return os;
}

std::ostream&
operator<<(std::ostream& os, const find_coordinator_request& req) {
    fmt::print(os, "{{ num_keys: {} }}", req.keys.size());
    return os;
}

std::ostream&
operator<<(std::ostream& os, const find_coordinator_response& resp) {
    fmt::print(
      os,
      "{{ coordinators: {}, errors: {} }}",
      resp.coordinators.size(),
      resp.errors.size());
    return os;
}

std::ostream& operator<<(std::ostream& os, const generate_report_request&) {
    fmt::print(os, "{{ }}");
    return os;
}

std::ostream& operator<<(std::ostream& os, const generate_report_reply& reply) {
    fmt::print(os, "{{ transforms: {} }}", reply.report.transforms.size());
    return os;
}

std::ostream& operator<<(std::ostream& os, const offset_fetch_request& resp) {
    fmt::print(
      os,
      "{{ keys: {}, coordinator: {} }}",
      resp.keys.size(),
      resp.coordinator);
    return os;
}

std::ostream& operator<<(std::ostream& os, const offset_fetch_response& resp) {
    fmt::print(
      os,
      "{{ errc: {}, results: {} }}",
      resp.errors.size(),
      resp.results.size());
    return os;
}

std::ostream&
operator<<(std::ostream& os, const load_wasm_binary_request& req) {
    fmt::print(os, "{{ offset: {}, timeout: {} }}", req.offset, req.timeout);
    return os;
}

std::ostream&
operator<<(std::ostream& os, const load_wasm_binary_reply& reply) {
    fmt::print(
      os,
      "{{ data_size: {}, errc: {} }}",
      reply.data()->size_bytes(),
      reply.ec);
    return os;
}

std::ostream&
operator<<(std::ostream& os, const delete_wasm_binary_reply& reply) {
    fmt::print(os, "{{ errc: {} }}", reply.ec);
    return os;
}

std::ostream&
operator<<(std::ostream& os, const delete_wasm_binary_request& req) {
    fmt::print(os, "{{ key: {}, timeout: {} }}", req.key, req.timeout);
    return os;
}

std::ostream&
operator<<(std::ostream& os, const store_wasm_binary_reply& reply) {
    fmt::print(os, "{{ errc: {}, stored: {} }}", reply.ec, reply.stored);
    return os;
}

std::ostream&
operator<<(std::ostream& os, const stored_wasm_binary_metadata& meta) {
    fmt::print(os, "{{ key: {}, offset: {} }}", meta.key, meta.offset);
    return os;
}

std::ostream&
operator<<(std::ostream& os, const store_wasm_binary_request& req) {
    fmt::print(
      os,
      "{{ data_size: {}, timeout: {} }}",
      req.data()->size_bytes(),
      req.timeout);
    return os;
}

std::ostream& operator<<(std::ostream& os, const produce_request& req) {
    fmt::print(
      os,
      "{{ topic_data: {}, timeout: {} }}",
      fmt::join(req.topic_data, ", "),
      req.timeout);
    return os;
}

std::ostream& operator<<(std::ostream& os, const produce_reply& reply) {
    fmt::print(os, "{{ results: {} }}", fmt::join(reply.results, ", "));
    return os;
}

std::ostream&
operator<<(std::ostream& os, const transformed_topic_data_result& result) {
    fmt::print(os, "{{ errc: {}, tp: {} }}", result.err, result.tp);
    return os;
}

std::ostream& operator<<(std::ostream& os, const transformed_topic_data& data) {
    fmt::print(
      os, "{{ tp: {}, batches_size: {} }}", data.tp, data.batches.size());
    return os;
}

std::ostream& operator<<(std::ostream& os, const list_commits_request& req) {
    fmt::print(os, "{{ partition: {} }}", req.partition);
    return os;
}

std::ostream& operator<<(std::ostream& os, const list_commits_reply& reply) {
    fmt::print(os, "{{ ec: {}, map_size: {} }}", reply.errc, reply.map.size());
    return os;
}

std::ostream& operator<<(std::ostream& os, const delete_commits_request& req) {
    fmt::print(
      os,
      "{{ partition: {}, transform_ids_size: {} }}",
      req.partition,
      req.ids.size());
    return os;
}

std::ostream& operator<<(std::ostream& os, const delete_commits_reply& reply) {
    fmt::print(os, "{{ ec: {} }}", reply.errc);
    return os;
}

} // namespace transform::rpc
