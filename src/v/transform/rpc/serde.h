/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "cluster/errc.h"
#include "model/record.h"
#include "model/timeout_clock.h"
#include "model/transform.h"
#include "serde/envelope.h"
#include "utils/uuid.h"

#include <seastar/core/chunked_fifo.hh>

#include <absl/container/flat_hash_map.h>

namespace transform::rpc {

struct transformed_topic_data
  : serde::envelope<
      transformed_topic_data,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    transformed_topic_data() = default;
    transformed_topic_data(model::topic_partition, model::record_batch);
    transformed_topic_data(
      model::topic_partition, ss::chunked_fifo<model::record_batch>);

    model::topic_partition tp;
    ss::chunked_fifo<model::record_batch> batches;

    transformed_topic_data share();

    auto serde_fields() { return std::tie(tp, batches); }
};

struct produce_request
  : serde::
      envelope<produce_request, serde::version<0>, serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    produce_request() = default;
    produce_request(
      ss::chunked_fifo<transformed_topic_data> topic_data,
      model::timeout_clock::duration timeout)
      : topic_data{std::move(topic_data)}
      , timeout{timeout} {}

    auto serde_fields() { return std::tie(topic_data, timeout); }

    produce_request share();

    ss::chunked_fifo<transformed_topic_data> topic_data;
    model::timeout_clock::duration timeout{};
};

struct transformed_topic_data_result
  : serde::envelope<
      transformed_topic_data_result,
      serde::version<0>,
      serde::compat_version<0>> {
    transformed_topic_data_result() = default;
    transformed_topic_data_result(model::topic_partition tp, cluster::errc ec)
      : tp(std::move(tp))
      , err(ec) {}

    model::topic_partition tp;
    cluster::errc err{cluster::errc::success};

    auto serde_fields() { return std::tie(tp, err); }
};

struct produce_reply
  : serde::
      envelope<produce_reply, serde::version<0>, serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    produce_reply() = default;
    explicit produce_reply(ss::chunked_fifo<transformed_topic_data_result> r)
      : results(std::move(r)) {}

    auto serde_fields() { return std::tie(results); }

    ss::chunked_fifo<transformed_topic_data_result> results;
};

struct store_wasm_binary_request
  : serde::envelope<
      store_wasm_binary_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    store_wasm_binary_request() = default;
    explicit store_wasm_binary_request(
      iobuf d, model::timeout_clock::duration t)
      : data(std::move(d))
      , timeout(t) {}

    auto serde_fields() { return std::tie(data, timeout); }

    iobuf data;
    model::timeout_clock::duration timeout{};
};

struct stored_wasm_binary_metadata
  : serde::envelope<
      stored_wasm_binary_metadata,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    stored_wasm_binary_metadata() = default;
    stored_wasm_binary_metadata(uuid_t k, model::offset o)
      : key(k)
      , offset(o){};

    auto serde_fields() { return std::tie(key, offset); }

    uuid_t key{};
    model::offset offset;
};

struct store_wasm_binary_reply
  : serde::envelope<
      store_wasm_binary_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    store_wasm_binary_reply() = default;
    explicit store_wasm_binary_reply(
      cluster::errc e, stored_wasm_binary_metadata s)
      : ec(e)
      , stored(s) {}

    auto serde_fields() { return std::tie(ec, stored); }

    cluster::errc ec = cluster::errc::success;
    stored_wasm_binary_metadata stored;
};

struct delete_wasm_binary_request
  : serde::envelope<
      delete_wasm_binary_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    delete_wasm_binary_request() = default;
    explicit delete_wasm_binary_request(
      uuid_t k, model::timeout_clock::duration t)
      : key(k)
      , timeout(t) {}

    auto serde_fields() { return std::tie(key, timeout); }

    uuid_t key{};
    model::timeout_clock::duration timeout{};
};

struct delete_wasm_binary_reply
  : serde::envelope<
      store_wasm_binary_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    delete_wasm_binary_reply() = default;
    explicit delete_wasm_binary_reply(cluster::errc e)
      : ec(e) {}

    auto serde_fields() { return std::tie(ec); }

    cluster::errc ec = cluster::errc::success;
};

struct load_wasm_binary_request
  : serde::envelope<
      store_wasm_binary_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    load_wasm_binary_request() = default;
    explicit load_wasm_binary_request(
      model::offset o, model::timeout_clock::duration t)
      : offset(o)
      , timeout(t) {}

    auto serde_fields() { return std::tie(offset, timeout); }

    model::offset offset;
    model::timeout_clock::duration timeout{};
};

struct load_wasm_binary_reply
  : serde::envelope<
      store_wasm_binary_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    load_wasm_binary_reply() = default;
    explicit load_wasm_binary_reply(cluster::errc e, iobuf b)
      : ec(e)
      , data(std::move(b)) {}

    auto serde_fields() { return std::tie(ec, data); }

    cluster::errc ec = cluster::errc::success;
    iobuf data;
};
} // namespace transform::rpc
