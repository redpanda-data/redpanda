/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "base/format_to.h"
#include "cluster/errc.h"
#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/timeout_clock.h"
#include "serde/envelope.h"

#include <seastar/core/chunked_fifo.hh>

#include <iosfwd>

namespace kafka::data::rpc {

struct kafka_topic_data
  : serde::
      envelope<kafka_topic_data, serde::version<0>, serde::compat_version<0>> {
    kafka_topic_data() = default;
    kafka_topic_data(model::topic_partition, model::record_batch);
    kafka_topic_data(
      model::topic_partition, ss::chunked_fifo<model::record_batch>);

    model::topic_partition tp;
    ss::chunked_fifo<model::record_batch> batches;

    kafka_topic_data share();

    auto serde_fields() { return std::tie(tp, batches); }

    fmt::iterator format_to(fmt::iterator it) const;
};

struct produce_request
  : serde::
      envelope<produce_request, serde::version<0>, serde::compat_version<0>> {
    produce_request() = default;
    produce_request(
      ss::chunked_fifo<kafka_topic_data> topic_data,
      model::timeout_clock::duration timeout)
      : topic_data{std::move(topic_data)}
      , timeout{timeout} {}

    auto serde_fields() { return std::tie(topic_data, timeout); }

    produce_request share();

    ss::chunked_fifo<kafka_topic_data> topic_data;
    model::timeout_clock::duration timeout{};

    fmt::iterator format_to(fmt::iterator it) const;
};

struct kafka_topic_data_result
  : serde::envelope<
      kafka_topic_data_result,
      serde::version<0>,
      serde::compat_version<0>> {
    kafka_topic_data_result() = default;
    kafka_topic_data_result(model::topic_partition tp, cluster::errc ec)
      : tp(std::move(tp))
      , err(ec) {}

    model::topic_partition tp;
    cluster::errc err{cluster::errc::success};

    auto serde_fields() { return std::tie(tp, err); }

    fmt::iterator format_to(fmt::iterator it) const;
};

struct produce_reply
  : serde::
      envelope<produce_reply, serde::version<0>, serde::compat_version<0>> {
    produce_reply() = default;
    explicit produce_reply(ss::chunked_fifo<kafka_topic_data_result> r)
      : results(std::move(r)) {}

    auto serde_fields() { return std::tie(results); }

    ss::chunked_fifo<kafka_topic_data_result> results;

    fmt::iterator format_to(fmt::iterator it) const;
};
struct topic_partitions
  : serde::
      envelope<topic_partitions, serde::version<0>, serde::compat_version<0>> {
    auto serde_fields() { return std::tie(topic, partitions); }

    topic_partitions copy() const {
        return topic_partitions{
          .topic = topic, .partitions = partitions.copy()};
    }

    model::topic topic;
    chunked_vector<model::partition_id> partitions;

    fmt::iterator format_to(fmt::iterator it) const;
};

struct partition_offsets
  : serde::
      envelope<partition_offsets, serde::version<0>, serde::compat_version<0>> {
    auto serde_fields() { return std::tie(high_watermark, last_stable_offset); }

    kafka::offset high_watermark;
    kafka::offset last_stable_offset;

    fmt::iterator format_to(fmt::iterator it) const;
};
struct partition_offset_result
  : serde::envelope<
      partition_offset_result,
      serde::version<0>,
      serde::compat_version<0>> {
    partition_offset_result() = default;

    explicit partition_offset_result(cluster::errc err)
      : err(err) {}

    explicit partition_offset_result(partition_offsets offsets)
      : offsets(offsets) {}

    auto serde_fields() { return std::tie(err, offsets); }

    cluster::errc err{cluster::errc::success};
    partition_offsets offsets;

    fmt::iterator format_to(fmt::iterator it) const;
};
using partition_offsets_map = chunked_hash_map<
  model::topic,
  chunked_hash_map<model::partition_id, partition_offset_result>>;

struct get_offsets_request
  : serde::envelope<
      get_offsets_request,
      serde::version<0>,
      serde::compat_version<0>> {
    get_offsets_request() = default;

    explicit get_offsets_request(chunked_vector<topic_partitions> topics)
      : topics(std::move(topics)) {}

    auto serde_fields() { return std::tie(topics); }

    chunked_vector<topic_partitions> topics;

    fmt::iterator format_to(fmt::iterator it) const;
};
struct get_offsets_reply
  : serde::
      envelope<get_offsets_reply, serde::version<0>, serde::compat_version<0>> {
    get_offsets_reply() = default;

    explicit get_offsets_reply(partition_offsets_map partition_offsets)
      : partition_offsets(std::move(partition_offsets)) {}

    auto serde_fields() { return std::tie(partition_offsets); }

    partition_offsets_map partition_offsets;

    fmt::iterator format_to(fmt::iterator it) const;
};

struct consume_request
  : serde::
      envelope<consume_request, serde::version<0>, serde::compat_version<0>> {
    consume_request() = default;
    consume_request(
      model::topic_partition tp,
      kafka::offset start_offset,
      kafka::offset max_offset,
      size_t min_bytes,
      size_t max_bytes,
      model::timeout_clock::duration timeout)
      : tp(std::move(tp))
      , start_offset(start_offset)
      , max_offset(max_offset)
      , min_bytes(min_bytes)
      , max_bytes(max_bytes)
      , timeout(timeout) {}

    auto serde_fields() {
        return std::tie(
          tp, start_offset, max_offset, min_bytes, max_bytes, timeout);
    }

    model::topic_partition tp;
    kafka::offset start_offset;
    kafka::offset max_offset;
    size_t min_bytes;
    size_t max_bytes;
    model::timeout_clock::duration timeout{};

    fmt::iterator format_to(fmt::iterator it) const;
};

struct consume_reply
  : serde::
      envelope<consume_reply, serde::version<0>, serde::compat_version<0>> {
    consume_reply() = default;
    consume_reply(
      model::topic_partition tp,
      cluster::errc err,
      chunked_vector<model::record_batch> batches)
      : tp(std::move(tp))
      , err(err)
      , batches(std::move(batches)) {}

    auto serde_fields() { return std::tie(tp, err, batches); }

    model::topic_partition tp;
    cluster::errc err{cluster::errc::success};
    chunked_vector<model::record_batch> batches;

    fmt::iterator format_to(fmt::iterator it) const;
};
} // namespace kafka::data::rpc
