/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#pragma once
#include "coproc/script_manager.h"
#include "coproc/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/record_batch_reader.h"
#include "model/record_batch_types.h"
#include "raft/types.h"
#include "storage/types.h"

#include <seastar/core/future.hh>

inline static model::topic_namespace make_ts(ss::sstring&& topic) {
    return model::topic_namespace(model::kafka_namespace, model::topic(topic));
}

inline static model::topic_namespace make_ts(const model::topic& topic) {
    return model::topic_namespace(model::kafka_namespace, topic);
}

inline static storage::log_reader_config
log_rdr_cfg(const model::offset& min_offset) {
    return storage::log_reader_config(
      min_offset,
      model::model_limits<model::offset>::max(),
      0,
      std::numeric_limits<size_t>::max(),
      ss::default_priority_class(),
      raft::data_batch_type,
      std::nullopt,
      std::nullopt);
}

inline static storage::log_reader_config log_rdr_cfg(const size_t min_bytes) {
    return storage::log_reader_config(
      model::offset(0),
      model::model_limits<model::offset>::max(),
      min_bytes,
      std::numeric_limits<size_t>::max(),
      ss::default_priority_class(),
      raft::data_batch_type,
      std::nullopt,
      std::nullopt);
}

/// \brief returns the number of records across all batches
std::size_t sum_records(const model::record_batch_reader::data_t&);

/// \brief short way to construct an enable_copros_request::data
coproc::enable_copros_request::data make_enable_req(
  uint32_t id,
  std::vector<std::pair<ss::sstring, coproc::topic_ingestion_policy>>);

/// \brief Register coprocessors with redpanda
ss::future<result<rpc::client_context<coproc::enable_copros_reply>>>
register_coprocessors(
  rpc::client<coproc::script_manager_client_protocol>&,
  std::vector<coproc::enable_copros_request::data>&&);

/// \brief Deregister coprocessors with redpanda
ss::future<result<rpc::client_context<coproc::disable_copros_reply>>>
deregister_coprocessors(
  rpc::client<coproc::script_manager_client_protocol>&,
  std::vector<uint32_t>&&);

/// \brief Return a client ready to connect to redpandas script manager svc
inline rpc::client<coproc::script_manager_client_protocol> make_client() {
    return rpc::client<coproc::script_manager_client_protocol>(
      rpc::transport_configuration{
        .server_addr = ss::socket_address(
          ss::net::inet_address("127.0.0.1"), 43118),
        .credentials = nullptr});
}

ss::future<model::record_batch_reader::data_t>
copy_batch(const model::record_batch_reader::data_t&);
