// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once
#include "cluster/fwd.h"
#include "cluster/leader_router.h"
#include "cluster/migrations/tx_manager_migrator_types.h"
#include "cluster/partition_manager.h"
#include "cluster/tx_manager_migrator_service.h"
#include "config/property.h"
#include "container/fragmented_vector.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/timeout_clock.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/sharded.hh>

#include <chrono>
#include <iosfwd>
#include <system_error>

namespace cluster {
inline const model::topic_namespace temporary_tx_manager_topic(
  model::kafka_internal_namespace, model::topic("tx_manager_tmp"));
/**
 * The `tx_manager_migrator` is a simple service that is only instantiated in
 * Recovery mode. The migrator is responsible for rehashing transaction ids to
 * change the number of partitions in transactional coordinator topic.
 *
 * The migration is executed in the following steps:
 *
 * 1) Create a temporary topic for new partitioning scheme
 * 2) For each partition of current tx manager topic read all the data, assign
 *    new partition and replicate according to the new scheme
 * 3) Delete current tx manager topic
 * 4) Create tx manager topic with new number of partitions
 * 5) Replicate data from temporary topic to the new tx manager topic.
 * 6) Delete temporary topic
 */
class tx_manager_read_handler {
public:
    explicit tx_manager_read_handler(ss::sharded<partition_manager>&);

    using proto_t = tx_manager_migrator_client_protocol;
    static ss::sstring process_name() { return "tx_manager_read"; }
    static tx_manager_read_reply error_resp(cluster::errc e) {
        return tx_manager_read_reply(e);
    }

    static ss::future<result<rpc::client_context<tx_manager_read_reply>>>
    dispatch(
      proto_t proto,
      tx_manager_read_request req,
      model::timeout_clock::duration timeout) {
        return proto.tx_manager_read(
          std::move(req),
          rpc::client_opts(model::timeout_clock::now() + timeout));
    }

    ss::future<tx_manager_read_reply>
    process(ss::shard_id shard, tx_manager_read_request req) {
        auto reply = co_await _partition_manager.invoke_on(
          shard, [this, r = std::move(req)](partition_manager& pm) mutable {
              return do_read(pm, std::move(r.ntp), r.start_offset);
          });
        co_return reply;
    }

private:
    ss::future<tx_manager_read_reply>
    do_read(partition_manager&, model::ntp, model::offset);
    ss::sharded<partition_manager>& _partition_manager;
};

class tx_manager_replicate_handler {
public:
    explicit tx_manager_replicate_handler(ss::sharded<partition_manager>&);

    using proto_t = tx_manager_migrator_client_protocol;
    static ss::sstring process_name() { return "tx_manager_replicate"; }

    static tx_manager_replicate_reply error_resp(cluster::errc e) {
        return tx_manager_replicate_reply{.ec = e};
    }

    static ss::future<result<rpc::client_context<tx_manager_replicate_reply>>>
    dispatch(
      proto_t proto,
      tx_manager_replicate_request req,
      model::timeout_clock::duration timeout) {
        return proto.tx_manager_replicate(
          std::move(req),
          rpc::client_opts(model::timeout_clock::now() + timeout));
    }

    ss::future<tx_manager_replicate_reply>
    process(ss::shard_id shard, tx_manager_replicate_request req) {
        auto reply = co_await _partition_manager.invoke_on(
          shard, [this, r = std::move(req)](partition_manager& pm) mutable {
              return do_replicate(pm, std::move(r.ntp), std::move(r.batches));
          });
        co_return reply;
    }

private:
    ss::future<tx_manager_replicate_reply> do_replicate(
      partition_manager&, model::ntp, fragmented_vector<model::record_batch>);

    ss::sharded<partition_manager>& _partition_manager;
};
/**
 * Routers to dispatch read/replicate requests to correct node and shard
 */
class tx_manager_read_router
  : public leader_router<
      tx_manager_read_request,
      tx_manager_read_reply,
      tx_manager_read_handler> {
public:
    tx_manager_read_router(
      ss::sharded<cluster::partition_manager>& partition_manager,
      ss::sharded<cluster::shard_table>& shard_table,
      ss::sharded<cluster::metadata_cache>& metadata_cache,
      ss::sharded<rpc::connection_cache>& connection_cache,
      ss::sharded<partition_leaders_table>& leaders,
      const model::node_id node_id);

private:
    tx_manager_read_handler _handler;
};

class tx_manager_replicate_router
  : public leader_router<
      tx_manager_replicate_request,
      tx_manager_replicate_reply,
      tx_manager_replicate_handler> {
public:
    tx_manager_replicate_router(
      ss::sharded<cluster::partition_manager>& partition_manager,
      ss::sharded<cluster::shard_table>& shard_table,
      ss::sharded<cluster::metadata_cache>& metadata_cache,
      ss::sharded<rpc::connection_cache>& connection_cache,
      ss::sharded<partition_leaders_table>& leaders,
      const model::node_id node_id);

private:
    tx_manager_replicate_handler _handler;
};

class tx_manager_migrator {
public:
    struct status {
        bool migration_in_progress;
        bool migration_required;
    };
    static std::chrono::milliseconds default_timeout;
    tx_manager_migrator(
      ss::sharded<topics_frontend>& topics_frontend,
      ss::sharded<controller_api>& controller_api,
      ss::sharded<topic_table>& topics,
      ss::sharded<cluster::partition_manager>& partition_manager,
      ss::sharded<cluster::shard_table>& shard_table,
      ss::sharded<cluster::metadata_cache>& metadata_cache,
      ss::sharded<rpc::connection_cache>& connection_cache,
      ss::sharded<partition_leaders_table>& leaders,
      model::node_id self,
      int16_t internal_topic_replication_factor,
      config::binding<int> requested_partition_count);

    ss::future<std::error_code> migrate();

    status get_status() const;

    ss::future<> stop();

private:
    enum class migration_step {
        create_new_temp_topic,
        rehash_tx_manager_topic,
        delete_old_tx_manager_topic,
        create_new_tx_manager_topic,
        copy_temp_to_new_tx_manger,
        delete_temp_topic,
        finished,
    };

    ss::future<std::error_code> create_topic(model::topic_namespace_view topic);

    ss::future<std::error_code>
    rehash_and_write_partition_data(model::partition_id source_partition_id);
    ss::future<std::error_code> rehash_chunk(
      model::partition_id source_partition_id,
      fragmented_vector<model::record_batch> batches);

    ss::future<std::error_code>
      copy_from_temporary_to_tx_manager_topic(model::partition_id);

    ss::future<std::error_code> delete_topic(model::topic_namespace_view);

    static model::timeout_clock::time_point deadline() {
        return default_timeout + model::timeout_clock::now();
    }

    bool is_migration_required() const;

    friend std::ostream& operator<<(std::ostream&, migration_step);
    ss::sharded<topics_frontend>& _topics_frontend;
    ss::sharded<controller_api>& _controller_api;
    ss::sharded<topic_table>& _topics;
    tx_manager_read_router _read_router;
    tx_manager_replicate_router _replicate_router;
    int16_t _internal_topic_replication_factor;
    config::binding<int> _manager_partition_count;
    int32_t _requested_partition_count;
    mutex _migration_mutex{"tx_manager_migrator"};

    ss::abort_source _as;
};
} // namespace cluster
