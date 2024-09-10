// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/migrations/tx_manager_migrator.h"

#include "base/vlog.h"
#include "cluster/controller_api.h"
#include "cluster/errc.h"
#include "cluster/migrations/tx_manager_migrator_types.h"
#include "cluster/topics_frontend.h"
#include "cluster/tx_coordinator_mapper.h"
#include "cluster/tx_hash_ranges.h"
#include "cluster/types.h"
#include "config/property.h"
#include "container/fragmented_vector.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/record_batch_types.h"
#include "model/timeout_clock.h"
#include "ssx/future-util.h"
#include "storage/types.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/log.hh>

#include <absl/container/flat_hash_map.h>
#include <absl/container/node_hash_map.h>
#include <boost/range/irange.hpp>

#include <chrono>
#include <system_error>
#include <utility>

namespace cluster {
static ss::logger logger("tx-migration");
namespace {

topic_configuration create_topic_configuration(
  model::topic_namespace_view tp_ns,
  int32_t partition_count,
  int16_t replication_factor) {
    return {tp_ns.ns, tp_ns.tp, partition_count, replication_factor};
}

kafka::transactional_id
read_transactional_id(const model::record_batch& batch) {
    vassert(
      batch.record_count() == 1, "tx manager batches are always of size 1");
    auto records = batch.copy_records();

    iobuf_parser key_parser(records.front().release_key());
    auto batch_type = reflection::adl<model::record_batch_type>{}.from(
      key_parser);
    vassert(
      batch_type == model::record_batch_type::tm_update,
      "only tm_update batch types are expected, current type: {}",
      batch_type);

    reflection::adl<model::producer_id>{}.from(key_parser);
    return reflection::adl<kafka::transactional_id>{}.from(key_parser);
}

template<typename Func>

ss::future<std::error_code> transform_batches(
  tx_manager_read_router& read_router,
  model::ntp source_ntp,
  std::chrono::milliseconds timeout,
  ss::abort_source& as,
  Func transform_f) {
    auto deadline = model::timeout_clock::now() + timeout;

    model::offset last_read;
    while (model::timeout_clock::now() < deadline && !as.abort_requested()) {
        auto read_result = co_await read_router.process_or_dispatch(
          tx_manager_read_request{
            .ntp = source_ntp, .start_offset = model::next_offset(last_read)},
          source_ntp,
          timeout);

        if (read_result.ec != errc::success) {
            vlog(
              logger.warn,
              "error reading batches from {} - {}",
              source_ntp,
              read_result.ec);
            co_await ss::sleep_abortable(1s, as);
            continue;
        }
        if (read_result.batches.empty()) {
            vlog(
              logger.warn,
              "Read no batches from {}, last read offset: {}, log end offset: "
              "{}",
              source_ntp,
              last_read,
              read_result.log_dirty_offset);
            if (last_read >= read_result.log_dirty_offset) {
                vlog(
                  logger.info,
                  "Finished transforming batches from {}",
                  source_ntp);

                co_return errc::success;
            }
            co_await ss::sleep_abortable(1s, as);
            continue;
        }
        const auto batch_last_offset = read_result.batches.back().last_offset();
        vlog(
          logger.info,
          "Read {} batches from {}, last offset: {}, log end offset: {}",
          read_result.batches.size(),
          source_ntp,
          batch_last_offset,
          read_result.log_dirty_offset);

        last_read = batch_last_offset;

        std::error_code transform_ec = co_await transform_f(
          std::move(read_result.batches));
        if (transform_ec) {
            vlog(
              logger.warn,
              "error while transforming batches from {} - {}",
              source_ntp,
              transform_ec.message());
            co_return transform_ec;
        }

        if (last_read >= read_result.log_dirty_offset) {
            vlog(
              logger.info,
              "Finished transforming batches from {} with last offset: {}",
              source_ntp,
              batch_last_offset);

            co_return errc::success;
        }
    }

    co_return errc::timeout;
}

}; // namespace

tx_manager_replicate_handler::tx_manager_replicate_handler(
  ss::sharded<partition_manager>& pm)
  : _partition_manager(pm) {}

tx_manager_read_handler::tx_manager_read_handler(
  ss::sharded<partition_manager>& pm)
  : _partition_manager(pm) {}

tx_manager_read_router::tx_manager_read_router(
  ss::sharded<cluster::partition_manager>& partition_manager,
  ss::sharded<cluster::shard_table>& shard_table,
  ss::sharded<cluster::metadata_cache>& metadata_cache,
  ss::sharded<rpc::connection_cache>& connection_cache,
  ss::sharded<partition_leaders_table>& leaders,
  const model::node_id node_id)
  : leader_router<
      tx_manager_read_request,
      tx_manager_read_reply,
      tx_manager_read_handler>(
      shard_table,
      metadata_cache,
      connection_cache,
      leaders,
      _handler,
      node_id,
      config::shard_local_cfg().metadata_dissemination_retries.value(),
      config::shard_local_cfg().metadata_dissemination_retry_delay_ms.value())
  , _handler(partition_manager) {}

tx_manager_replicate_router::tx_manager_replicate_router(
  ss::sharded<cluster::partition_manager>& partition_manager,
  ss::sharded<cluster::shard_table>& shard_table,
  ss::sharded<cluster::metadata_cache>& metadata_cache,
  ss::sharded<rpc::connection_cache>& connection_cache,
  ss::sharded<partition_leaders_table>& leaders,
  const model::node_id node_id)
  : leader_router<
      tx_manager_replicate_request,
      tx_manager_replicate_reply,
      tx_manager_replicate_handler>(
      shard_table,
      metadata_cache,
      connection_cache,
      leaders,
      _handler,
      node_id,
      config::shard_local_cfg().metadata_dissemination_retries.value(),
      config::shard_local_cfg().metadata_dissemination_retry_delay_ms.value())
  , _handler(partition_manager) {}

ss::future<tx_manager_read_reply> tx_manager_read_handler::do_read(
  partition_manager& pm, model::ntp ntp, model::offset offset) {
    vlog(logger.debug, "Requested read from {} at offset: {}", ntp, offset);
    auto partition = pm.get(ntp);
    if (!partition) {
        co_return tx_manager_read_reply(errc::partition_not_exists);
    }
    storage::log_reader_config reader_cfg(
      std::max(offset, partition->raft_start_offset()),
      model::offset::max(),
      ss::default_priority_class());
    // read up to 128 KiB
    reader_cfg.max_bytes = 128_KiB;
    auto reader = co_await partition->make_reader(std::move(reader_cfg));

    auto batches = co_await model::consume_reader_to_fragmented_memory(
      std::move(reader), model::no_timeout);

    co_return tx_manager_read_reply(
      std::move(batches), partition->dirty_offset());
}

ss::future<tx_manager_replicate_reply>
tx_manager_replicate_handler::do_replicate(
  partition_manager& pm,
  model::ntp ntp,
  fragmented_vector<model::record_batch> batches) {
    vlog(
      logger.info,
      "Requested replication of {} batches to {}",
      batches.size(),
      ntp);
    auto partition = pm.get(ntp);
    if (!partition) {
        co_return tx_manager_replicate_reply{.ec = errc::partition_not_exists};
    }

    auto r = co_await partition->replicate(
      model::make_fragmented_memory_record_batch_reader(std::move(batches)),
      raft::replicate_options(raft::consistency_level::quorum_ack));

    if (r.has_error()) {
        co_return tx_manager_replicate_reply{.ec = errc::replication_error};
    }
    co_return tx_manager_replicate_reply{.ec = errc::success};
}

tx_manager_migrator::tx_manager_migrator(
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
  config::binding<int> requested_partition_count)
  : _topics_frontend(topics_frontend)
  , _controller_api(controller_api)
  , _topics(topics)
  , _read_router(
      partition_manager,
      shard_table,
      metadata_cache,
      connection_cache,
      leaders,
      self)
  , _replicate_router(
      partition_manager,
      shard_table,
      metadata_cache,
      connection_cache,
      leaders,
      self)
  , _internal_topic_replication_factor(internal_topic_replication_factor)
  , _manager_partition_count(std::move(requested_partition_count)) {}

std::chrono::milliseconds tx_manager_migrator::default_timeout = 30s;

ss::future<std::error_code> tx_manager_migrator::migrate() {
    auto units = co_await _migration_mutex.get_units();
    // snapshot partition count for current migration
    _requested_partition_count = _manager_partition_count();
    migration_step current_step = migration_step::create_new_temp_topic;

    const auto current_topic_md = _topics.local().get_topic_metadata_ref(
      model::tx_manager_nt);

    auto temp_topic_md = _topics.local().get_topic_metadata_ref(
      temporary_tx_manager_topic);

    if (!current_topic_md) {
        /**
         * No original and temporary topic exists, there is nothing to
         * migrate, simply create a tx_manager topic with desired number of
         * partitions
         */
        if (!temp_topic_md) {
            vlog(
              logger.info,
              "skipping tx manager topic migration, no previous tx manager "
              "topic "
              "is present",
              _requested_partition_count);
            co_return co_await create_topic(model::tx_manager_nt);
        }
        /**
         * Temporary topic exists but tx_manager topic was deleted, migration
         * was interrupted. Temp topic is now a source of truth, tx info has to
         * be copied over to the new topic
         */
        current_step = migration_step::create_new_tx_manager_topic;
    }
    const auto current_partition_count
      = current_topic_md->get().get_assignments().size();

    /**
     * Both the original one and temporary topic exists, if original topic is
     * older than the temporary one we will recreate the temp topic and start
     * over, otherwise we need to recreate the target tx manager topic and start
     * fresh.
     */
    if (temp_topic_md) {
        // original topic is older than temporary, rehash once again to fresh
        // topic
        if (
          temp_topic_md->get().get_revision()
          > current_topic_md->get().get_revision()) {
            current_step = migration_step::create_new_temp_topic;
        } else {
            // otherwise copying to new tx manager topic may failed, create it
            // once again and retry copy
            current_step = migration_step::delete_old_tx_manager_topic;
        }
    } else {
        // no temp topic exists, and we have a tx manager topic already, this
        // should initiate a migration if there is anything to migrate
        if (
          current_partition_count
          == static_cast<size_t>(_requested_partition_count)) {
            vlog(
              logger.info,
              "current partition count {} is equal to desired partition count "
              "{}, doing nothing",
              current_partition_count,
              _requested_partition_count);

            co_return errc::success;
        }
    }

    vlog(
      logger.info,
      "starting tx manager topic migration from {} to {} partitions",
      current_partition_count,
      _requested_partition_count);

    while (current_step != migration_step::finished) {
        vlog(
          logger.info, "Executing tx manager migration step: {}", current_step);
        switch (current_step) {
        case migration_step::create_new_temp_topic: {
            if (_topics.local().contains(temporary_tx_manager_topic)) {
                co_await delete_topic(temporary_tx_manager_topic);
            }
            co_await create_topic(temporary_tx_manager_topic);
            current_step = migration_step::rehash_tx_manager_topic;
            break;
        }
        case migration_step::rehash_tx_manager_topic: {
            auto results = co_await ssx::parallel_transform(
              boost::irange<model::partition_id>(
                model::partition_id(0),
                model::partition_id(current_partition_count)),
              [this](model::partition_id p_id) {
                  return rehash_and_write_partition_data(p_id);
              });

            auto success = std::all_of(
              results.begin(), results.end(), [](std::error_code ec) {
                  if (ec) {
                      return false;
                  }
                  return true;
              });

            if (!success) {
                vlog(
                  logger.warn,
                  "Error rehashing tx manager topic, results: {}",
                  results);
                // TODO: return correct error here
                co_return errc::partition_operation_failed;
            }
            current_step = migration_step::delete_old_tx_manager_topic;
            break;
        }
        case migration_step::delete_old_tx_manager_topic: {
            auto ec = co_await delete_topic(model::tx_manager_nt);
            if (ec) {
                vlog(
                  logger.warn,
                  "Error deleting tx manager topic - {}",
                  ec.message());
                co_return ec;
            }
            current_step = migration_step::create_new_tx_manager_topic;
            break;
        }
        case migration_step::create_new_tx_manager_topic: {
            auto ec = co_await create_topic(model::tx_manager_nt);
            if (ec) {
                co_return ec;
            }
            current_step = migration_step::copy_temp_to_new_tx_manger;
            break;
        }
        case migration_step::copy_temp_to_new_tx_manger: {
            temp_topic_md = _topics.local().get_topic_metadata_ref(
              temporary_tx_manager_topic);
            if (
              temp_topic_md.value().get().get_assignments().size()
              != static_cast<size_t>(_requested_partition_count)) {
                co_return errc::topic_invalid_partitions;
            }
            auto results = co_await ssx::parallel_transform(
              boost::irange<model::partition_id>(
                model::partition_id(0),
                model::partition_id(_requested_partition_count)),
              [this](model::partition_id p_id) {
                  return copy_from_temporary_to_tx_manager_topic(p_id);
              });

            auto success = std::all_of(
              results.begin(), results.end(), [](std::error_code ec) {
                  if (ec) {
                      return false;
                  }
                  return true;
              });

            if (!success) {
                vlog(
                  logger.warn,
                  "Error copying data to new tx_manager topic, results: {}",
                  results);
                co_return errc::partition_operation_failed;
            }
            current_step = migration_step::delete_temp_topic;
            break;
        }
        case migration_step::delete_temp_topic: {
            auto ec = co_await delete_topic(temporary_tx_manager_topic);
            if (ec) {
                vlog(
                  logger.warn,
                  "Error deleting temp tx manager topic - {}",
                  ec.message());
                co_return errc::partition_operation_failed;
            }
            current_step = migration_step::finished;
            break;
        }
        case migration_step::finished:
            vlog(logger.info, "Finished migrating tx manager topic");
            co_return errc::success;
        }
    }
    co_return errc::success;
}
tx_manager_migrator::status tx_manager_migrator::get_status() const {
    return {
      .migration_in_progress = !_migration_mutex.ready(),
      .migration_required = is_migration_required(),
    };
}

bool tx_manager_migrator::is_migration_required() const {
    const auto current_topic_md = _topics.local().get_topic_metadata_ref(
      model::tx_manager_nt);

    auto temp_topic_md = _topics.local().get_topic_metadata_ref(
      temporary_tx_manager_topic);

    // temp topic should not exists after migration
    if (temp_topic_md) {
        return true;
    }
    // partition count in tx manager topic differs
    if (
      current_topic_md
      && current_topic_md.value().get().get_assignments().size()
           != static_cast<size_t>(_manager_partition_count())) {
        return true;
    }

    return false;
}

// create new temporary topic with updated number of partitions

ss::future<std::error_code>
tx_manager_migrator::create_topic(model::topic_namespace_view tp_ns) {
    vlog(
      logger.info,
      "Creating topic {} with {} partitions",
      tp_ns,
      _requested_partition_count);

    auto result = co_await _topics_frontend.local().autocreate_topics(
      {create_topic_configuration(
        tp_ns, _requested_partition_count, _internal_topic_replication_factor)},
      default_timeout);
    vassert(
      result.size() == 1,
      "Expected to have one result as only one topic has been created. "
      "Current "
      "size: {}",
      result.size());
    if (result.front().ec != errc::success) {
        co_return result.front().ec;
    }

    co_return co_await _controller_api.local().wait_for_topic(
      tp_ns, deadline());
}

ss::future<std::error_code>
tx_manager_migrator::rehash_and_write_partition_data(
  model::partition_id source_partition_id) {
    model::ntp source_ntp(
      model::kafka_internal_namespace,
      model::tx_manager_nt.tp,
      source_partition_id);

    co_return co_await transform_batches(
      _read_router,
      std::move(source_ntp),
      default_timeout,
      _as,
      [this,
       source_partition_id](fragmented_vector<model::record_batch> batches) {
          return rehash_chunk(source_partition_id, std::move(batches));
      });
}

ss::future<std::error_code> tx_manager_migrator::rehash_chunk(
  model::partition_id source_partition_id,
  fragmented_vector<model::record_batch> batches) {
    absl::
      flat_hash_map<model::partition_id, fragmented_vector<model::record_batch>>
        target_partition_batches;
    target_partition_batches.reserve(_requested_partition_count);
    for (auto& b : batches) {
        // we are not interested in batches not related with tx coordinator
        // stm
        if (b.header().type != model::record_batch_type::tm_update) {
            continue;
        }
        auto tx_id = read_transactional_id(b);
        auto target_partition_id = get_tx_coordinator_partition(
          get_tx_id_hash(tx_id), _requested_partition_count);
        vlog(
          logger.trace,
          "Moving {} transactional id from partition {} to partition {}",
          tx_id,
          source_partition_id,
          target_partition_id);

        target_partition_batches[target_partition_id].push_back(std::move(b));
    }

    for (auto& [p_id, batches] : target_partition_batches) {
        model::ntp target_ntp(
          temporary_tx_manager_topic.ns, temporary_tx_manager_topic.tp, p_id);

        auto res = co_await _replicate_router.process_or_dispatch(
          tx_manager_replicate_request(target_ntp, std::move(batches)),
          target_ntp,
          default_timeout);

        if (res.ec != errc::success) {
            co_return res.ec;
        }
    }
    co_return errc::success;
}

ss::future<std::error_code>
tx_manager_migrator::delete_topic(model::topic_namespace_view topic_namespace) {
    auto res = co_await _topics_frontend.local().dispatch_delete_topics(
      {model::topic_namespace(topic_namespace)}, default_timeout);

    vassert(
      res.size() == 1,
      "Expected result size must be equal to 1 as one topic delete was "
      "requested. Current size: {}",
      res.size());
    co_return res.front().ec;
}

ss::future<std::error_code>
tx_manager_migrator::copy_from_temporary_to_tx_manager_topic(
  model::partition_id p_id) {
    /**
     * copy from temp topic to new tx manager topic
     */
    model::ntp source_ntp(
      model::kafka_internal_namespace, temporary_tx_manager_topic.tp, p_id);
    model::ntp target_ntp(
      model::kafka_internal_namespace, model::tx_manager_nt.tp, p_id);
    vlog(
      logger.info,
      "copying tx_manager batches from temporary partition {} to {}",
      source_ntp,
      target_ntp);

    co_return co_await transform_batches(
      _read_router,
      std::move(source_ntp),
      default_timeout,
      _as,
      [this, target_ntp = std::move(target_ntp)](
        fragmented_vector<model::record_batch> all_batches) {
          fragmented_vector<model::record_batch> tx_batches;
          for (auto& b : all_batches) {
              if (b.header().type == model::record_batch_type::tm_update) {
                  tx_batches.push_back(std::move(b));
              }
          }
          if (tx_batches.empty()) {
              return ss::make_ready_future<std::error_code>(errc::success);
          }
          return _replicate_router
            .process_or_dispatch(
              tx_manager_replicate_request(target_ntp, std::move(tx_batches)),
              target_ntp,
              default_timeout)
            .then([](tx_manager_replicate_reply reply) {
                return make_error_code(reply.ec);
            });
      });
}

ss::future<> tx_manager_migrator::stop() {
    _as.request_abort();
    co_return;
}

std::ostream&
operator<<(std::ostream& o, tx_manager_migrator::migration_step step) {
    switch (step) {
    case tx_manager_migrator::migration_step::create_new_temp_topic:
        return o << "create_new_temp_topic";
    case tx_manager_migrator::migration_step::rehash_tx_manager_topic:
        return o << "rehash_tx_manager_topic";
    case tx_manager_migrator::migration_step::delete_old_tx_manager_topic:
        return o << "delete_old_tx_manager_topic";
    case tx_manager_migrator::migration_step::create_new_tx_manager_topic:
        return o << "create_new_tx_manager_topic";
    case tx_manager_migrator::migration_step::copy_temp_to_new_tx_manger:
        return o << "copy_temp_to_new_tx_manger";
    case tx_manager_migrator::migration_step::delete_temp_topic:
        return o << "delete_temp_topic";
    case tx_manager_migrator::migration_step::finished:
        return o << "finished";
    }
    __builtin_unreachable();
}

} // namespace cluster
