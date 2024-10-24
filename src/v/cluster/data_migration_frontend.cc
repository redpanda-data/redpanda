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
#include "cluster/data_migration_frontend.h"

#include "cluster/cluster_utils.h"
#include "cluster/commands.h"
#include "cluster/controller_stm.h"
#include "cluster/data_migration_rpc_service.h"
#include "cluster/data_migration_table.h"
#include "cluster/data_migration_types.h"
#include "cluster/errc.h"
#include "cluster/logger.h"
#include "features/feature_table.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "model/timeout_clock.h"
#include "partition_leaders_table.h"
#include "rpc/connection_cache.h"
#include "ssx/future-util.h"
#include "ssx/single_sharded.h"

#include <fmt/ostream.h>

namespace cluster::data_migrations {

frontend::frontend(
  model::node_id self,
  bool cloud_storage_api_initialized,
  ssx::single_sharded<migrations_table>& table,
  ss::sharded<features::feature_table>& features,
  ss::sharded<controller_stm>& stm,
  ss::sharded<partition_leaders_table>& leaders,
  ss::sharded<rpc::connection_cache>& connections,
  ss::sharded<ss::abort_source>& as)
  : _self(self)
  , _cloud_storage_api_initialized(cloud_storage_api_initialized)
  , _table(table)
  , _features(features)
  , _controller(stm)
  , _leaders_table(leaders)
  , _connections(connections)
  , _as(as)
  , _operation_timeout(10s) {}

template<
  typename Request,
  typename Reply,
  typename DispatchFunc,
  typename ProcessFunc,
  typename ReplyMapperFunc>
ss::future<std::invoke_result_t<ReplyMapperFunc, result<Reply>>>
frontend::process_or_dispatch(
  Request req,
  can_dispatch_to_leader can_dispatch,
  DispatchFunc dispatch,
  ProcessFunc process,
  ReplyMapperFunc reply_mapper) {
    auto controller_leader = _leaders_table.local().get_leader(
      model::controller_ntp);
    /// Return early if there is no controller leader
    if (!controller_leader) {
        vlog(
          dm_log.warn,
          "unable to process request {}, no controller present",
          req);

        co_return errc::no_leader_controller;
    }

    /// If current node is a leader, process request
    if (controller_leader == _self) {
        vlog(
          dm_log.debug,
          "current node is controller leader, processing request {}",
          req);
        co_return co_await process(std::move(req));
    }

    /// If current node is not a leader and request can not be dispatched,
    /// return error
    if (!can_dispatch) {
        co_return errc::not_leader;
    }

    vlog(
      dm_log.debug,
      "dispatching request {} to controller leader at {}",
      req,
      *controller_leader);
    /// If leader is somewhere else, dispatch RPC request to current leader
    auto reply = co_await _connections.local()
                   .with_node_client<data_migrations_client_protocol>(
                     _self,
                     ss::this_shard_id(),
                     *controller_leader,
                     _operation_timeout,
                     [req = std::move(req),
                      dispatch = std::forward<DispatchFunc>(dispatch)](
                       data_migrations_client_protocol client) mutable {
                         return dispatch(std::move(req), client)
                           .then(&rpc::get_ctx_data<Reply>);
                     });
    vlog(
      dm_log.debug,
      "got reply {} from controller leader at {}",
      reply,
      *controller_leader);
    co_return reply_mapper(std::move(reply));
}

bool frontend::data_migrations_active() const {
    return _features.local().is_active(features::feature::data_migrations)
           && _cloud_storage_api_initialized;
}

ss::future<result<id>> frontend::create_migration(
  data_migration migration, can_dispatch_to_leader can_dispatch) {
    if (!data_migrations_active()) {
        return ssx::now<result<id>>(errc::feature_disabled);
    }
    vlog(dm_log.debug, "creating migration: {}", migration);

    return process_or_dispatch<
      create_migration_request,
      create_migration_reply>(
      create_migration_request{.migration = std::move(migration)},
      can_dispatch,
      [timeout = _operation_timeout](
        create_migration_request req, data_migrations_client_protocol& client) {
          return client.create_migration(
            std::move(req), rpc::client_opts(timeout));
      },
      [this](create_migration_request req) {
          return container().invoke_on(
            data_migrations_shard,
            [req = std::move(req)](frontend& local) mutable {
                return local.do_create_migration(std::move(req.migration));
            });
      },
      [](result<create_migration_reply> reply) -> result<id> {
          if (reply.has_error()) {
              return reply.error();
          }
          if (reply.assume_value().ec != errc::success) {
              return make_error_code(reply.assume_value().ec);
          }
          return reply.assume_value().id;
      });
}

ss::future<std::error_code> frontend::update_migration_state(
  id id, state state, can_dispatch_to_leader can_dispatch) {
    if (!data_migrations_active()) {
        return ssx::now<std::error_code>(errc::feature_disabled);
    }
    vlog(dm_log.debug, "updating migration: {} state with: {}", id, state);
    return process_or_dispatch<
      update_migration_state_request,
      update_migration_state_reply>(
      update_migration_state_request{.id = id, .state = state},
      can_dispatch,
      [timeout = _operation_timeout](
        update_migration_state_request req,
        data_migrations_client_protocol& client) {
          return client.update_migration_state(
            std::move(req), rpc::client_opts(timeout));
      },
      [this](update_migration_state_request req) {
          return container().invoke_on(
            data_migrations_shard, [req](frontend& local) mutable {
                return local.do_update_migration_state(req.id, req.state);
            });
      },
      [](result<update_migration_state_reply> reply) -> std::error_code {
          if (reply.has_error()) {
              return reply.error();
          }
          if (reply.assume_value().ec != errc::success) {
              return make_error_code(reply.assume_value().ec);
          }
          return make_error_code(errc::success);
      });
}

ss::future<std::error_code>
frontend::remove_migration(id id, can_dispatch_to_leader can_dispatch) {
    if (!data_migrations_active()) {
        return ssx::now<std::error_code>(errc::feature_disabled);
    }
    vlog(dm_log.debug, "removing migration: {}", id);
    return process_or_dispatch<
      remove_migration_request,
      remove_migration_reply>(
      remove_migration_request{.id = id},
      can_dispatch,
      [timeout = _operation_timeout](
        remove_migration_request req, data_migrations_client_protocol& client) {
          return client.remove_migration(
            std::move(req), rpc::client_opts(timeout));
      },
      [this](remove_migration_request req) {
          return container().invoke_on(
            data_migrations_shard, [req](frontend& local) mutable {
                return local.do_remove_migration(req.id);
            });
      },
      [](result<remove_migration_reply> reply) -> std::error_code {
          if (reply.has_error()) {
              return reply.error();
          }
          if (reply.assume_value().ec != errc::success) {
              return make_error_code(reply.assume_value().ec);
          }
          return make_error_code(errc::success);
      });
}

ss::future<check_ntp_states_reply> frontend::check_ntp_states_on_foreign_node(
  model::node_id node, check_ntp_states_request&& req) {
    vlog(dm_log.debug, "dispatching node request {} to node {}", req, node);

    return _connections.local()
      .with_node_client<data_migrations_client_protocol>(
        _self,
        ss::this_shard_id(),
        node,
        _operation_timeout,
        [req = std::move(req),
         this](data_migrations_client_protocol client) mutable {
            return client
              .check_ntp_states(
                std::move(req), rpc::client_opts(_operation_timeout))
              .then(&rpc::get_ctx_data<check_ntp_states_reply>);
        })
      .then([](result<check_ntp_states_reply> res) {
          return res.has_value() ? std::move(res.assume_value())
                                 : check_ntp_states_reply{};
      });
}

ss::future<result<id>> frontend::do_create_migration(data_migration migration) {
    validate_migration_shard();
    auto ec = co_await insert_barrier();
    if (ec) {
        co_return ec;
    }
    /**
     * preliminary validation of migration after the barrier
     */
    if (migrations_table::is_empty_migration(migration)) {
        vlog(dm_log.warn, "data migration can not be empty.");
        co_return make_error_code(errc::data_migration_invalid_resources);
    }

    auto v_err = _table.local().validate_migrated_resources(migration);

    if (v_err) {
        vlog(
          dm_log.warn,
          "data migration {} validation error - {}",
          migration,
          v_err.value());
        co_return make_error_code(errc::data_migration_invalid_resources);
    }

    auto id = _table.local().get_next_id();
    ec = co_await replicate_and_wait(
      _controller,
      _as,
      create_data_migration_cmd(
        0,
        create_migration_cmd_data{.id = id, .migration = std::move(migration)}),
      _operation_timeout + model::timeout_clock::now());
    if (ec) {
        co_return ec;
    }
    co_return result<data_migrations::id>(id);
}

ss::future<chunked_vector<migration_metadata>> frontend::list_migrations() {
    return container().invoke_on(data_migrations_shard, [](frontend& local) {
        return local._table.local().list_migrations();
    });
}

ss::future<result<migration_metadata>>
frontend::get_migration(id migration_id) {
    return container().invoke_on(
      data_migrations_shard, [migration_id](frontend& local) {
          auto maybe_migration = local._table.local().get_migration(
            migration_id);
          return maybe_migration
                   ? result<migration_metadata>(maybe_migration->get().copy())
                   : errc::data_migration_not_exists;
      });
}

ss::future<std::error_code> frontend::insert_barrier() {
    const auto barrier_deadline = _operation_timeout
                                  + model::timeout_clock::now();
    /**
     * Inject linearizable barrier before creating a new migration. This is not
     * required for correctness but allows the fronted to do more accurate
     * preliminary validation.
     */
    auto barrier_result
      = co_await _controller.local().insert_linearizable_barrier(
        _operation_timeout + model::timeout_clock::now());
    if (!barrier_result) {
        co_return barrier_result.error();
    }
    try {
        co_await _controller.local().wait(
          barrier_result.value(), barrier_deadline);
    } catch (...) {
        co_return errc::timeout;
    }

    co_return errc::success;
}

ss::future<std::error_code>
frontend::do_update_migration_state(id id, state state) {
    validate_migration_shard();
    auto ec = co_await insert_barrier();
    if (ec) {
        vlog(dm_log.warn, "failed waiting for barrier: {}", ec);
        co_return ec;
    }
    /**
     * preliminary validation of migration state transition
     */
    auto migration = _table.local().get_migration(id);
    if (!migration) {
        vlog(dm_log.warn, "migration {} id not found", id);
        co_return errc::data_migration_not_exists;
    }
    auto cur_state = migration.value().get().state;
    if (!migrations_table::is_valid_state_transition(cur_state, state)) {
        vlog(
          dm_log.warn,
          "migration {} cannot be transitioned from state {} to {}",
          id,
          cur_state,
          state);
        co_return errc::invalid_data_migration_state;
    }
    ec = co_await replicate_and_wait(
      _controller,
      _as,
      update_data_migration_state_cmd(
        0, update_migration_state_cmd_data{.id = id, .requested_state = state}),
      _operation_timeout + model::timeout_clock::now());

    if (ec) {
        vlog(
          dm_log.warn,
          "failed to send update_data_migration_state_cmd: {}",
          ec);
        co_return ec;
    }
    vlog(
      dm_log.debug,
      "successfully sent migration {} transition request from state {} to {}",
      id,
      cur_state,
      state);

    co_return errc::success;
}
ss::future<std::error_code> frontend::do_remove_migration(id id) {
    validate_migration_shard();
    auto ec = co_await insert_barrier();
    if (ec) {
        vlog(dm_log.warn, "failed waiting for barrier: {}", ec);
        co_return ec;
    }
    /**
     * preliminary validation of migration existence
     */
    auto migration = _table.local().get_migration(id);
    if (!migration) {
        vlog(dm_log.warn, "migration {} id not found", id);
        co_return errc::data_migration_not_exists;
    }

    ec = co_await replicate_and_wait(
      _controller,
      _as,
      remove_data_migration_cmd(0, remove_migration_cmd_data{.id = id}),
      _operation_timeout + model::timeout_clock::now());

    if (ec) {
        vlog(dm_log.warn, "failed to send remove_data_migration_cmd: {}", ec);
        co_return ec;
    }
    vlog(dm_log.debug, "successfully sent migration {} delete request", id);

    co_return errc::success;
}

} // namespace cluster::data_migrations
