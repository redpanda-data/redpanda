// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "protocol.h"

#include "cluster/topics_frontend.h"
#include "config/configuration.h"
#include "kafka/server/connection_context.h"
#include "kafka/server/coordinator_ntp_mapper.h"
#include "kafka/server/group_router.h"
#include "kafka/server/logger.h"
#include "kafka/server/request_context.h"
#include "kafka/server/response.h"
#include "security/scram_algorithm.h"
#include "utils/utf8.h"
#include "vlog.h"

#include <seastar/core/byteorder.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/util/log.hh>

#include <fmt/format.h>

#include <chrono>
#include <exception>
#include <limits>

namespace kafka {

protocol::protocol(
  ss::smp_service_group smp,
  ss::sharded<cluster::metadata_cache>& meta,
  ss::sharded<cluster::topics_frontend>& tf,
  ss::sharded<cluster::config_frontend>& cf,
  ss::sharded<cluster::feature_table>& ft,
  ss::sharded<quota_manager>& quota,
  ss::sharded<kafka::group_router>& router,
  ss::sharded<cluster::shard_table>& tbl,
  ss::sharded<cluster::partition_manager>& pm,
  ss::sharded<fetch_session_cache>& session_cache,
  ss::sharded<cluster::id_allocator_frontend>& id_allocator_frontend,
  ss::sharded<security::credential_store>& credentials,
  ss::sharded<security::authorizer>& authorizer,
  ss::sharded<cluster::security_frontend>& sec_fe,
  ss::sharded<cluster::controller_api>& controller_api,
  ss::sharded<cluster::tx_gateway_frontend>& tx_gateway_frontend,
  ss::sharded<coproc::partition_manager>& coproc_partition_manager,
  ss::sharded<v8_engine::data_policy_table>& data_policy_table,
  std::optional<qdc_monitor::config> qdc_config) noexcept
  : _smp_group(smp)
  , _topics_frontend(tf)
  , _config_frontend(cf)
  , _feature_table(ft)
  , _metadata_cache(meta)
  , _quota_mgr(quota)
  , _group_router(router)
  , _shard_table(tbl)
  , _partition_manager(pm)
  , _fetch_session_cache(session_cache)
  , _id_allocator_frontend(id_allocator_frontend)
  , _is_idempotence_enabled(
      config::shard_local_cfg().enable_idempotence.value())
  , _are_transactions_enabled(
      config::shard_local_cfg().enable_transactions.value())
  , _credentials(credentials)
  , _authorizer(authorizer)
  , _security_frontend(sec_fe)
  , _controller_api(controller_api)
  , _tx_gateway_frontend(tx_gateway_frontend)
  , _coproc_partition_manager(coproc_partition_manager)
  , _data_policy_table(data_policy_table) {
    if (qdc_config) {
        _qdc_mon.emplace(*qdc_config);
    }
    _probe.setup_metrics();
}

coordinator_ntp_mapper& protocol::coordinator_mapper() {
    return _group_router.local().coordinator_mapper().local();
}

ss::future<> protocol::apply(net::server::resources rs) {
    /*
     * if sasl authentication is not enabled then initialize the sasl state to
     * complete. this will cause auth to be skipped during request processing.
     *
     * TODO: temporarily acl authorization is enabled/disabled based on sasl
     * being enabled/disabled. it may be useful to configure them separately,
     * but this will come when identity management is introduced.
     */
    security::sasl_server sasl(
      config::shard_local_cfg().enable_sasl()
        ? security::sasl_server::sasl_state::initial
        : security::sasl_server::sasl_state::complete);

    auto ctx = ss::make_lw_shared<connection_context>(
      *this,
      std::move(rs),
      std::move(sasl),
      config::shard_local_cfg().enable_sasl());

    return ss::do_until(
             [ctx] { return ctx->is_finished_parsing(); },
             [ctx] { return ctx->process_one_request(); })
      .handle_exception([ctx](std::exception_ptr eptr) {
          if (config::shard_local_cfg().enable_sasl()) {
              vlog(
                klog.info,
                "{}:{} errored, proto: {}, sasl state: {} - {}",
                ctx->client_host(),
                ctx->client_port(),
                ctx->server().name(),
                security::sasl_state_to_str(ctx->sasl().state()),
                eptr);
          } else {
              vlog(
                klog.info,
                "{}:{} errored, proto: {} - {}",
                ctx->client_host(),
                ctx->client_port(),
                ctx->server().name(),
                eptr);
          }
          return ss::make_exception_future(eptr);
      })
      .finally([ctx] {});
}

} // namespace kafka
