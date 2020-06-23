#pragma once

#include "cluster/controller_stm.h"
#include "cluster/logger.h"
#include "cluster/namespace.h"
#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "cluster/types.h"
#include "model/metadata.h"

#include <vector>
namespace cluster {

static ss::future<consensus_ptr> create_raft0(
  ss::sharded<partition_manager>& pm,
  ss::sharded<shard_table>& st,
  const ss::sstring& data_directory,
  std::vector<model::broker> initial_brokers) {
    // root, otherwise it will use one of the seed servers to join the
    // cluster
    if (!initial_brokers.empty()) {
        vlog(clusterlog.info, "Current node is cluster root");
    }

    return pm.local()
      .manage(
        storage::ntp_config(controller_ntp, data_directory),
        raft::group_id(0),
        std::move(initial_brokers))
      .then([&st](consensus_ptr p) {
          // Add raft 0 to shard table
          return st
            .invoke_on_all([gr = p->group()](shard_table& local_st) {
                local_st.insert(controller_ntp, controller_stm_shard);
                local_st.insert(gr, controller_stm_shard);
            })
            .then([p] { return p; });
      });
}
} // namespace cluster