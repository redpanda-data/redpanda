/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "coproc/offset_storage_utils.h"

#include "bytes/iobuf.h"
#include "coproc/logger.h"
#include "coproc/ntp_context.h"
#include "model/fundamental.h"
#include "reflection/absl/btree_map.h"
#include "reflection/absl/flat_hash_map.h"
#include "storage/log_manager.h"
#include "storage/snapshot.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>
#include <seastar/util/defer.hh>

#include <bits/stdint-intn.h>

#include <optional>

namespace coproc {

using iresults_map
  = absl::flat_hash_map<model::ntp, ntp_context::offset_tracker>;

ss::future<ntp_context_cache> recover_offsets(
  storage::simple_snapshot_manager& snap, cluster::partition_manager& pm) {
    ntp_context_cache recovered;
    auto optional_snap_reader = co_await snap.open_snapshot();
    if (!optional_snap_reader) {
        co_return recovered;
    }
    storage::snapshot_reader& reader = *optional_snap_reader;
    /// Read the metadata which contains the total number of elements
    iobuf metadata = co_await reader.read_metadata();
    const int8_t version = iobuf_const_parser(metadata).consume_type<int8_t>();
    if (version != 1) {
        vlog(coproclog.error, "Incorrect version in recover_offsets snapshot");
        co_await reader.close();
        co_return recovered;
    }
    /// Query the total size, and read the rest of the data
    const size_t size = co_await reader.get_snapshot_size();
    iobuf data = co_await read_iobuf_exactly(reader.input(), size);
    iobuf_parser iobuf_p(std::move(data));
    co_await reader.close();
    /// Deserialize the data
    iresults_map irm = co_await reflection::async_adl<iresults_map>{}.from(
      iobuf_p);
    vlog(coproclog.info, "Recovered {} coprocessor offsets....", irm.size());
    /// Match the offsets map with the corresponding storage::log queried from
    /// the log_manager, building the completed ntp_context_cache
    for (auto& [key, offsets] : irm) {
        auto partition = pm.get(key);
        if (!partition) {
            vlog(
              coproclog.error,
              "Coult not recover ntp {}, for some reason it does not exist in "
              "the partition_manager",
              key);
        } else {
            auto [itr, _] = recovered.emplace(
              key, ss::make_lw_shared<ntp_context>(std::move(partition)));
            itr->second->offsets = std::move(offsets);
        }
    }
    co_return recovered;
}

ss::future<> save_offsets(
  storage::simple_snapshot_manager& snap, const ntp_context_cache& ntp_cache) {
    /// Create the metadata, and data iobuffers
    iobuf metadata = reflection::to_iobuf(static_cast<int8_t>(1));
    iresults_map irm_map;
    irm_map.reserve(ntp_cache.size());
    std::transform(
      ntp_cache.cbegin(),
      ntp_cache.end(),
      std::inserter(irm_map, irm_map.end()),
      [](const ntp_context_cache::value_type& p) {
          return std::make_pair<>(p.first, p.second->offsets);
      });
    iobuf data;
    co_await reflection::async_adl<iresults_map>{}.to(data, std::move(irm_map));

    /// Serialize this to disk via the simple_snapshot_manager
    storage::snapshot_writer writer = co_await snap.start_snapshot();
    co_await writer.write_metadata(std::move(metadata));
    co_await write_iobuf_to_output_stream(std::move(data), writer.output());
    co_await writer.close();
    co_await snap.finish_snapshot(writer);
}

} // namespace coproc
