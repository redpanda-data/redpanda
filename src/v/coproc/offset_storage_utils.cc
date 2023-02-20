/*
 * Copyright 2020 Redpanda Data, Inc.
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
#include "bytes/iostream.h"
#include "coproc/logger.h"
#include "reflection/absl/flat_hash_map.h"
#include "reflection/absl/node_hash_map.h"
#include "storage/snapshot.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>

#include <optional>

namespace coproc {

ss::future<all_routes> recover_offsets(storage::simple_snapshot_manager& snap) {
    all_routes recovered;
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
    co_return co_await reflection::async_adl<all_routes>{}.from(iobuf_p);
}

ss::future<>
save_offsets(storage::simple_snapshot_manager& snap, all_routes routes) {
    /// Create the metadata, and data iobuffers
    iobuf metadata = reflection::to_iobuf(static_cast<int8_t>(1));
    iobuf data;
    co_await reflection::async_adl<all_routes>{}.to(data, std::move(routes));

    /// Serialize this to disk via the simple_snapshot_manager
    storage::snapshot_writer writer = co_await snap.start_snapshot();
    co_await writer.write_metadata(std::move(metadata));
    co_await write_iobuf_to_output_stream(std::move(data), writer.output());
    co_await writer.close();
    co_await snap.finish_snapshot(writer);
}

} // namespace coproc
