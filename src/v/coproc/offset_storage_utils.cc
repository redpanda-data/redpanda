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
#include "reflection/adl.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>
#include <seastar/util/defer.hh>

#include <bits/stdint-intn.h>

#include <optional>

namespace reflection {

template<>
struct adl<coproc::ntp_context::offset_tracker> {
    void to(iobuf& out, coproc::ntp_context::offset_tracker&& offsets) {
        reflection::serialize(out, static_cast<int32_t>(offsets.size()));
        for (const auto& e : offsets) {
            reflection::serialize(
              out, e.first, e.second.last_read, e.second.last_acked);
        }
    }

    coproc::ntp_context::offset_tracker from(iobuf_parser& in) {
        coproc::ntp_context::offset_tracker offsets;
        auto size = reflection::adl<int32_t>{}.from(in);
        for (auto i = 0; i < size; ++i) {
            auto id = reflection::adl<coproc::script_id>{}.from(in);
            auto last_read = reflection::adl<model::offset>{}.from(in);
            auto last_acked = reflection::adl<model::offset>{}.from(in);
            offsets.emplace(
              id,
              coproc::ntp_context::offset_pair{
                .last_read = last_read, .last_acked = last_acked});
        }
        return offsets;
    }
};

} // namespace reflection

namespace coproc {

using iresults_map
  = absl::flat_hash_map<model::ntp, ntp_context::offset_tracker>;

ss::future<iresults_map> deserialize_data_field(iobuf data) {
    return ss::do_with(
      iresults_map(),
      iobuf_parser(std::move(data)),
      [](iresults_map& irm, iobuf_parser& p) {
          const int32_t n_elements = reflection::adl<int32_t>{}.from(p);
          auto range = boost::irange<int32_t>(0, n_elements);
          return ss::do_for_each(
                   range,
                   [&irm, &p](int32_t) {
                       model::ntp key = reflection::adl<model::ntp>{}.from(p);
                       ntp_context::offset_tracker offsets
                         = reflection::adl<ntp_context::offset_tracker>{}.from(
                           p);
                       irm.emplace(std::move(key), std::move(offsets));
                   })
            .then([&irm] { return std::move(irm); });
      });
}

ss::future<iobuf> serialize_data_field(const ntp_context_cache& ntp_cache) {
    return ss::do_with(iobuf(), [&ntp_cache](iobuf& data) {
        reflection::serialize(data, static_cast<int32_t>(ntp_cache.size()));
        return ss::do_for_each(
                 ntp_cache,
                 [&data](const ntp_context_cache::value_type& p) {
                     reflection::serialize(
                       data,
                       model::ntp(p.first),
                       ntp_context::offset_tracker(p.second->offsets));
                 })
          .then([&data] { return std::move(data); });
    });
}

ss::future<ntp_context_cache> recover_offsets(
  storage::snapshot_manager& snap, storage::log_manager& log_mgr) {
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
    co_await reader.close();
    /// Deserialize the data
    iresults_map irm = co_await deserialize_data_field(std::move(data));
    vlog(coproclog.info, "Recovered {} coprocessor offsets....", irm.size());
    /// Match the offsets map with the corresponding storage::log queried from
    /// the log_manager, building the completed ntp_context_cache
    for (auto& [key, offsets] : irm) {
        std::optional<storage::log> log = log_mgr.get(key);
        if (!log) {
            vlog(
              coproclog.error,
              "Coult not recover ntp {}, for some reason it does not exist in "
              "the log_manager",
              key);
        } else {
            auto [itr, _] = recovered.emplace(
              std::move(key), ss::make_lw_shared<ntp_context>(std::move(*log)));
            itr->second->offsets = std::move(offsets);
        }
    }
    co_return recovered;
}

ss::future<> save_offsets(
  storage::snapshot_manager& snap, const ntp_context_cache& ntp_cache) {
    vlog(
      coproclog.info,
      "Saving {} coprocessor offsets to disk....",
      ntp_cache.size());
    /// Create the metadata, and data iobuffers
    iobuf metadata = reflection::to_iobuf(static_cast<int8_t>(1));
    iobuf data = co_await serialize_data_field(ntp_cache);

    /// Serialize this to disk via the snapshot_manager
    storage::snapshot_writer writer = co_await snap.start_snapshot();
    co_await writer.write_metadata(std::move(metadata));
    co_await write_iobuf_to_output_stream(std::move(data), writer.output());
    co_await writer.close();
    co_await snap.finish_snapshot(writer);
}

} // namespace coproc
