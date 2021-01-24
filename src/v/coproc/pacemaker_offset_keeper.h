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

#pragma once
#include "coproc/ntp_context.h"
#include "coproc/offset_utils.h"

#include <chrono>

using namespace std::chrono_literals;

namespace coproc {

class pacemaker_offset_keeper {
public:
    explicit pacemaker_offset_keeper(ntp_context_cache& ntp_cache)
      : _ntp_cache(ntp_cache)
      , _timer([this] { (void)persist_to_disk(); }) {}

    ss::future<> start() {
        _timer.arm(5min);
        return ss::now();
    }

    ss::future<> stop() {
        _timer.cancel();
        return _gate.close();
    }

    /// When a script restarts, it should call this method to retrieve saved
    /// offsets from disk, if nothing was found, an empty map is returned
    ntp_context::offsets_map get(model::ntp k) {
        const bytes key = ntp_to_bytes(std::move(k));
        auto ovalue = _kvstore.get(
          storage::kvstore::key_space::coproc, bytes_view(key));
        return !ovalue ? ntp_context::offsets_map()
                       : offsets_map_from_iobuf(std::move(*ovalue));
    }

private:
    ss::future<> persist_to_disk() {
        return ss::with_gate(_gate, [this] { return do_persist_to_disk(); })
          .then([this] {
              if (!_timer.armed()) {
                  _timer.arm(5min);
              }
          });
    }

    ss::future<> do_persist_to_disk() {
        return ss::do_for_each(
          _ntp_cache, [this](const ntp_context_cache::value_type& p) {
              return _kvstore.put(
                storage::kvstore::key_space::coproc,
                ntp_to_bytes(p.first),
                offsets_map_to_iobuf(p.second->offsets));
          });
    }

    static storage::kvstore_config kvstore_config() {
        std::filesystem::path kv_copro_root
          = config::shard_local_cfg().data_directory().path / "copro_metadata";
        return storage::kvstore_config(
          config::shard_local_cfg().kvstore_max_segment_size(),
          1s,
          kv_copro_root.string(),
          storage::debug_sanitize_files::no);
    }

    /// Reference to all ntps shared amongst all script_contexts
    ntp_context_cache& _ntp_cache;

    /// Gate & timer for triggering and proper shutdown of loop
    ss::gate _gate;
    ss::timer<ss::lowres_clock> _timer;

    /// Experimental feature to store offsets in our own kvstore. Making another
    /// instance here to not negatively affect the main kvstore in storage::api.
    /// This should evolve into a slightly different type of class that has
    /// different properties then the kvstore, catered to coproc's usage
    storage::kvstore _kvstore{kvstore_config()};
};

} // namespace coproc
