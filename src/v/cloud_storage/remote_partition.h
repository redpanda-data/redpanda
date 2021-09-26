/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_storage/offset_translation_layer.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/remote_segment.h"
#include "cloud_storage/types.h"
#include "model/metadata.h"
#include "s3/client.h"
#include "storage/ntp_config.h"
#include "storage/types.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/weak_ptr.hh>

namespace cloud_storage {

class remote_partition : public ss::weakly_referencable<remote_partition> {
public:
    /// C-tor
    ///
    /// The manifest's lifetime should be bound to the lifetime of the owner
    /// of the remote_partition.
    remote_partition(
      const manifest& m, remote& api, cache& c, s3::bucket_name bucket);

    ss::future<model::record_batch_reader> make_reader(
      storage::log_reader_config config,
      std::optional<model::timeout_clock::time_point> deadline = std::nullopt);

    model::offset first_uploaded_offset() const {
        // TODO: cache in the field
        model::offset starting_offset = model::offset::max();
        for (const auto& m : _manifest) {
            starting_offset = std::min(starting_offset, m.second.base_offset);
        }
        return starting_offset;
    }

    model::offset last_uploaded_offset() const {
        return _manifest.get_last_offset();
    }

private:
    remote& _api;
    cache& _cache;
    const manifest& _manifest;
    ss::lw_shared_ptr<offset_translator> _translator;
    s3::bucket_name _bucket;
    retry_chain_node _rtc;
    retry_chain_logger _ctxlog;
};

} // namespace cloud_storage
