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

#include "archival/manifest.h"
#include "model/fundamental.h"
#include "storage/log_manager.h"

namespace archival {

/// Policy that controls how archiver picks upload candidates
enum class upload_policy_selector {
    /// Archive only original, non-compacted segments
    archive_non_compacted,
};

/// Policy that controls how archiver picks delete candidates
enum class delete_policy_selector {
    /// Don't keep segments in S3 if they're deleted localy
    do_not_keep,
};

class upload_policy_base {
public:
    /// \brief Generate list of segments that should be uploaded to S3
    ///
    /// \param remote is a remote manifest (cached or downloade)
    /// \param lm is log manager
    virtual std::optional<manifest>
    generate_upload_set(const manifest& remote, storage::log_manager& lm) = 0;
};

class delete_policy_base {
public:
    /// \brief Generate list of segments that should be remove from S3
    ///
    /// \param remote is a remote manifest (cached or downloade)
    /// \param lm is log manager
    virtual std::optional<manifest>
    generate_delete_set(const manifest& remote, storage::log_manager& lm) = 0;
};

/// Archiving policy implementation interface
class archival_policy_base
  : public upload_policy_base
  , public delete_policy_base {
public:
    archival_policy_base() = default;
    virtual ~archival_policy_base() = default;
    archival_policy_base(const archival_policy_base&) = delete;
    archival_policy_base(archival_policy_base&&) = delete;
    archival_policy_base& operator=(const archival_policy_base&) = delete;
    archival_policy_base& operator=(archival_policy_base&&) = delete;
};

std::unique_ptr<archival_policy_base> make_archival_policy(
  upload_policy_selector e,
  delete_policy_selector d,
  model::ntp ntp,
  model::revision_id rev);
} // namespace archival