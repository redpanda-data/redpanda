/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cloud_io/remote.h"
#include "cloud_storage_clients/types.h"
#include "cloud_topics/level_one/metastore/lsm/stm.h"
#include "lsm/io/persistence.h"
#include "model/fundamental.h"

namespace cloud_topics::l1 {

// Open a metadata persistence object in the given bucket at the prefix. In
// addition to writing to cloud, this metadata persistence replicates the
// manifest via the replicated state machine.
//
// The caller of this is expected to be the current leader of the Raft term,
// and will return an error if the manifest to be returned from the state
// machine corresponds to a higher term (implying that the caller is no longer
// leader).
//
// The input domain UUID and object storage prefix are expected to correspond
// to one another. If the state machine points to a different domain (e.g.
// after recovery), this metadata persistence will subsequently return errors,
// with the expectation that it will be closed and reopened.
ss::future<std::unique_ptr<lsm::io::metadata_persistence>>
open_replicated_metadata_persistence(
  stm* stm,
  cloud_io::remote* remote,
  cloud_storage_clients::bucket_name bucket,
  domain_uuid domain_uuid,
  const cloud_storage_clients::object_key& prefix);

} // namespace cloud_topics::l1
