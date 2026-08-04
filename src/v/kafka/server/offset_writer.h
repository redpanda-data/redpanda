/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "base/seastarx.h"
#include "cluster/fwd.h"
#include "container/chunked_vector.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "raft/replicate.h"

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>

#include <string_view>

namespace kafka {

/// The writes a consumer group makes to its `__consumer_offsets` partition.
class offset_writer {
public:
    offset_writer() = default;
    offset_writer(const offset_writer&) = delete;
    offset_writer& operator=(const offset_writer&) = delete;
    offset_writer(offset_writer&&) = delete;
    offset_writer& operator=(offset_writer&&) = delete;
    virtual ~offset_writer() = default;

    /// The partition's current term. A caller whose own term differs no longer
    /// owns the group's state and must not write.
    virtual model::term_id term() const = 0;

    virtual ss::future<result<raft::replicate_result>>
    replicate(model::record_batch batch, model::term_id term) = 0;

    virtual ss::future<result<raft::replicate_result>> replicate(
      chunked_vector<model::record_batch> batches, model::term_id term) = 0;

    virtual raft::replicate_stages replicate_in_stages(
      chunked_vector<model::record_batch> batches, model::term_id term) = 0;

    /// \brief Give up leadership after a failed write, if still the leader in
    /// \p term.
    ///
    /// A write that fails leaves the group's state uncertain on this node, so
    /// the caller sheds leadership and lets recovery re-read the log.
    virtual ss::future<>
    maybe_step_down(model::term_id term, std::string_view reason) = 0;
};

/// \brief The writes as they land on a real partition.
///
/// Every write is at quorum acknowledgement in the term the caller passes,
/// which is the term the caller adopted when it took over the partition.
/// Requests carrying an older term are rejected by raft rather than silently
/// applied.
class partition_offset_writer final : public offset_writer {
public:
    explicit partition_offset_writer(
      ss::lw_shared_ptr<cluster::partition> partition);

    model::term_id term() const final;

    ss::future<result<raft::replicate_result>>
    replicate(model::record_batch batch, model::term_id term) final;

    ss::future<result<raft::replicate_result>> replicate(
      chunked_vector<model::record_batch> batches, model::term_id term) final;

    raft::replicate_stages replicate_in_stages(
      chunked_vector<model::record_batch> batches, model::term_id term) final;

    ss::future<>
    maybe_step_down(model::term_id term, std::string_view reason) final;

private:
    ss::lw_shared_ptr<cluster::partition> _partition;
};

} // namespace kafka
