// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/offset_writer.h"

#include "cluster/partition.h"

namespace kafka {

namespace {

raft::replicate_options at_quorum(model::term_id term) {
    return raft::replicate_options(raft::consistency_level::quorum_ack, term);
}

} // namespace

partition_offset_writer::partition_offset_writer(
  ss::lw_shared_ptr<cluster::partition> partition)
  : _partition(std::move(partition)) {}

model::term_id partition_offset_writer::term() const {
    return _partition->term();
}

ss::future<result<raft::replicate_result>> partition_offset_writer::replicate(
  model::record_batch batch, model::term_id term) {
    return _partition->raft()->replicate(std::move(batch), at_quorum(term));
}

ss::future<result<raft::replicate_result>> partition_offset_writer::replicate(
  chunked_vector<model::record_batch> batches, model::term_id term) {
    return _partition->raft()->replicate(std::move(batches), at_quorum(term));
}

raft::replicate_stages partition_offset_writer::replicate_in_stages(
  chunked_vector<model::record_batch> batches, model::term_id term) {
    return _partition->raft()->replicate_in_stages(
      std::move(batches), at_quorum(term));
}

ss::future<> partition_offset_writer::maybe_step_down(
  model::term_id term, std::string_view reason) {
    if (_partition->raft()->is_leader() && _partition->raft()->term() == term) {
        return _partition->raft()->step_down(reason);
    }
    return ss::now();
}

} // namespace kafka
