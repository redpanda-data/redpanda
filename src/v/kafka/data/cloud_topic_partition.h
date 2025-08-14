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

#include "kafka/data/partition_proxy.h"
#include "kafka/protocol/errors.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "raft/replicate.h"

#include <seastar/core/coroutine.hh>

#include <boost/numeric/conversion/cast.hpp>

#include <optional>
#include <system_error>

namespace experimental::cloud_topics {
class frontend;
} // namespace experimental::cloud_topics

namespace cluster {
class partition;
}

namespace kafka {

/// This class implements the partition_proxy interface for cloud topics.
/// It's a wrapper around the cloud_topics::frontend.
class cloud_topic_partition final : public kafka::partition_proxy::impl {
public:
    explicit cloud_topic_partition(
      ss::lw_shared_ptr<cluster::partition> p,
      std::unique_ptr<experimental::cloud_topics::frontend> fe) noexcept;

    const model::ntp& ntp() const final;

    ss::future<result<model::offset, error_code>>
    sync_effective_start(model::timeout_clock::duration timeout) final;

    model::offset local_start_offset() const final;

    model::offset start_offset() const final;

    model::offset high_watermark() const final;

    checked<model::offset, error_code> last_stable_offset() const final;

    bool is_leader() const final;

    ss::future<error_code>
      prefix_truncate(model::offset, ss::lowres_clock::time_point) final;

    ss::future<std::error_code> linearizable_barrier() final;

    ss::future<std::optional<storage::timequery_result>>
    timequery(storage::timequery_config cfg) final;

    ss::future<result<model::offset>>
      replicate(model::record_batch, raft::replicate_options) final;

    ss::future<result<model::offset>> replicate(
      chunked_vector<model::record_batch>, raft::replicate_options) final;
    raft::replicate_stages replicate(
      model::batch_identity,
      model::record_batch,
      raft::replicate_options) final;

    ss::future<storage::translating_reader> make_reader(
      storage::log_reader_config cfg,
      std::optional<model::timeout_clock::time_point>) final;

    ss::future<std::vector<model::tx_range>> aborted_transactions(
      model::offset base,
      model::offset last,
      ss::lw_shared_ptr<const storage::offset_translator_state>) final;

    cluster::partition_probe& probe() final;

    ss::future<std::optional<model::offset>>
      get_leader_epoch_last_offset(kafka::leader_epoch) const final;

    kafka::leader_epoch leader_epoch() const final;

    ss::future<error_code> validate_fetch_offset(
      model::offset, bool, model::timeout_clock::time_point) final;

    result<partition_info> get_partition_info() const final;

    size_t estimate_size_between(kafka::offset, kafka::offset) const final;

private:
    ss::lw_shared_ptr<cluster::partition> _partition;
    std::unique_ptr<experimental::cloud_topics::frontend> _fe;
};

} // namespace kafka
