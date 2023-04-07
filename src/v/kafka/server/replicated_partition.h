/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "cluster/partition.h"
#include "cluster/partition_probe.h"
#include "kafka/protocol/errors.h"
#include "kafka/server/partition_proxy.h"
#include "kafka/types.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "raft/errc.h"
#include "raft/types.h"

#include <seastar/core/coroutine.hh>

#include <boost/numeric/conversion/cast.hpp>

#include <memory>
#include <system_error>

namespace kafka {

class replicated_partition final : public kafka::partition_proxy::impl {
public:
    explicit replicated_partition(
      ss::lw_shared_ptr<cluster::partition> p) noexcept;

    const model::ntp& ntp() const final { return _partition->ntp(); }

    model::offset start_offset() const final {
        if (
          _partition->is_read_replica_mode_enabled()
          && _partition->cloud_data_available()) {
            // Always assume remote read in this case.
            return _partition->start_cloud_offset();
        }

        auto local_kafka_start_offset = _translator->from_log_offset(
          _partition->start_offset());
        if (
          _partition->is_remote_fetch_enabled()
          && _partition->cloud_data_available()
          && (_partition->start_cloud_offset() < local_kafka_start_offset)) {
            return _partition->start_cloud_offset();
        }
        return local_kafka_start_offset;
    }

    model::offset high_watermark() const final {
        if (_partition->is_read_replica_mode_enabled()) {
            if (_partition->cloud_data_available()) {
                return _partition->next_cloud_offset();
            } else {
                return model::offset(0);
            }
        }
        return _translator->from_log_offset(_partition->high_watermark());
    }

    checked<model::offset, error_code> last_stable_offset() const final {
        if (_partition->is_read_replica_mode_enabled()) {
            if (_partition->cloud_data_available()) {
                // There is no difference between HWM and LO in this mode
                return _partition->next_cloud_offset();
            } else {
                return model::offset(0);
            }
        }
        auto maybe_lso = _partition->last_stable_offset();
        if (maybe_lso == model::invalid_lso) {
            return error_code::offset_not_available;
        }
        return _translator->from_log_offset(maybe_lso);
    }

    bool is_elected_leader() const final {
        return _partition->is_elected_leader();
    }

    bool is_leader() const final { return _partition->is_leader(); }

    ss::future<std::error_code> linearizable_barrier() final {
        auto r = co_await _partition->linearizable_barrier();
        if (r) {
            co_return raft::errc::success;
        }
        co_return r.error();
    }

    ss::future<std::optional<storage::timequery_result>>
    timequery(storage::timequery_config cfg) final;

    ss::future<result<model::offset>>
      replicate(model::record_batch_reader, raft::replicate_options);

    raft::replicate_stages replicate(
      model::batch_identity,
      model::record_batch_reader&&,
      raft::replicate_options);

    ss::future<storage::translating_reader> make_reader(
      storage::log_reader_config cfg,
      std::optional<model::timeout_clock::time_point>) final;

    ss::future<std::vector<cluster::rm_stm::tx_range>> aborted_transactions(
      model::offset base,
      model::offset last,
      ss::lw_shared_ptr<const storage::offset_translator_state>) final;

    cluster::partition_probe& probe() final { return _partition->probe(); }

    std::optional<model::offset>
      get_leader_epoch_last_offset(kafka::leader_epoch) const final;

    kafka::leader_epoch leader_epoch() const final {
        return leader_epoch_from_term(_partition->term());
    }

    ss::future<error_code> validate_fetch_offset(
      model::offset, model::timeout_clock::time_point) final;

private:
    ss::future<std::vector<cluster::rm_stm::tx_range>>
      aborted_transactions_local(
        cloud_storage::offset_range,
        ss::lw_shared_ptr<const storage::offset_translator_state>);

    ss::future<std::vector<cluster::rm_stm::tx_range>>
    aborted_transactions_remote(
      cloud_storage::offset_range offsets,
      ss::lw_shared_ptr<const storage::offset_translator_state> ot_state);

    bool may_read_from_cloud(kafka::offset);

    ss::lw_shared_ptr<cluster::partition> _partition;
    ss::lw_shared_ptr<const storage::offset_translator_state> _translator;
};

} // namespace kafka
