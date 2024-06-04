// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once
#include "kafka/server/group.h"
#include "kafka/server/group_metadata.h"
#include "model/fundamental.h"
#include "model/record.h"

#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/log.hh>

#include <absl/container/node_hash_map.h>
#include <absl/container/node_hash_set.h>

namespace kafka {

class group_stm {
public:
    struct logged_metadata {
        model::offset log_offset;
        offset_metadata_value metadata;
    };

    struct tx_info {
        model::tx_seq tx_seq;
        model::partition_id tm_partition;
    };

    void overwrite_metadata(group_metadata_value&&);

    void update_offset(
      const model::topic_partition&, model::offset, offset_metadata_value&&);
    void remove_offset(const model::topic_partition&);
    void update_prepared(model::offset, group_tx::offsets_metadata);
    void commit(model::producer_identity);
    void abort(model::producer_identity, model::tx_seq);
    void try_set_fence(model::producer_id id, model::producer_epoch epoch) {
        auto [fence_it, _] = _fence_pid_epoch.try_emplace(id, epoch);
        if (fence_it->second < epoch) {
            fence_it->second = epoch;
        }
    }
    void try_set_fence(
      model::producer_id id,
      model::producer_epoch epoch,
      model::tx_seq txseq,
      model::timeout_clock::duration transaction_timeout_ms,
      model::partition_id tm_partition) {
        auto [fence_it, _] = _fence_pid_epoch.try_emplace(id, epoch);
        if (fence_it->second <= epoch) {
            fence_it->second = epoch;
            model::producer_identity pid(id(), epoch());
            _tx_data[pid] = tx_info{txseq, tm_partition};
            _timeouts[pid] = transaction_timeout_ms;
        }
    }
    bool has_data() const {
        return !_is_removed && (_is_loaded || _offsets.size() > 0);
    }
    bool is_removed() const { return _is_removed; }

    const absl::node_hash_map<model::producer_id, group::ongoing_tx_offsets>&
    ongoing_tx_offsets() const {
        return _ongoing_tx_offsets;
    }

    const absl::node_hash_map<model::topic_partition, logged_metadata>&
    offsets() const {
        return _offsets;
    }

    const absl::node_hash_map<model::producer_id, model::producer_epoch>&
    fences() const {
        return _fence_pid_epoch;
    }

    const absl::node_hash_map<model::producer_identity, tx_info>&
    tx_data() const {
        return _tx_data;
    }

    const absl::
      node_hash_map<model::producer_identity, model::timeout_clock::duration>&
      timeouts() const {
        return _timeouts;
    }

    group_metadata_value& get_metadata() { return _metadata; }

    const group_metadata_value& get_metadata() const { return _metadata; }

private:
    absl::node_hash_map<model::topic_partition, logged_metadata> _offsets;
    absl::node_hash_map<model::producer_id, group::ongoing_tx_offsets>
      _ongoing_tx_offsets;
    absl::node_hash_map<model::producer_id, model::producer_epoch>
      _fence_pid_epoch;
    absl::node_hash_map<model::producer_identity, tx_info> _tx_data;
    absl::
      node_hash_map<model::producer_identity, model::timeout_clock::duration>
        _timeouts;
    group_metadata_value _metadata;
    bool _is_loaded{false};
    bool _is_removed{false};
};

} // namespace kafka
