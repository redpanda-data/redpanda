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

#include <absl/container/node_hash_set.h>

#include <memory>

namespace kafka {

class group_stm {
public:
    struct logged_metadata {
        model::offset log_offset;
        offset_metadata_value metadata;
    };

    struct ongoing_tx {
        model::tx_seq tx_seq;
        model::partition_id tm_partition;
        model::timeout_clock::duration timeout;
        chunked_hash_map<model::topic_partition, group::offset_metadata>
          offsets;
    };

    struct producer {
        model::producer_epoch epoch;
        std::unique_ptr<ongoing_tx> tx;
    };
    void overwrite_metadata(group_metadata_value&&);

    void update_offset(
      const model::topic_partition&, model::offset, offset_metadata_value&&);
    void remove_offset(const model::topic_partition&);
    void update_tx_offset(model::offset, group_tx::offsets_metadata);
    void commit(model::producer_identity);
    void abort(model::producer_identity, model::tx_seq);
    void try_set_fence(model::producer_id id, model::producer_epoch epoch);
    void try_set_fence(
      model::producer_id id,
      model::producer_epoch epoch,
      model::tx_seq txseq,
      model::timeout_clock::duration transaction_timeout_ms,
      model::partition_id tm_partition);

    bool has_data() const {
        return !_is_removed && (_is_loaded || _offsets.size() > 0);
    }
    bool is_removed() const { return _is_removed; }
    const chunked_hash_map<model::producer_id, producer>& producers() const {
        return _producers;
    }

    const chunked_hash_map<model::topic_partition, logged_metadata>&
    offsets() const {
        return _offsets;
    }

    group_metadata_value& get_metadata() { return _metadata; }

    const group_metadata_value& get_metadata() const { return _metadata; }

private:
    chunked_hash_map<model::topic_partition, logged_metadata> _offsets;
    chunked_hash_map<model::producer_id, producer> _producers;

    group_metadata_value _metadata;
    bool _is_loaded{false};
    bool _is_removed{false};
};

} // namespace kafka
