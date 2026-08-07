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
#include "absl/container/flat_hash_map.h"
#include "base/format_to.h"
#include "base/seastarx.h"
#include "cluster/fwd.h"
#include "cluster/simple_batch_builder.h"
#include "cluster/tx_protocol_types.h"
#include "cluster/tx_utils.h"
#include "config/configuration.h"
#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"
#include "features/feature_table.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/offset_fetch.h"
#include "kafka/server/group_probe.h"
#include "kafka/server/stages.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "ssx/mutex.h"

#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/timer.hh>
#include <seastar/util/noncopyable_function.hh>

#include <functional>
#include <memory>
#include <optional>

namespace kafka {

/// \brief The committed and transactional offset state of one consumer group.
///
/// Owns the types a committed offset and an open transaction are held as.
class offset_store {
public:
    using clock_type = ss::lowres_clock;
    using time_point_type = clock_type::time_point;

    static constexpr int8_t fence_control_record_v0_version{0};
    static constexpr int8_t fence_control_record_v1_version{1};
    static constexpr int8_t fence_control_record_version{2};
    static constexpr int8_t prepared_tx_record_version{0};
    static constexpr int8_t commit_tx_record_version{0};
    static constexpr int8_t aborted_tx_record_version{0};

    /// Whether the owning group is being torn down. A commit that completes
    /// after that point must not apply its offsets.
    using group_is_dead_t = ss::noncopyable_function<bool()>;

    /**
     * represents an offset that is to be stored as a part of transaction
     */
    struct pending_tx_offset {
        group_tx::partition_offset offset_metadata;
        model::offset log_offset;
    };

    /**
     * In memory representation of active transaction. The transaction is added
     * when a state machine executes begin transaction request. The transaction
     * is removed when the state machine executes commit or abort transaction
     * request. The transaction holds all pending offset commits.
     */
    struct ongoing_transaction {
        ongoing_transaction(
          model::tx_seq,
          model::partition_id,
          model::timeout_clock::duration,
          model::offset);

        model::tx_seq tx_seq;
        model::partition_id coordinator_partition;

        model::timeout_clock::duration timeout;
        model::timeout_clock::time_point last_update;

        bool is_expiration_requested{false};
        model::offset begin_offset{-1};

        model::timeout_clock::time_point deadline() const {
            return last_update + timeout;
        }

        bool is_expired() const {
            return is_expiration_requested || deadline() <= clock_type::now();
        }

        void update_last_update_time() {
            last_update = model::timeout_clock::now();
        }

        chunked_hash_map<model::topic_partition, pending_tx_offset> offsets;
    };

    struct tx_producer {
        explicit tx_producer(model::producer_epoch);

        model::producer_epoch epoch;
        std::unique_ptr<ongoing_transaction> transaction;
    };

    using producers_map = chunked_hash_map<model::producer_id, tx_producer>;

    struct offset_metadata {
        model::offset log_offset;
        model::offset offset;
        ss::sstring metadata;
        kafka::leader_epoch committed_leader_epoch;
        model::timestamp commit_timestamp;
        std::optional<model::timestamp> expiry_timestamp;
        /*
         * this is an offset that was written prior to upgrading to redpanda
         * with offset retention support. because these offsets did not
         * persistent retention metadata we act conservatively and skip
         * automatic reclaim. offset delete api can be used to remove them.
         */
        bool non_reclaimable{false};

        fmt::iterator format_to(fmt::iterator it) const;
    };

    struct offset_metadata_with_probe {
        offset_metadata metadata;
        group_offset_probe probe;
        metrics_conversion_binding enable_group_metrics;

        offset_metadata_with_probe(
          offset_store::offset_metadata _metadata,
          const kafka::group_id& group_id,
          const model::topic_partition& tp,
          metrics_conversion_binding _enable_group_metrics)
          : metadata(std::move(_metadata))
          , probe(metadata.offset)
          , enable_group_metrics(std::move(_enable_group_metrics)) {
            const auto metrics_registration = [this, group_id, tp]() {
                if (enable_group_metrics().partition) {
                    probe.register_metrics(group_id, tp);
                    probe.register_public_metrics(group_id, tp);
                } else {
                    probe.deregister_metrics();
                    probe.deregister_public_metrics();
                }
            };

            enable_group_metrics.watch(metrics_registration);
            metrics_registration();
        }
    };

    using partition_offsets_map = chunked_hash_map<
      model::partition_id,
      std::unique_ptr<offset_metadata_with_probe>>;
    using offsets_map = chunked_hash_map<model::topic, partition_offsets_map>;
};

} // namespace kafka
