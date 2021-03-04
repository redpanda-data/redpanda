/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "cluster/metadata_cache.h"
#include "hashing/jump_consistent_hash.h"
#include "hashing/xx.h"
#include "kafka/types.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "seastar/core/sharded.hh"
#include "seastarx.h"
#include "utils/concepts-enabled.h"

#include <seastar/core/reactor.hh>

namespace kafka {

/**
 * \brief Mapping from kafka coordinator key to ntp.
 *
 * A kafka coordinator key is an identifier for a coordinator resource, such as
 * a consumer group or transaction coordinator. Every coordinator is associated
 * with a single partition that it uses to persist its state. All coordinator
 * operations are handled by the active coordinator which is the the leader for
 * the partition that the coordinator key is associated with.
 *
 * This class maintains the mapping between coordinator keys and their
 * associated ntps. Currently this is implemented with a static ntp
 * configuration and a hash-based mapping between key and partition number.
 *
 * However, future iterations of the system may instead wish to maintain
 * explicit and possibly dynamic mappings that are discovered from a source such
 * as the controller much like partition assignments are discovered.
 */
class coordinator_ntp_mapper {
public:
    explicit coordinator_ntp_mapper(ss::sharded<cluster::metadata_cache>& md)
      : _md(md)
      , _tp_ns(model::kafka_internal_namespace, model::kafka_group_topic) {}

    std::optional<model::ntp> ntp_for(const kafka::group_id& group) const {
        auto md = _md.local().get_topic_metadata(_tp_ns);
        if (!md) {
            return std::nullopt;
        }
        incremental_xxhash64 inc;
        inc.update(group);
        auto p = static_cast<model::partition_id::type>(
          jump_consistent_hash(inc.digest(), md->partitions.size()));
        return model::ntp(_tp_ns.ns, _tp_ns.tp, model::partition_id{p});
    }

    const model::ns& ns() const { return _tp_ns.ns; }
    const model::topic& topic() const { return _tp_ns.tp; }

private:
    ss::sharded<cluster::metadata_cache>& _md;
    model::topic_namespace _tp_ns;
};

} // namespace kafka
