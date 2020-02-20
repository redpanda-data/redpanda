#pragma once
#include "hashing/jump_consistent_hash.h"
#include "hashing/xx.h"
#include "kafka/types.h"
#include "model/fundamental.h"
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
    model::ntp ntp_for(const kafka::group_id& group) const {
        incremental_xxhash64 inc;
        inc.update(group);
        auto p = static_cast<model::partition_id::type>(
          jump_consistent_hash(inc.digest(), _num_partitions));
        return model::ntp{
          .ns = _ns,
          .tp = model::topic_partition{_topic, model::partition_id{p}},
        };
    }

private:
    model::ns _ns;
    model::topic _topic;
    model::partition_id::type _num_partitions{};
};

} // namespace kafka
