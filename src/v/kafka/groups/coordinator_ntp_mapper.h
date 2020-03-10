#pragma once
#include "cluster/metadata_cache.h"
#include "cluster/namespace.h"
#include "hashing/jump_consistent_hash.h"
#include "hashing/xx.h"
#include "kafka/types.h"
#include "model/fundamental.h"
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
    coordinator_ntp_mapper(ss::sharded<cluster::metadata_cache>& md)
      : _md(md)
      , _ns(cluster::kafka_internal_namespace)
      , _topic(cluster::kafka_group_topic) {}

    std::optional<model::ntp> ntp_for(const kafka::group_id& group) const {
        auto md = _md.local().get_topic_metadata(_topic);
        if (!md) {
            return std::nullopt;
        }
        incremental_xxhash64 inc;
        inc.update(group);
        auto p = static_cast<model::partition_id::type>(
          jump_consistent_hash(inc.digest(), md->partitions.size()));
        return model::ntp{
          .ns = _ns,
          .tp = model::topic_partition{_topic, model::partition_id{p}},
        };
    }

    const model::ns& ns() const { return _ns; }
    const model::topic& topic() const { return _topic; }

private:
    ss::sharded<cluster::metadata_cache>& _md;
    model::ns _ns;
    model::topic _topic;
};

} // namespace kafka
