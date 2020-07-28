#pragma once

#include "cluster/commands.h"
#include "cluster/controller_stm.h"
#include "cluster/partition_allocator.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/types.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/timeout_clock.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/sharded.hh>

#include <absl/container/flat_hash_map.h>

#include <system_error>

namespace cluster {

// on every core
class topics_frontend {
public:
    topics_frontend(
      model::node_id,
      ss::sharded<controller_stm>&,
      ss::sharded<rpc::connection_cache>&,
      ss::sharded<partition_allocator>&,
      ss::sharded<partition_leaders_table>&,
      ss::sharded<ss::abort_source>&);

    ss::future<std::vector<topic_result>> create_topics(
      std::vector<topic_configuration>, model::timeout_clock::time_point);

    ss::future<std::vector<topic_result>> delete_topics(
      std::vector<model::topic_namespace>, model::timeout_clock::time_point);

    ss::future<std::vector<topic_result>> autocreate_topics(
      std::vector<topic_configuration>, model::timeout_clock::duration);

private:
    ss::future<topic_result>
      do_create_topic(topic_configuration, model::timeout_clock::time_point);

    ss::future<topic_result> replicate_create_topic(
      topic_configuration,
      partition_allocator::allocation_units,
      model::timeout_clock::time_point);

    ss::future<topic_result>
      do_delete_topic(model::topic_namespace, model::timeout_clock::time_point);

    template<typename Cmd>
    ss::future<std::error_code>
    replicate_and_wait(Cmd&&, model::timeout_clock::time_point);

    ss::future<std::vector<topic_result>> dispatch_create_to_leader(
      model::node_id,
      std::vector<topic_configuration>,
      model::timeout_clock::duration);

    // returns true if the topic name is valid
    static bool validate_topic_name(const model::topic_namespace&);

    model::node_id _self;
    ss::sharded<controller_stm>& _stm;
    ss::sharded<partition_allocator>& _allocator;
    ss::sharded<rpc::connection_cache>& _connections;
    ss::sharded<partition_leaders_table>& _leaders;
    ss::sharded<ss::abort_source>& _as;
};

} // namespace cluster
