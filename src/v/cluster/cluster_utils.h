#pragma once
#include "cluster/types.h"
#include "raft/types.h"

namespace cluster {
/// This method calculates the machine nodes that were updated/added
/// and removed
brokers_diff calculate_changed_brokers(
  std::vector<broker_ptr> new_list, std::vector<broker_ptr> old_list);

} // namespace cluster