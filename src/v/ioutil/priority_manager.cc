#include "priority_manager.h"

#include <seastar/core/reactor.hh>

/// brief - inspired by the priority manager of scylla
namespace v {
priority_manager &
priority_manager::get() {
  static thread_local priority_manager pm = priority_manager();
  return pm;
}

priority_manager::priority_manager()
  : commitlog_priority_(
      seastar::engine().register_one_priority_class("v::commitlog", 1000)),
    compaction_priority_(
      seastar::engine().register_one_priority_class("v::compaction", 1000)),
    stream_read_priority_(
      seastar::engine().register_one_priority_class("v::wal_read", 200)),
    stream_write_priority_(
      seastar::engine().register_one_priority_class("v::wal_write", 200)),
    default_priority_(
      seastar::engine().register_one_priority_class("v::defult_priority", 1)) {}

}  // namespace v
