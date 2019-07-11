#include "ioutil/priority_manager.h"

#include <seastar/core/reactor.hh>

/// brief - inspired by the priority manager of scylla
priority_manager& priority_manager::get() {
    static thread_local priority_manager pm = priority_manager();
    return pm;
}

priority_manager::priority_manager()
  : commitlog_priority_(
    engine().register_one_priority_class("commitlog", 1000))
  , compaction_priority_(
      engine().register_one_priority_class("compaction", 1000))
  , stream_read_priority_(
      engine().register_one_priority_class("wal_read", 200))
  , stream_write_priority_(
      engine().register_one_priority_class("wal_write", 200))
  , default_priority_(
      engine().register_one_priority_class("defult_priority", 1)) {
}
