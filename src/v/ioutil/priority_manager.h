#pragma once

#include <seastar/core/file.hh>
#include <smf/macros.h>

/// brief - inspired by the priority manager of scylla
class priority_manager {
 public:
  SMF_DISALLOW_COPY_AND_ASSIGN(priority_manager);

  inline const seastar::io_priority_class &
  commitlog_priority() {
    return commitlog_priority_;
  }
  inline const seastar::io_priority_class &
  compaction_priority() {
    return compaction_priority_;
  }
  inline const seastar::io_priority_class &
  streaming_read_priority() {
    return stream_read_priority_;
  }
  inline const seastar::io_priority_class &
  streaming_write_priority() {
    return stream_write_priority_;
  }
  inline const seastar::io_priority_class &
  default_priority() {
    return default_priority_;
  }

  /// \brief main API
  static priority_manager &get();

 private:
  priority_manager();

  // high
  seastar::io_priority_class commitlog_priority_;
  seastar::io_priority_class compaction_priority_;
  // low
  seastar::io_priority_class stream_read_priority_;
  seastar::io_priority_class stream_write_priority_;
  // lowest
  seastar::io_priority_class default_priority_;
};

