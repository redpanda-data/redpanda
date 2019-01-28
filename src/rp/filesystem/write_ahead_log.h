#pragma once

#include <memory>
#include <ostream>
#include <utility>

#include <seastar/core/distributed.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include <smf/macros.h>

#include "wal_compaction_manager.h"
#include "wal_opts.h"
#include "wal_requests.h"
#include "wal_topics_manager.h"

namespace rp {

/// brief - write ahead log
class write_ahead_log {
 public:
  explicit write_ahead_log(wal_opts opt);

  /// \brief returns the starting_offset of `this` write & ending_offset
  ///
  seastar::future<std::unique_ptr<wal_create_reply>>
  create(wal_create_request r);

  /// \brief returns the starting_offset of `this` write & ending_offset
  ///
  seastar::future<std::unique_ptr<wal_write_reply>> append(wal_write_request r);

  /// \brief given a topic, partition set, it will return the data
  /// specified up to a max in the read_request
  ///
  seastar::future<std::unique_ptr<wal_read_reply>> get(wal_read_request r);

  /// \brief gets the basic stats of all the topic-partitions
  ///
  std::unique_ptr<wal_stats_reply> stats() const;

  /// \brief open the write ahead log & initialize
  /// root directory - if not present created *ONLY*
  /// by core 0
  ///
  seastar::future<> open();
  /// \brief closes the file handles, and all associated
  /// topic partitions that were open by this lcore
  ///
  seastar::future<> close();
  /// \brief scans the root directory for topic/partitions
  /// belonging to this core local, then initiates the
  /// recovery / indexing per log segment written
  ///
  seastar::future<> index();
  /// \brief support seastar shardable
  ///
  seastar::future<>
  stop() {
    return close();
  }

  ~write_ahead_log() = default;
  SMF_DISALLOW_COPY_AND_ASSIGN(write_ahead_log);
  const wal_opts opts;

 private:
  wal_topics_manager tm_;
  wal_compaction_manager cm_;
};

}  // namespace rp
