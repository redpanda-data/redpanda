#pragma once

#include <memory>

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <smf/fbs_typed_buf.h>
#include <smf/macros.h>

#include "wal_nstpidx.h"
#include "wal_nstpidx_manager.h"
#include "wal_opts.h"
#include "wal_requests.h"

class wal_topics_manager {
 public:
  using topic_meta_map =
    ska::bytell_hash_map<wal_nstpidx,
                         smf::fbs_typed_buf<wal_topic_create_request>>;

 public:
  explicit wal_topics_manager(wal_opts o);

  /// \brief API for explicitly creating a topic.
  /// the request contains a set of partitions assigned to *this* shard
  ///
  seastar::future<std::unique_ptr<wal_create_reply>>
  create(wal_create_request r);

  /// \brief opens a topic partition - which means reading the
  /// $root/<ns>/<topic>.<partition>/metadata file
  ///
  seastar::future<> open(seastar::sstring ns, seastar::sstring topic,
                         int32_t partition);

  /// \brief gets the active topic-partition manager
  seastar::future<wal_nstpidx_manager *> get_manager(wal_nstpidx idx);

  /// \brief return a list of active stats such as
  /// log segment count, start and end offsets. Useful for clients
  /// connecting to a service'd WAL to know where to start consuming
  std::unique_ptr<wal_stats_reply> stats() const;

  /// \brief return a map of all topics managed
  /// useful for compaction thread
  const topic_meta_map &
  props() const {
    return props_;
  }

  /// \brief closes all of the active topic partition managers
  ///
  seastar::future<> close();

  /// \brief a copy of the write ahead log properties such as timeouts/log
  /// cleanups, etc
  ///
  const wal_opts opts;

  SMF_DISALLOW_COPY_AND_ASSIGN(wal_topics_manager);

 private:
  /// \brief *must* exec as a subroutine of open()
  /// for safety
  seastar::future<wal_topic_create_request *>
  nstpidx_props(wal_nstpidx idx, seastar::sstring props_dir);

 private:
  /// \brief main workhorse
  ska::bytell_hash_map<wal_nstpidx, std::unique_ptr<wal_nstpidx_manager>>
    mngrs_;
  /// \brief every topic, olds a set of properties for RBAC and other properties
  /// We store one copy for all partitions and pass a constant pointer to them
  topic_meta_map props_;
  seastar::semaphore serialize_open_{1};
  seastar::semaphore serialize_create_{1};
};

