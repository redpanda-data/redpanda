#pragma once

#include <set>

#include <bytell_hash_map.hpp>
#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include <smf/macros.h>

namespace v {
// TODO(agallego) - needs a smaller test, bigger tests are hard to fail

struct wal_cold_boot {
  SMF_DISALLOW_COPY_AND_ASSIGN(wal_cold_boot);

  using topic_p = ska::bytell_hash_map<seastar::sstring, std::set<int32_t>>;
  using map_t = ska::bytell_hash_map<seastar::sstring, topic_p>;
  /// \brief - a map of (namespace -> (topic -> [partition]))
  ///          uses DFS as the search algorithm
  ///          Only indexes this *lcore*
  ///
  /// We want a map in case an administrator accidentally removes a partition
  /// We still want to service the rest of the partitions / topics
  /// for read at least even if we can't service some writes
  ///
  static seastar::future<wal_cold_boot>
  filesystem_lcore_index(seastar::sstring top_level_dir);

  explicit wal_cold_boot(seastar::sstring tld)
    : top_level_dir(std::move(tld)) {}
  wal_cold_boot(wal_cold_boot &&o) noexcept : fsidx(std::move(o.fsidx)) {}
  ~wal_cold_boot() = default;

  /// \brief first top level directory
  /// usually refers to the original WAL dir
  ///
  const seastar::sstring top_level_dir;

  /// \brief map of namespace,topic,partitions - main interface
  /// see filesystem_index() function
  ///
  map_t fsidx;

  /// --
  seastar::future<> visit_namespace(seastar::directory_entry de);
  seastar::future<> visit_topic(seastar::sstring ns,
                                seastar::directory_entry de);
  seastar::future<> visit_partition(seastar::sstring ns, seastar::sstring topic,
                                    seastar::directory_entry de);
};
}  // namespace v
