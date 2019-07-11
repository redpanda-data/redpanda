#pragma once

#include "seastarx.h"

#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include <smf/macros.h>

#include <bytell_hash_map.hpp>
#include <set>

// TODO(agallego) - needs a smaller test, bigger tests are hard to fail

struct wal_cold_boot {
    SMF_DISALLOW_COPY_AND_ASSIGN(wal_cold_boot);

    using topic_p = ska::bytell_hash_map<sstring, std::set<int32_t>>;
    using map_t = ska::bytell_hash_map<sstring, topic_p>;
    /// \brief - a map of (namespace -> (topic -> [partition]))
    ///          uses DFS as the search algorithm
    ///          Only indexes this *lcore*
    ///
    /// We want a map in case an administrator accidentally removes a partition
    /// We still want to service the rest of the partitions / topics
    /// for read at least even if we can't service some writes
    ///
    static future<wal_cold_boot>
    filesystem_lcore_index(sstring top_level_dir);

    explicit wal_cold_boot(sstring tld)
      : top_level_dir(std::move(tld)) {
    }
    wal_cold_boot(wal_cold_boot&& o) noexcept
      : fsidx(std::move(o.fsidx)) {
    }
    ~wal_cold_boot() = default;

    /// \brief first top level directory
    /// usually refers to the original WAL dir
    ///
    const sstring top_level_dir;

    /// \brief map of namespace,topic,partitions - main interface
    /// see filesystem_index() function
    ///
    map_t fsidx;

    /// --
    future<> visit_namespace(directory_entry de);
    future<>
    visit_topic(sstring ns, directory_entry de);
    future<> visit_partition(
      sstring ns, sstring topic, directory_entry de);
};
