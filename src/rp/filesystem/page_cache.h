#pragma once
#include <array>
#include <chrono>
#include <cstdint>
#include <experimental/array>
#include <map>
#include <vector>

#include <boost/intrusive/list.hpp>
#include <seastar/core/sstring.hh>
// sstring must come before metrics
#include <seastar/core/metrics_registration.hh>

#include <bytell_hash_map.hpp>
#include <seastar/core/align.hh>
#include <seastar/core/file.hh>
#include <seastar/core/timer.hh>
#include <smf/macros.h>

#include "random/fast_prng.h"
// filesystem
#include "page_cache_buffer_manager.h"
#include "page_cache_file_idx.h"
#include "page_cache_request.h"
#include "page_cache_stats.h"

/*
  XXX: Add admissions controller.
  have a semaphore for the memory that the page_cache can actually handle.
  it needs to know the active readers so it can do max-min balancing across
  readers
*/

namespace rp {
/// \brief due to malloc(2) restrictions we can only use the
/// seastar::memory_reclaimer::sync mode. That is, the allocator
/// might reclaim memory in the middle of the routine.
/// We grow on cpu::idle and shrink on memory pressure.
class page_cache final {
 public:
  using page_range_ptr = page_cache_buffer_manager::page_cache_result_ptr;
  /// \brief *MAIN* API. No way to create an instance
  static page_cache &get();

  void evict_pages(uint32_t fileid, std::set<int32_t> pages);
  seastar::future<page_cache_result_lease> read(page_cache_request);
  seastar::future<> remove_file(uint32_t fileid);
  ~page_cache();

 private:
  SMF_DISALLOW_COPY_AND_ASSIGN(page_cache);
  /// \brief ctor
  explicit page_cache(int64_t min_reserve, int64_t max_limit);
  page_cache_result *try_get(page_cache_request r);
  void prefetch_next(page_cache_request current);
  void cache(uint32_t fileid, page_range_ptr data);
  page_cache_file_idx *index(uint32_t file);
  seastar::future<> prefetch(page_cache_request r,
                             page_cache_result::priority prio);

  /// \brief called during low memory pressure
  seastar::memory::reclaiming_result reclaim_region();

 private:
  page_cache_buffer_manager mngr_;
  seastar::memory::reclaimer reclaimer_;
  fast_prng rng_;
  // need random iterator for randomized eviction
  std::map<uint32_t, std::unique_ptr<page_cache_file_idx>> files_;
  seastar::metrics::metric_groups metrics_;
  page_cache_stats stats_;
};

}  // namespace rp
