#include "filesystem/page_cache_file_idx.h"

#include "filesystem/page_cache_result.h"

#include <seastar/core/reactor.hh>

#include <smf/log.h>

using page_range_ptr = page_cache_file_idx::page_range_ptr;
using set_t = page_cache_file_idx::set_t;

page_cache_file_idx::page_cache_file_idx(uint32_t fileid)
  : file_id(fileid) {
}
struct page_comparator {
    bool operator()(const int32_t& pageno, const page_range_ptr& p) const {
        return pageno < p->end_pageno() - 1;
    }
    bool operator()(const page_range_ptr& p, const int32_t& pageno) const {
        return p->end_pageno() - 1 < pageno;
    }
};

page_cache_file_idx::iterator page_cache_file_idx::as_iterator(const int32_t pageno) {
    auto it = std::lower_bound(
      _ranges.begin(), _ranges.end(), pageno, page_comparator{});
    if (it != _ranges.end()) {
        // only return if in range.
        if (!it->get()->is_page_in_range(pageno))
            return _ranges.end();
    }
    return it;
}

page_cache_result* page_cache_file_idx::range(int32_t pageno) {
    auto it = as_iterator(pageno);
    if (it == _ranges.end()) {
        return nullptr;
    }
    auto retval = it->get();
    if (retval->marked_for_eviction) {
        return nullptr;
    }
    DLOG_THROW_IF(
      !retval->is_page_in_range(pageno),
      "Page out of bounds: {}, for {}",
      pageno,
      *retval);
    // success case!
    return retval;
}

void page_cache_file_idx::evict_pages(std::set<int32_t> pages) {
    while (!pages.empty()) {
        const int32_t pageno = *pages.begin();
        auto* r = range(pageno);
        pages.erase(pageno);
        if (r != nullptr) {
            int32_t i = pageno;
            // most important method
            r->marked_for_eviction = true;
            while (!pages.empty() && r->is_page_in_range(i)) {
                pages.erase(i++);
            }
            if (r->is_evictable()) {
                erase(as_iterator(pageno));
            }
        }
    }
}

page_range_ptr page_cache_file_idx::erase(page_cache_file_idx::iterator it) {
    // return & update database
    std::swap(*it, _ranges.back());
    page_range_ptr retval = std::move(_ranges.back());
    _ranges.pop_back(); // remove!
    std::stable_sort(_ranges.begin(), _ranges.end(), range_comparator{});
    return retval;
}
std::optional<page_range_ptr> page_cache_file_idx::try_evict() {
    std::vector<page_cache_result*> evictable;
    // try finding one that is evictable && low priority & evict it!
    auto it = _ranges.begin();
    auto lowest_pending_reads = _ranges.end();
    for (; it != _ranges.end(); it++) {
        auto ptr = it->get();
        if (ptr->is_evictable()) {
            if (
              lowest_pending_reads == _ranges.end()
              || ptr->pending_reads
                   < lowest_pending_reads->get()->pending_reads) {
                lowest_pending_reads = it;
            }
            if (ptr->prio == page_cache_result::priority::low) {
                return erase(it);
            }
        }
    }
    if (lowest_pending_reads != _ranges.end()) {
        return erase(lowest_pending_reads);
    }
    return std::nullopt;
}
