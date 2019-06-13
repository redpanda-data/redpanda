#pragma once
#include "filesystem/page_cache_buffer_manager.h"

#include <seastar/core/memory.hh>

#include <smf/log.h>

#include <algorithm>
#include <optional>
#include <vector>

class page_cache_file_idx {
public:
    using page_range_ptr = page_cache_buffer_manager::page_cache_result_ptr;
    struct range_comparator {
        bool
        operator()(const page_range_ptr& lhs, const page_range_ptr& rhs) const {
            return lhs->begin_pageno < rhs->begin_pageno;
        }
    };

    // needs to be ordered for `lower_bound` operation
    using set_t = std::vector<page_range_ptr>;
    using iterator = set_t::iterator;

public:
    SMF_DISALLOW_COPY_AND_ASSIGN(page_cache_file_idx);
    explicit page_cache_file_idx(uint32_t fileid);

    /// brief global file id
    const uint32_t file_id;

    page_cache_result* range(int32_t pageno);

    void cache(page_range_ptr p) {
        auto it = as_iterator(p->begin_pageno);
        LOG_THROW_IF(
          it != _ranges.end(),
          "Could not insert page into info table. Have: {}, attempted "
          "to insert: {}",
          *it->get(),
          *p);
        _ranges.push_back(std::move(p));
        std::stable_sort(_ranges.begin(), _ranges.end(), range_comparator{});
    }
    void evict_pages(std::set<int32_t> pages);
    /// \brief evicts one page, started at the lowest tag count
    std::optional<page_range_ptr> try_evict();

private:
    iterator as_iterator(const int32_t pageno);

    ///  \brief erases and updates index
    page_range_ptr erase(iterator it);
    set_t _ranges;
};
