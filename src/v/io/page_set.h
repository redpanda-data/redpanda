/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "io/interval_map.h"
#include "io/page.h"

#include <seastar/core/shared_ptr.hh>

#include <boost/iterator/iterator_adaptor.hpp>

namespace experimental::io {

/**
 * A container holding non-overlapping pages.
 */
class page_set {
    using map_type = interval_map<uint64_t, seastar::lw_shared_ptr<page>>;

public:
    /**
     * Container value iterator.
     */
    class const_iterator
      : public boost::iterator_adaptor<
          const_iterator,
          map_type::const_iterator,
          const seastar::lw_shared_ptr<page>,
          boost::iterators::forward_traversal_tag> {
    private:
        friend class boost::iterator_core_access;
        friend class page_set;
        explicit const_iterator(map_type::const_iterator it);
        [[nodiscard]] reference dereference() const;
    };

    /**
     * Find the page containing \p offset. If no such page exists, then the
     * returned iterator will compare equal to end().
     */
    [[nodiscard]] const_iterator find(uint64_t offset) const;

    /**
     * Add \p page to the container. The return value will contain true if
     * insertion succeeded, and the iterator will point to the newly inserted
     * page.
     *
     * If the page is empty or it overlaps an existing page then no insertion
     * will occur, and false will be returned. When the page is empty the
     * iterator will compare equal to end(), and when an overlap occurs the
     * iterator will point at some existing page that overlaps.
     */
    [[nodiscard]] std::pair<const_iterator, bool>
    insert(seastar::lw_shared_ptr<page> page);

    /**
     * Remove the page pointed to be \p it.
     */
    void erase(const_iterator it);

    /**
     * Return an iterator to the first page in the set.
     */
    [[nodiscard]] const_iterator begin() const;

    /**
     * Return an iterator to the end of the page set.
     */
    [[nodiscard]] const_iterator end() const;

private:
    map_type pages_;
};

} // namespace experimental::io
