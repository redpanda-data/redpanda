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

#include <algorithm>
#include <functional>
#include <iterator>
#include <span>

/// \brief: Perfom lower bound on a span while excluding certain entries.
/// It's a standard binary search, but if we land on an element that
/// has to be filtered out we iterate backwards (and then forwards)
/// and attempt to find an element which is included.
///
/// Note that the elements which pass the filter should be sorted for
/// this function to return correct results.
///
///
/// \param span: the span to search inside
/// \param needle: the value to search for
/// \param comp: the comparator to use; only elements that pass the 'filter'
/// will be used
/// \param filter: callable that returns 'true' if the element should be
/// considered and 'false' otherwise
/// \return iterator to the result; the result should be the same as using
/// std::lower_bound on the filtered span. Should be equivalent to the
/// result of:
/// std::erase_if(begin, end, std::not_fn(filter))
/// std::lower_bound(begin, end, comp);
///
/// TODO(vlad): Come back and make  this more elegant when not in a rush.
template<typename Iterator, typename Needle, typename Compare, typename Filter>
inline Iterator filtered_lower_bound(
  Iterator begin, Iterator end, Needle needle, Compare comp, Filter filter) {
    auto container_end = end;

    while (begin != container_end && !filter(*begin)) {
        ++begin;
    }

    while (begin != end) {
        auto step = std::distance(begin, end) / 2;

        Iterator middle = begin + step;
        bool nothing_backwards = false;
        if (!filter(*middle)) {
            auto backwards = container_end;
            for (auto iter = middle;; --iter) {
                if (filter(*iter)) {
                    backwards = iter;
                    break;
                }

                if (iter == begin) {
                    backwards = end;
                    nothing_backwards = true;
                    break;
                }
            }

            if (backwards != end) {
                middle = backwards;
            } else {
                auto forwards = end;
                for (auto iter = middle; iter != end; ++iter) {
                    if (filter(*iter)) {
                        forwards = iter;
                        break;
                    }
                }

                if (forwards != end) {
                    middle = forwards;
                } else {
                    // Search area contains no elements that pass the filtering
                    return container_end;
                }
            }
        }

        if (comp(*middle, needle)) {
            begin = middle + 1;
            while (begin != end && !filter(*begin)) {
                ++begin;
            }
        } else {
            if (nothing_backwards) {
                begin = middle;
                end = middle;
            } else {
                end = middle;
            }
        }
    }

    return begin;
}
