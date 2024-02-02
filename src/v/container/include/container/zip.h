/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include <boost/iterator/zip_iterator.hpp>

#include <tuple>

namespace container {
namespace detail {
template<typename IteratorTuple>
class zipper {
public:
    zipper(
      boost::zip_iterator<IteratorTuple> begin,
      boost::zip_iterator<IteratorTuple> end)
      : _begin(begin)
      , _end(end) {}

    auto begin() const { return _begin; }
    auto end() const { return _end; }

private:
    boost::zip_iterator<IteratorTuple> _begin;
    boost::zip_iterator<IteratorTuple> _end;
};
} // namespace detail

/**
 * A method to zip and iterate over two containers at the same time (at least
 * until std::views::zip exists).
 *
 * NOTE: Containers passed into this method **must** have the same length.
 *
 * Example usage:
 *
 * ```
 * std::vector<int> a = ...;
 * std::vector<std::string> b = ...;
 * for (const auto& [a_elem, b_elem] : zip(a, b)) {
 *   // do something fun!
 * }
 * ```
 */
template<typename... Args>
auto zip(Args&&... args) {
    auto begin = boost::make_zip_iterator(boost::make_tuple((args.begin())...));
    auto end = boost::make_zip_iterator(boost::make_tuple((args.end())...));
    return detail::zipper(std::move(begin), std::move(end));
}
} // namespace container
