/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include <string>

namespace seastar {
template<typename char_type, typename Size, Size max_size, bool NulTerminate>
class basic_sstring;
} // namespace seastar

namespace boost::property_tree {

template<typename Internal, typename External>
struct translator_between;

template<
  typename Ch,
  typename Traits,
  typename Alloc,
  typename CharT,
  typename Size,
  Size MaxSize,
  bool NulTerminate>
struct translator_between<
  std::basic_string<Ch, Traits, Alloc>,
  ::seastar::basic_sstring<CharT, Size, MaxSize, NulTerminate>> {
    static_assert(
      false,
      "boost::property_tree::ptree::get<seastar::sstring> silently returns "
      "the default for non-empty trees because seastar::sstring lacks a "
      "stream extractor. Use std::string and convert at the call site.");
};

} // namespace boost::property_tree
