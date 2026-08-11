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

#include "bytes/iobuf.h"

#include <seastar/util/bool_class.hh>
#include <seastar/util/log.hh>

#include <boost/property_tree/ptree.hpp>

namespace xml {

/// Controls whether iobuf_to_ptree dumps a prefix of the input buffer to the
/// log when XML parsing fails. Callers whose response bodies may contain
/// secrets (e.g. STS credentials) must pass `no`.
using log_body_on_failure = ss::bool_class<struct log_body_on_failure_tag>;

/// \brief: Convert iobuf that contains xml data to boost::property_tree
boost::property_tree::ptree iobuf_to_ptree(
  iobuf&& buf,
  ss::logger& logger,
  log_body_on_failure log_body = log_body_on_failure::yes);

/// A type usable with boost::property_tree's typed extraction.
///
/// ss::sstring is excluded because its stream extraction reads only until
/// whitespace, which would truncate output - use std::string instead for
/// string parsing from a boost::property_tree.
template<typename T>
concept PtreeExtractable = !std::is_same_v<T, ss::sstring>;

template<PtreeExtractable T>
decltype(auto) get_from_ptree(
  const boost::property_tree::ptree& tree,
  const boost::property_tree::ptree::path_type& path) {
    return tree.get<T>(path);
}

template<PtreeExtractable T>
decltype(auto) get_from_ptree(
  const boost::property_tree::ptree& tree,
  const boost::property_tree::ptree::path_type& path,
  const T& default_value) {
    return tree.get<T>(path, default_value);
}

template<PtreeExtractable T>
decltype(auto) get_optional_from_ptree(
  const boost::property_tree::ptree& tree,
  const boost::property_tree::ptree::path_type& path) {
    return tree.get_optional<T>(path);
}

} // namespace xml
