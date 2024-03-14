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

#include "kafka/protocol/describe_configs.h"
#include "kafka/server/handlers/configs/config_response_utils.h"

namespace kafka {

template<typename T>
ss::sstring describe_as_string(const T& t);

template<typename T, typename Func>
void add_topic_config_if_requested(
  const config_key_t& config_keys,
  config_response_container_t& result,
  std::string_view default_name,
  const T& default_value,
  std::string_view override_name,
  const std::optional<T>& overrides,
  bool include_synonyms,
  std::optional<ss::sstring> documentation,
  Func&& describe_f,
  bool hide_default_override = false);

template<typename T>
void add_topic_config_if_requested(
  const config_key_t& config_keys,
  config_response_container_t& result,
  std::string_view default_name,
  const std::optional<T>& default_value,
  std::string_view override_name,
  const tristate<T>& overrides,
  bool include_synonyms,
  std::optional<ss::sstring> documentation);

} // namespace kafka
