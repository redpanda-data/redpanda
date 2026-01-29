// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "pandaproxy/schema_registry/types.h"

namespace pp = pandaproxy;
namespace pps = pp::schema_registry;

namespace pandaproxy::schema_registry::test_utils {

ss::sstring make_proto_schema(const pps::context_subject& sub, int n_fields);

std::string sanitize(
  std::string_view raw_proto,
  pps::normalize norm = pps::normalize::no,
  pps::output_format format = pps::output_format::none);

std::string normalize(std::string_view raw_proto);

} // namespace pandaproxy::schema_registry::test_utils
