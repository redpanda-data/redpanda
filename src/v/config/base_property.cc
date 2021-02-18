// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/base_property.h"

#include "config/config_store.h"

#include <ostream>

namespace config {
base_property::base_property(
  config_store& conf,
  std::string_view name,
  std::string_view desc,
  required req)
  : _name(name)
  , _desc(desc)
  , _required(req) {
    conf._properties.emplace(name, this);
}

std::ostream& operator<<(std::ostream& o, const base_property& p) {
    p.print(o);
    return o;
}

}; // namespace config
