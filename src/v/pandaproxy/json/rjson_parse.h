/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "json/encodings.h"
#include "json/reader.h"
#include "pandaproxy/json/types.h"

namespace pandaproxy::json {

template<typename encoding = ::json::UTF8<>>
class base_handler
  : public ::json::BaseReaderHandler<encoding, base_handler<encoding>> {
public:
    using Ch = typename encoding::Ch;

    explicit base_handler(serialization_format fmt = serialization_format::none)
      : _fmt{fmt} {}

    bool Default() { return false; }
    serialization_format format() const { return _fmt; }

private:
    serialization_format _fmt{serialization_format::none};
};

} // namespace pandaproxy::json
