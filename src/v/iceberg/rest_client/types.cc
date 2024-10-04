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

#include "iceberg/rest_client/types.h"

#include <boost/algorithm/string/join.hpp>

namespace iceberg::rest_client {

ss::sstring make_identifier(std::initializer_list<ss::sstring> identifiers) {
    return boost::algorithm::join(identifiers, "\x1f");
}

} // namespace iceberg::rest_client
