/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "kafka/client/transport.h"

#include "kafka/server/flex_versions.h"

namespace kafka::client {
void transport::write_header(
  response_writer& wr, api_key key, api_version version) {
    wr.write(int16_t(key()));
    wr.write(int16_t(version()));
    wr.write(int32_t(_correlation()));
    wr.write(std::string_view("test_client"));
    vassert(
      flex_versions::is_api_in_schema(key),
      "Attempted to send request to non-existent API: {}",
      key);
    if (flex_versions::is_flexible_request(key, version)) {
        /// Tags are unused by the client but to be protocol compliant
        /// with flex versions at least a 0 byte must be written
        wr.write_tags();
    }
    _correlation = _correlation + correlation_id(1);
}

} // namespace kafka::client
