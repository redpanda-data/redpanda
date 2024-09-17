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

#include "http/client.h"

namespace http {

ss::future<boost::beast::http::status>
status(http::client::response_stream_ref response);

ss::future<iobuf> drain(http::client::response_stream_ref response);

} // namespace http
