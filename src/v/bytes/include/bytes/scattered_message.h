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
#pragma once
#include "bytes/iobuf.h"

#include <seastar/core/scattered_message.hh>

/// \brief keeps the iobuf in the deferred destructor of scattered_msg<char>
/// and wraps each details::io_fragment as a scattered_message<char>::static()
/// const char*
ss::scattered_message<char> iobuf_as_scattered(iobuf b);
