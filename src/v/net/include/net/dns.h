/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once
#include "utils/unresolved_address.h"

namespace net {

/**
 * Resolves addresses using seastar DNS resolver. It uses mutex to workaround
 * seastar bug causing segmentation fault when udp channel is being accessed by
 * different fibers.
 */
ss::future<ss::socket_address> resolve_dns(unresolved_address);

} // namespace net
