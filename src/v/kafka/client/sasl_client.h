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
#include "kafka/client/broker.h"
#include "kafka/client/configuration.h"
#include "kafka/protocol/sasl_authenticate.h"
#include "kafka/protocol/sasl_handshake.h"
#include "random/generators.h"

#include <seastar/core/coroutine.hh>

namespace kafka::client {

ss::future<> do_authenticate(shared_broker_t, const configuration& config);
/*
 * SASL handshake negotiates mechanism. In this case that process is simple: if
 * the server doesn't support the requested mechanism there is no fallback.
 */
ss::future<> do_sasl_handshake(shared_broker_t broker, ss::sstring mechanism);

ss::future<> do_authenticate_scram256(
  shared_broker_t broker, ss::sstring username, ss::sstring password);

ss::future<> do_authenticate_scram512(
  shared_broker_t broker, ss::sstring username, ss::sstring password);

} // namespace kafka::client
