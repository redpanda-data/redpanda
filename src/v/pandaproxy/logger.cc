// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/logger.h"

#include "utils/truncating_logger.h"

namespace pandaproxy {
ss::logger plog{"pandaproxy"};
ss::logger srlog{"schemaregistry"};

ss::logger _preqs{"pandaproxy/requests"};
truncating_logger preqs(_preqs, max_log_line_bytes);

ss::logger _srreqs{"schemaregistry/requests"};
truncating_logger srreqs(_srreqs, max_log_line_bytes);

} // namespace pandaproxy
