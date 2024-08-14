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
#include "base/seastarx.h"

#include <seastar/core/future.hh>

namespace net {

/// Find CA trust file using the predefined set of locations
///
/// Historically, different linux distributions use different locations to
/// store certificates for their private key infrastructure. This is just a
/// convention and can't be queried by the application code. The application
/// is required to 'know' where to find the certs. In case of OpenSSL the
/// location is configured during build time. It depend on distribution on
/// which OpenSSL is built. This approach doesn't work for Redpanda because
/// single Redpanda binary can be executed on any linux distro. So the default
/// option only work on some distributions. The rest require the location to
/// be explicitly specified. This function does different thing. It probes
/// the set of default locations for different distributions untill it finds
/// the one that exists. This path is then passed to OpenSSL.
ss::future<std::optional<ss::sstring>> find_ca_file();

} // namespace net
