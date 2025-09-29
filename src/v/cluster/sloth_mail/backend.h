/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "config.h"
#include "impl/backend_decl.h"

namespace cluster::sloth_mail {
extern template class impl::backend<config>;
using backend = impl::backend<config>;
} // namespace cluster::sloth_mail
