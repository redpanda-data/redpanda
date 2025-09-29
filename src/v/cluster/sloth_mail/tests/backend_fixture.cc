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
#include "cluster/sloth_mail/impl/backend_impl.h"
#include "cluster/sloth_mail/tests/config.h"

namespace cluster::sloth_mail {
template class impl::backend<tests::config>;
} // namespace cluster::sloth_mail
