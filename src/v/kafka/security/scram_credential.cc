/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "kafka/security/scram_credential.h"

#include <ostream>

namespace kafka {

std::ostream& operator<<(std::ostream& os, const scram_credential& cred) {
    fmt::print(
      os,
      "salt {} server_key {} stored_key {} iterations {}",
      cred._salt,
      cred._server_key,
      cred._stored_key,
      cred._iterations);
    return os;
}

} // namespace kafka
