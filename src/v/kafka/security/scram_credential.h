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
#pragma once
#include "bytes/bytes.h"

#include <iosfwd>

namespace kafka {

class scram_credential {
public:
    scram_credential() noexcept = default;

    scram_credential(
      bytes salt, bytes server_key, bytes stored_key, int iterations) noexcept
      : _salt(std::move(salt))
      , _server_key(std::move(server_key))
      , _stored_key(std::move(stored_key))
      , _iterations(iterations) {}

    const bytes& salt() const { return _salt; }
    const bytes& server_key() const { return _server_key; }
    const bytes& stored_key() const { return _stored_key; }
    int iterations() const { return _iterations; }

    bool operator==(const scram_credential&) const = default;

private:
    friend std::ostream& operator<<(std::ostream&, const scram_credential&);

    bytes _salt;
    bytes _server_key;
    bytes _stored_key;
    int _iterations{0};
};

std::ostream& operator<<(std::ostream&, const kafka::scram_credential&);

} // namespace kafka
