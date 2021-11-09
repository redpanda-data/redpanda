/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "bytes/iobuf.h"
#include "coproc/types.h"
#include "hashing/xx.h"
#include "model/metadata.h"

#include <absl/container/node_hash_map.h>

namespace v8_engine {

class code_database {
public:
    void insert_or_assign(coproc::script_id id, iobuf code) {
        _code_db.insert_or_assign(id, std::move(code));
    }

    void erase(coproc::script_id id) { _code_db.erase(id); }

    std::optional<iobuf> get_code(std::string_view name) {
        // rpk use xxhash_64 for create script_is from script name
        size_t id(xxhash_64(name.data(), name.size()));
        auto code = _code_db.find(coproc::script_id(id));
        if (code == _code_db.end()) {
            return std::nullopt;
        }

        return code->second.share(0, code->second.size_bytes());
    }

private:
    absl::node_hash_map<coproc::script_id, iobuf> _code_db;
};

} // namespace v8_engine