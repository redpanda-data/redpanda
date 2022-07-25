// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "bytes/iobuf.h"
#include "compat/json_helpers.h"

#include <filesystem>
#include <memory>
#include <string>
#include <vector>

namespace compat {

/**
 * Workflow:
 *
 * struct my_compat_check : public compat_check { ... };
 *
 * // Write corpus.
 * auto c = my_compat_check{};
 * c.random_initialize();
 * write_file(c.to_json(), corpus_dir / "my.json");
 * write_file(c.to_binary(), corpus_dir / "my.bin");;
 *
 *
 * // Read & check corpus.
 * auto c = my_compat_check{};
 * c.init_from_json(read_file("my.json"));
 * c.check_compatibility(read_file("my.bin"));
 */
struct compat_check {
    virtual ~compat_check() = default;

    // called to generate the random data
    virtual void random_init() = 0;

    // called to read a JSON file
    virtual void json_init(rapidjson::Value& w) = 0;

    // called to dump the data as binary
    virtual iobuf to_binary() = 0;

    // called to dump the data as JSON
    virtual iobuf to_json() const = 0;

    // called to check if this version can read data
    // from another version that's marked compatible
    virtual bool check_compatibility(iobuf&& binary) const = 0;

    // name (for filenames)
    virtual std::string name() const = 0;

    std::filesystem::path bin_file(std::filesystem::path const& corpus) {
        return corpus / (name() + ".bin");
    }

    std::filesystem::path json_file(std::filesystem::path const& corpus) {
        return corpus / (name() + ".json");
    }
};

} // namespace compat
