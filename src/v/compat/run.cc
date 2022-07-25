// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "compat/run.h"

#include "bytes/iobuf_istreambuf.h"
#include "compat/compat_check.h"
#include "compat/index_state_compat.h"
#include "compat/logger.h"
#include "compat/remote_segment_index_compat.h"
#include "json/writer.h"
#include "utils/file_io.h"

#include <seastar/core/coroutine.hh>

#include <rapidjson/istreamwrapper.h>

#include <filesystem>

namespace compat {

ss::future<void>
write_corpus(std::filesystem::path const& dir, compat_check& c) {
    vlog(compat_log.info, "generating {} data", c.name());
    c.random_init();

    vlog(compat_log.info, "writing {}", c.bin_file(dir));
    co_await write_fully(c.bin_file(dir), c.to_binary());

    vlog(compat_log.info, "writing {}", c.json_file(dir));
    co_await write_fully(c.json_file(dir), c.to_json());

    co_return;
}

ss::future<void>
read_corpus(std::filesystem::path const& dir, compat_check& c) {
    {
        vlog(compat_log.info, "reading {}", c.json_file(dir));
        auto os = co_await read_fully(c.json_file(dir));
        iobuf_istreambuf ibuf(os);
        std::istream stream(&ibuf);
        rapidjson::IStreamWrapper wrapper(stream);

        rapidjson::Document d;
        d.ParseStream(wrapper);

        c.json_init(d);
    }

    {
        vlog(compat_log.info, "reading {}", c.bin_file(dir));
        c.check_compatibility(co_await read_fully(c.bin_file(dir)));
    }

    vlog(compat_log.info, "{} check: OK", c.name());

    co_return;
}

ss::future<int> run(int ac, char** av) {
    auto checks = std::vector<std::unique_ptr<compat::compat_check>>{};
    checks.emplace_back(new index_state_compat{});
    checks.emplace_back(new remote_segment_index_compat{});

    auto read_corpus = false;
    auto write_corpus = false;
    for (auto i = 0; i != ac; ++i) {
        if (std::string_view{av[i]} == "--read-corpus") {
            read_corpus = true;
        } else if (std::string_view{av[i]} == "--write-corpus") {
            write_corpus = true;
        }
    }

    auto const corpus_dir = std::filesystem::path{av[ac - 1]};

    if (write_corpus) {
        if (auto const type = co_await ss::file_type(corpus_dir.string());
            !type.has_value()) {
            vlog(compat_log.info, "Creating corpus directory {}", corpus_dir);
            co_await ss::recursive_touch_directory(corpus_dir.string());
        } else if (type.value() != seastar::directory_entry_type::directory) {
            throw std::runtime_error(fmt::format(
              "Corpus directory path {} already exists"
              "but is not a directory",
              corpus_dir));
        } else {
            vlog(
              compat_log.info,
              "Corpus directory {} already exists",
              std::filesystem::current_path() / corpus_dir);
        }

        for (auto const& c : checks) {
            co_await compat::write_corpus(corpus_dir, *c);
        }
    }

    if (read_corpus) {
        for (auto const& c : checks) {
            co_await compat::read_corpus(corpus_dir, *c);
        }
    }

    co_return 0;
}

} // namespace compat
