/*
 * Copyright 2024 Redpanda Data, Inc.
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

#include <seastar/http/file_handler.hh>

namespace debug_bundle {

/**
 * A handler for reading files from the disk and streaming them to the client.
 */
class file_handler final : public ss::httpd::file_interaction_handler {
public:
    file_handler() = default;

    /**
     * read a file from the disk and stream it in the reply.
     * @param file the full path to a file on the disk
     * @param req the reuest
     * @param rep the reply
     */
    ss::future<std::unique_ptr<ss::http::reply>> handle(
      const ss::sstring& file,
      std::unique_ptr<ss::http::request> req,
      std::unique_ptr<ss::http::reply> rep) final;
};

} // namespace debug_bundle
