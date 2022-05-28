/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "raft/kvelldb/kvrsm.h"
#include "seastarx.h"

#include <seastar/http/httpd.hh>
#include <seastar/http/reply.hh>
#include <seastar/http/request.hh>

namespace raft::kvelldb {

struct kv_record {
    ss::sstring key;
    ss::sstring value;
    ss::sstring write_id;
};

class httpkvrsm {
public:
    explicit httpkvrsm(
      ss::lw_shared_ptr<raft::kvelldb::kvrsm> kvrsm,
      const ss::sstring& server_name,
      ss::socket_address addr);

    ss::future<> start();

    ss::future<std::unique_ptr<ss::httpd::reply>> read(
      std::unique_ptr<ss::httpd::request> req,
      std::unique_ptr<ss::httpd::reply> rep);

    ss::future<std::unique_ptr<ss::httpd::reply>> write(
      std::unique_ptr<ss::httpd::request> req,
      std::unique_ptr<ss::httpd::reply> rep);

    ss::future<std::unique_ptr<ss::httpd::reply>> cas(
      std::unique_ptr<ss::httpd::request> req,
      std::unique_ptr<ss::httpd::reply> rep);

private:
    ss::lw_shared_ptr<raft::kvelldb::kvrsm> _kvrsm;
    ss::httpd::http_server _server;
    ss::socket_address _addr;
};

} // namespace raft::kvelldb