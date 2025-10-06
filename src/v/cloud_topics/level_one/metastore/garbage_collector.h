/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "base/seastarx.h"
#include "utils/named_type.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>

#include <expected>

namespace cloud_topics::l1 {

class io;
class simple_stm;

class garbage_collector {
public:
    garbage_collector(simple_stm* stm, io* io);

    using error = named_type<ss::sstring, struct gc_error_tag>;
    ss::future<std::expected<void, error>>
    remove_unreferenced_objects(ss::abort_source*);

private:
    // Callers are expected to ensure this object outlives the stm and io.
    simple_stm* stm_;
    io* io_;
};

} // namespace cloud_topics::l1
