/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_io/io_result.h"

namespace cloud_io {

std::ostream& operator<<(std::ostream& o, const download_result& r) {
    switch (r) {
    case download_result::success:
        o << "{success}";
        break;
    case download_result::notfound:
        o << "{key_not_found}";
        break;
    case download_result::timedout:
        o << "{timed_out}";
        break;
    case download_result::failed:
        o << "{failed}";
        break;
    };
    return o;
}

std::ostream& operator<<(std::ostream& o, const upload_result& r) {
    switch (r) {
    case upload_result::success:
        o << "{success}";
        break;
    case upload_result::timedout:
        o << "{timed_out}";
        break;
    case upload_result::failed:
        o << "{failed}";
        break;
    case upload_result::cancelled:
        o << "{cancelled}";
        break;
    };
    return o;
}

} // namespace cloud_io
