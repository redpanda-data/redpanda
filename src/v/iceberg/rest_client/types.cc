/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/rest_client/types.h"

#include "iceberg/logger.h"

namespace iceberg::rest_client {
using errc = enum iceberg::catalog::errc;
using enum errc;

using http_status = enum boost::beast::http::status;

errc domain_error_mapper::operator()(http_status status) const {
    if (status == http_status::not_found) {
        return not_found;
    }

    if (status == http_status::conflict) {
        return already_exists;
    }
    return unexpected_state;
}

errc domain_error_mapper::operator()(http_call_error err) const {
    return std::visit(domain_error_mapper{}, err);
}

errc domain_error_mapper::operator()(ss::sstring) const {
    return unexpected_state;
}

errc domain_error_mapper::operator()(json_parse_error) const {
    return unexpected_state;
}

errc domain_error_mapper::operator()(retries_exhausted) const {
    return timedout;
}

errc domain_error_mapper::operator()(http::url_build_error) const {
    return unexpected_state;
}

// Translates domain error to more general catalog::errc.
errc map_error(const domain_error& error, std::string_view context) {
    vlog(log.error, "{}: {}", context, error);
    return std::visit(domain_error_mapper{}, error);
}

std::ostream&
domain_error_printer::operator()(http::url_build_error err) const {
    return os << err;
}

std::ostream& domain_error_printer::operator()(json_parse_error err) const {
    return os << "json_parse_error{{context:" << err.context
              << ",error:" << err.error << "}}";
}

std::ostream& domain_error_printer::operator()(http_call_error err) const {
    std::visit([this](auto error) { os << error; }, err);
    return os;
}

std::ostream& domain_error_printer::operator()(retries_exhausted err) const {
    return os << fmt::format(
             "retries_exhausted:[{}]", fmt::join(err.errors, ","));
}

std::ostream& operator<<(std::ostream& os, domain_error err) {
    std::visit(domain_error_printer{.os = os}, err);
    return os;
}

} // namespace iceberg::rest_client
