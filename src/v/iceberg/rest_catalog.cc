/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/rest_catalog.h"

#include "iceberg/rest_client/catalog_client.h"
#include "iceberg/table_requests.h"
namespace iceberg {

namespace {

using http_status = enum boost::beast::http::status;
using errc = enum iceberg::catalog::errc;
using enum errc;

struct http_error_mapping_visitor {
    errc map_http_status(http_status status) const {
        if (status == http_status::not_found) {
            return not_found;
        }

        if (status == http_status::conflict) {
            return already_exists;
        }
        return unexpected_state;
    }

    errc operator()(const rest_client::http_call_error& err) const {
        return ss::visit(
          err,
          [this](http_status status) { return map_http_status(status); },
          [](const ss::sstring&) { return unexpected_state; });
    }

    errc operator()(const rest_client::json_parse_error&) const {
        return unexpected_state;
    }

    errc operator()(const rest_client::retries_exhausted&) const {
        return timedout;
    }

    errc operator()(const http::url_build_error&) const {
        return unexpected_state;
    }
};
// Translates domain error to more general catalog::errc.
errc map_error(
  std::string_view context, const rest_client::domain_error& error) {
    vlog(log.warn, "error returned when executing {} - {}", context, error);
    return std::visit(http_error_mapping_visitor{}, error);
}

table_metadata get_metadata(load_table_result&& result) {
    return std::move(result.metadata);
}

table_update::update copy(const table_update::update& update) {
    return ss::visit(
      update, [](const auto& u) { return table_update::update{u.copy()}; });
}

} // namespace

rest_catalog::rest_catalog(
  std::unique_ptr<rest_client::catalog_client> client,
  config::binding<std::chrono::milliseconds> request_timeout)
  : client_(std::move(client))
  , request_timeout_(std::move(request_timeout))
  , lock_("iceberg/rest-catalog") {}

ss::future<checked<table_metadata, catalog::errc>>
rest_catalog::load_table(const table_identifier& t_id) {
    vlog(log.trace, "load table {} requested", t_id);
    auto rtc = create_rtc();
    auto h = co_await lock_.get_units();

    co_return (co_await client_->load_table(t_id.ns, t_id.table, rtc))
      .transform(get_metadata)
      .transform_error([](const rest_client::domain_error& err) {
          return map_error("load_table", err);
      });
}

ss::future<checked<table_metadata, catalog::errc>> rest_catalog::create_table(
  const table_identifier& t_id,
  const schema& schema,
  const partition_spec& spec) {
    auto rtc = create_rtc();
    vlog(log.trace, "create table {} requested", t_id);
    create_table_request request{
      .name = t_id.table,
      .schema = schema.copy(),
      .partition_spec = spec.copy()};
    auto h = co_await lock_.get_units();

    auto res = co_await client_->create_table(t_id.ns, std::move(request), rtc);
    if (res.has_value()) {
        co_return get_metadata(std::move(res.value()));
    }
    auto err = map_error("create_table", res.error());
    if (err != catalog::errc::not_found) {
        co_return err;
    }

    // The namespace presumably doesn't exist. Create it and then try again.
    vlog(
      log.trace, "received not_found for table {}, creating namespace", t_id);
    create_namespace_request ns_request{
      .ns = t_id.ns.copy(),
    };
    auto ns_res = co_await client_->create_namespace(
      std::move(ns_request), rtc);
    if (!ns_res.has_value()) {
        auto ns_errc = map_error("create_namespace", ns_res.error());
        if (ns_errc != catalog::errc::already_exists) {
            co_return ns_errc;
        }
        // If the namespace already exists, presumably there was a race, so
        // fall through to retry table creation.
    }
    create_table_request retry_request{
      .name = t_id.table,
      .schema = schema.copy(),
      .partition_spec = spec.copy()};
    co_return (
      co_await client_->create_table(t_id.ns, std::move(retry_request), rtc))
      .transform(get_metadata)
      .transform_error([](const rest_client::domain_error& err) {
          return map_error("create_table_retry", err);
      });
}

ss::future<checked<void, catalog::errc>>
rest_catalog::drop_table(const table_identifier& table_ident, bool purge) {
    auto rtc = create_rtc();
    vlog(log.trace, "drop table {} requested, purge: {}", table_ident, purge);
    auto h = co_await lock_.get_units();

    auto res = co_await client_->drop_table(
      table_ident.ns, table_ident.table, purge, rtc);
    if (res.has_value()) {
        co_return outcome::success();
    }
    co_return map_error("drop_table", res.error());
}

ss::future<checked<std::nullopt_t, errc>>
rest_catalog::commit_txn(const table_identifier& t_id, transaction txn) {
    auto rtc = create_rtc();
    vlog(log.trace, "commit table update {} requested", t_id);
    if (txn.error()) {
        vlog(
          log.warn,
          "Expected transaction to have no errors when committing it with rest "
          "catalog. Current transaction error: {}",
          txn.error());
        co_return errc::unexpected_state;
    }

    commit_table_request req;
    req.identifier = t_id.copy();
    req.requirements = txn.updates().requirements.copy();
    req.updates.reserve(txn.updates().updates.size());

    for (const auto& u : txn.updates().updates) {
        req.updates.push_back(copy(u));
    }
    auto h = co_await lock_.get_units();
    co_return (co_await client_->commit_table_update(std::move(req), rtc))
      .transform([](commit_table_response&&) { return std::nullopt; })
      .transform_error([](const rest_client::domain_error& err) {
          return map_error("commit_txn", err);
      });
}

retry_chain_node rest_catalog::create_rtc() {
    return {as_, request_timeout_(), request_timeout_() / 10};
}

ss::future<> rest_catalog::stop() {
    as_.request_abort();
    return client_->shutdown();
}

}; // namespace iceberg
