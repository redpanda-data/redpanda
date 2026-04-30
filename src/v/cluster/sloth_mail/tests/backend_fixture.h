/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "cluster/commands.h"
#include "cluster/sloth_mail/impl/backend_decl.h"
#include "cluster/sloth_mail/tests/config.h"
#include "model/fundamental.h"
#include "test_utils/test.h"

#include <ranges>

namespace cluster::sloth_mail {

extern template class impl::backend<tests::config>;

namespace tests {

using backend = impl::backend<tests::config>;

class backend_fixture : public seastar_test {
public:
    backend_fixture()
      : shipper()
      , backend_config(shipper)
      , members()
      , backend_instance({shipper}, model::node_id(0), members, 1000, 100ms) {}

    ss::future<> SetUpAsync() override {
        add_node(model::node_id{0});
        backend_instance.start();
        return ss::now();
    }

    ss::future<> TearDownAsync() override { return backend_instance.stop(); }

    void add_node(model::node_id id) {
        model::broker broker(
          id,
          net::unresolved_address("fake_host", 0),
          net::unresolved_address("fake_host", 0),
          {},
          model::broker_properties{
            .cores = 0, .available_memory_gb = 0, .available_disk_gb = 0});

        auto ec = members.apply(
          model::offset(0), cluster::add_node_cmd(0, broker));
        if (ec) {
            throw std::runtime_error(
              ss::format("unable to apply add node cmd: {}", ec.message()));
        }
    }

    void remove_node(model::node_id id) {
        auto ec = members.apply(
          model::offset(0), cluster::decommission_node_cmd(id, 0));
        if (ec) {
            throw std::runtime_error(
              ss::format(
                "unable to apply decommission node cmd: {}", ec.message()));
        }
        ec = members.apply(model::offset(0), cluster::remove_node_cmd(id, 0));
        if (ec) {
            throw std::runtime_error(
              ss::format("unable to apply remove node cmd: {}", ec.message()));
        }
    }

    template<typename Kind>
    using pairs_vector_of_kind = impl::types<
      tests::config::supported_kinds>::template pairs_vector_of_kind<Kind>;

    template<typename Kind>
    void validate_pairs(
      const pairs_vector_of_kind<Kind>& actual,
      const pairs_vector_of_kind<Kind>& expected) {
        if (to_std_map(actual) != to_std_map(expected)) {
            throw std::runtime_error(
              fmt::format(
                "data differ: expected={} vs actual={}",
                to_printable(expected),
                to_printable(actual)));
        }
    }

private:
    template<typename Kind>
    std::unordered_map<typename Kind::key_t, typename Kind::value_t>
    to_std_map(const pairs_vector_of_kind<Kind>& pairs) {
        return {std::from_range, pairs | std::views::transform([](auto& p) {
                                     return std::make_pair(p.key, p.value);
                                 })};
    }

    template<typename Kind>
    chunked_vector<std::string>
    to_printable(const pairs_vector_of_kind<Kind>& pairs) {
        return {std::from_range, pairs | std::views::transform([](auto& p) {
                                     return fmt::format(
                                       "({}, {})", p.key.key, p.value.value);
                                 })};
    }

protected:
    mock_shipper shipper;
    config backend_config;
    members_table members;
    backend backend_instance;
};

} // namespace tests
} // namespace cluster::sloth_mail
