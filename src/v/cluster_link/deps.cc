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

#include "cluster_link/deps.h"

#include "cluster/security_frontend.h"
#include "cluster_link/errc.h"
#include "cluster_link/utils.h"

namespace cluster_link {

namespace {
class security_impl : public security_service {
public:
    explicit security_impl(ss::sharded<cluster::security_frontend>* security_fe)
      : _security_fe(security_fe) {}
    ss::future<std::vector<cluster::errc>> create_acls(
      std::vector<security::acl_binding> bindings,
      ::model::timeout_clock::duration timeout) final {
        return _security_fe->local().create_acls(std::move(bindings), timeout);
    }

private:
    ss::sharded<cluster::security_frontend>* _security_fe;
};
} // namespace

std::unique_ptr<security_service> security_service::make_default(
  ss::sharded<cluster::security_frontend>* security_fe) {
    return std::make_unique<security_impl>(security_fe);
}

std::unique_ptr<kafka::client::cluster>
cluster_factory::create_cluster(const model::metadata& md) {
    return std::make_unique<kafka::client::cluster>(
      metadata_to_kafka_config(md));
}
} // namespace cluster_link
