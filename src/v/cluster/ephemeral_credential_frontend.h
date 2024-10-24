/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/errc.h"
#include "features/fwd.h"
#include "model/metadata.h"
#include "rpc/fwd.h"
#include "security/ephemeral_credential.h"
#include "security/fwd.h"
#include "security/scram_credential.h"
#include "security/types.h"

#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>

namespace cluster {

/*
 * ephemeral_credential_frontend manages ephemeral credentials in memory as well
 * as allowing to propagate them to other nodes.
 */
class ephemeral_credential_frontend {
public:
    ephemeral_credential_frontend(
      model::node_id self,
      ss::sharded<security::credential_store>&,
      ss::sharded<security::ephemeral_credential_store>&,
      ss::sharded<features::feature_table>&,
      ss::sharded<rpc::connection_cache>&);

    ss::future<> start();
    ss::future<> stop();

    // Get a credential
    // Mint one if it doesn't exist
    struct get_return {
        security::ephemeral_credential credential;
        std::error_code err{errc::success};
    };
    ss::future<get_return> get(const security::acl_principal&);

    // Insert or update a scram_credential
    ss::future<> put(
      security::acl_principal,
      security::credential_user,
      security::scram_credential);

    // Insert or update an ephemeral_credential
    ss::future<> put(security::ephemeral_credential);

    // Inform a node of the credential for the given principal
    ss::future<std::error_code>
    inform(model::node_id, const security::acl_principal&);

private:
    model::node_id _self;
    ss::sharded<security::credential_store>& _c_store;
    ss::sharded<security::ephemeral_credential_store>& _e_store;
    ss::sharded<features::feature_table>& _feature_table;
    ss::sharded<rpc::connection_cache>& _connections;
    ss::gate _gate;
};

} // namespace cluster
