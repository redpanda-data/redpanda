/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_roles/types.h"
#include "http/client.h"

#include <memory>

namespace cloud_roles {

class apply_credentials {
public:
    class impl {
    public:
        /// Given a request header, adds authentication information to it.
        /// Depending on the implementation this can vary from a simple oauth
        /// token added as an authorization header, to multiple headers added
        /// along with signing of the request payload.
        virtual std::error_code
        add_auth(http::client::request_header& header) const = 0;

        /// Changes the authentication key and secret or the oauth token
        /// associated with the credentials. The concept of temporary
        /// credentials is based on periodically rotating tokens, and this
        /// method consumes the newly acquired tokens and uses them to authorize
        /// headers.
        virtual void reset_creds(credentials creds) = 0;

        virtual std::ostream& print(std::ostream& os) const = 0;

        virtual ~impl() = default;
    };

    explicit apply_credentials(std::unique_ptr<impl> impl)
      : _impl{std::move(impl)} {}

    std::error_code add_auth(http::client::request_header& header) const {
        return _impl->add_auth(header);
    }

    void reset_creds(credentials creds) {
        _impl->reset_creds(std::move(creds));
    }

    std::ostream& print(std::ostream& os) const { return _impl->print(os); }

private:
    std::unique_ptr<impl> _impl;
};

std::ostream& operator<<(std::ostream& os, const apply_credentials& ac);

/// Creates a credential applier based on the kind of credentials passed in. The
/// input credentials object is a sum type, and it can contain any one of the
/// variants for all supported credentials sources. This function uses the
/// underlying type to create the corresponding credential applier. If the input
/// is a gcp credential, then a gcp credential applier is created, and so on.
apply_credentials make_credentials_applier(credentials creds);

} // namespace cloud_roles
