/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

namespace encryption {

class kms_provider;
class dek_manager;
class schema_resolver;
class field_transformer;
class encryption_service;
struct encryption_services;

} // namespace encryption
