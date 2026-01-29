// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/schema_registry/test/store_fixture.h"

#include "pandaproxy/schema_registry/types.h"

#include <seastar/core/smp.hh>

namespace pandaproxy::schema_registry::test_utils {

store_fixture::store_fixture() {
    enable_qualified_subjects::set_local(true);
    _smp_svc_group = std::make_unique<ss::smp_service_group>(
      ss::create_smp_service_group(ss::smp_service_group_config{}).get());
    _store.start(is_mutable::yes, *_smp_svc_group).get();
}

store_fixture::~store_fixture() {
    _store.stop().get();
    ss::destroy_smp_service_group(*_smp_svc_group).get();
    _smp_svc_group.reset();
    enable_qualified_subjects::reset_local();
}

schema_id store_fixture::insert(
  const context_subject& sub,
  const schema_definition& schema_def,
  schema_version version) {
    const auto id = _next_id++;
    _store
      .upsert(
        seq_marker{},
        subject_schema{sub, schema_def.share()},
        id,
        version,
        is_deleted::no)
      .get();
    return id;
}

schema_id store_fixture::insert(
  const subject_schema& sub_schema, schema_version version) {
    return insert(sub_schema.sub(), sub_schema.def(), version);
}

} // namespace pandaproxy::schema_registry::test_utils
