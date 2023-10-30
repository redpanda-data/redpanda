/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/record.h"
#include "seastarx.h"
#include "serde/envelope.h"
#include "utils/named_type.h"

#include <seastar/core/sstring.hh>

#include <absl/container/btree_map.h>
#include <absl/container/flat_hash_map.h>

#include <cstdint>
#include <type_traits>

namespace model {
/**
 * An ID for a transform, these get allocated globally, to allow for users to
 * re-use names of transforms.
 */
using transform_id = named_type<int64_t, struct transform_id_tag>;
/**
 * The name of a transform, which is the user defined.
 *
 * Generally you should only use names when surfacing information to a user,
 * otherwise the ID should be used, especially if the information is persisted.
 */
using transform_name = named_type<ss::sstring, struct transform_name_tag>;

/**
 * Metadata for a WebAssembly powered data transforms.
 */
struct transform_metadata
  : serde::envelope<
      transform_metadata,
      serde::version<0>,
      serde::compat_version<0>> {
    // The user specified name of the transform.
    transform_name name;
    // The input topic being transformed.
    model::topic_namespace input_topic;
    // Right now we force there is only one, but we're mostly setup for
    // multiple. These are also validated to be unique, but we use a vector
    // here to preserve the user specified order (which is important as it
    // will be how the ABI boundary specifies which output topic to write too).
    std::vector<model::topic_namespace> output_topics;
    // The user specified environment variable configuration.
    absl::flat_hash_map<ss::sstring, ss::sstring> environment;
    // Each transform revision has a UUID, which is the key for the wasm_binary
    // topic and can be used to uniquely identify this revision.
    uuid_t uuid;
    // The offset of the committed WASM source in the wasm_binary topic.
    model::offset source_ptr;

    friend bool operator==(const transform_metadata&, const transform_metadata&)
      = default;

    friend std::ostream& operator<<(std::ostream&, const transform_metadata&);

    auto serde_fields() {
        return std::tie(
          name, input_topic, output_topics, environment, uuid, source_ptr);
    }
};

// key / value types used to track consumption offsets by transforms.
struct transform_offsets_key
  : serde::envelope<
      transform_offsets_key,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    transform_id id;
    // id of the partition from transform's input/source topic.
    partition_id partition;

    auto operator<=>(const transform_offsets_key&) const = default;

    friend std::ostream&
    operator<<(std::ostream&, const transform_offsets_key&);

    template<typename H>
    friend H AbslHashValue(H h, const transform_offsets_key& k) {
        return H::combine(std::move(h), k.id(), k.partition());
    }

    auto serde_fields() { return std::tie(id, partition); }
};

struct transform_offsets_value
  : serde::envelope<
      transform_offsets_value,
      serde::version<0>,
      serde::compat_version<0>> {
    kafka::offset offset;

    friend std::ostream&
    operator<<(std::ostream&, const transform_offsets_value&);

    auto serde_fields() { return std::tie(offset); }
};

static const model::topic transform_offsets_topic("transform_offsets");
static const model::topic_namespace transform_offsets_nt(
  model::kafka_internal_namespace, transform_offsets_topic);

/**
 * A (possibly inconsistent) snapshot of a transform.
 *
 * We capture the metadata associated with the transform for as well as the
 * state for each processor.
 */
struct transform_report
  : serde::
      envelope<transform_report, serde::version<0>, serde::compat_version<0>> {
    struct processor
      : serde::
          envelope<processor, serde::version<0>, serde::compat_version<0>> {
        enum class state : uint8_t { unknown, inactive, running, errored };
        model::partition_id id;
        state status;
        model::node_id node;
        friend bool operator==(const processor&, const processor&) = default;

        friend std::ostream& operator<<(std::ostream&, const processor&);

        auto serde_fields() { return std::tie(id, status, node); }
    };

    transform_report() = default;
    explicit transform_report(transform_metadata meta);
    transform_report(
      transform_metadata meta, absl::btree_map<model::partition_id, processor>);

    // The overall metadata for a transform.
    transform_metadata metadata;

    // The state of each processor of a transform.
    //
    // Currently, there should be a single processor for each partition on the
    // input topic.
    absl::btree_map<model::partition_id, processor> processors;

    friend bool operator==(const transform_report&, const transform_report&)
      = default;

    auto serde_fields() { return std::tie(metadata, processors); }

    // Add a processor's report to the overall transform's report.
    void add(processor);
};

std::string_view
processor_state_to_string(transform_report::processor::state state);

std::ostream&
operator<<(std::ostream& os, transform_report::processor::state s);

/**
 * A cluster wide view of all the currently running transforms.
 */
struct cluster_transform_report
  : serde::envelope<
      cluster_transform_report,
      serde::version<0>,
      serde::compat_version<0>> {
    absl::btree_map<model::transform_id, transform_report> transforms;

    friend bool
    operator==(const cluster_transform_report&, const cluster_transform_report&)
      = default;

    auto serde_fields() { return std::tie(transforms); }

    // Add a processor's report for a single transform to this overall report.
    void
    add(transform_id, const transform_metadata&, transform_report::processor);

    // Merge cluster views of transforms into this report.
    //
    // This is useful for aggregating multiple node's reports into a single
    // report.
    void merge(const cluster_transform_report&);
};

/**
 * The output of a user's transformed function.
 *
 * It is a buffer formatted with the "payload" of a record using Kafka's wire
 * format. Specifically the following fields are included:
 *
 * keyLength: varint
 * key: byte[]
 * valueLen: varint
 * value: byte[]
 * Headers => [Header]
 *
 * Where Header is:
 *
 * headerKeyLength: varint
 * headerKey: String
 * headerValueLength: varint
 * Value: byte[]
 *
 * See: https://kafka.apache.org/documentation/#record for more information.
 *
 */
class transformed_data {
public:
    /**
     * Create a transformed record - validating the format is correct.
     */
    static std::optional<transformed_data> create_validated(iobuf);

    /**
     * Generate a serialized record from the following metadata.
     */
    iobuf to_serialized_record(
      record_attributes, int64_t timestamp_delta, int32_t offset_delta) &&;

private:
    explicit transformed_data(iobuf d);

    iobuf _data;
};

} // namespace model
