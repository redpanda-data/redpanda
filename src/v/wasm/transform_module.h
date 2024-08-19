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

#include "bytes/iobuf.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/transform.h"
#include "wasm/engine.h"
#include "wasm/ffi.h"
#include "wasm/wasi.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/util/noncopyable_function.hh>

#include <exception>

namespace wasm {
// Metadata about a record within a batch
struct record_metadata {
    // The size of the metadata with the record.
    // This is the serialized size of the metadata values below
    // plus the overall record size header.
    size_t metadata_size;
    // The length of the record's payload data.
    size_t payload_size;

    // Metadata we pass directly via the ABI (unserialized).
    model::record_attributes attributes;
    model::timestamp timestamp;
    model::offset offset;
};

/**
 * Callbacks for the transform lifecycle as records go through the VM.
 *
 * Generally the lifecycle callbacks are firing in a pattern as follows:
 *
 * pre_record();
 * emit({...});
 * emit({...});
 * emit({...});
 * post_record();
 * pre_record();
 * post_record();
 * pre_record();
 * emit({...});
 * post_record();
 *
 */
class record_callback {
public:
    record_callback() = default;
    record_callback(const record_callback&) = delete;
    record_callback(record_callback&&) = delete;
    record_callback& operator=(const record_callback&) = delete;
    record_callback& operator=(record_callback&&) = delete;
    virtual ~record_callback() = default;

    // Called before surfacing a record to the VM.
    virtual void pre_record() = 0;
    // Called for each record output from the VM.
    virtual ss::future<write_success>
      emit(std::optional<model::topic_view>, model::transformed_data) = 0;
    // Called after a VM specifies it's done with a record.
    virtual void post_record() = 0;
};

// The data needed during a single transformation of a record_batch
struct batch_transform_context {
    model::record_batch_header batch_header;
    iobuf batch_data;
    // The largest record size for the input batch, used so SDKs can
    // correctly allocate a batch of the right size.
    size_t max_input_record_size{0};
    // The remaining records to transform
    ss::chunked_fifo<record_metadata> records;
    record_callback* callback;
};

/**
 * The WASM module for redpanda transform specific host calls.
 *
 * This provides an ABI to WASM guests, as well as the mechanism for
 * guest<->host interactions (such as how we call into a wasm host and when).
 */
class transform_module {
public:
    explicit transform_module(wasi::preview1_module*);
    transform_module(const transform_module&) = delete;
    transform_module(transform_module&&) = delete;
    transform_module& operator=(const transform_module&) = delete;
    transform_module& operator=(transform_module&&) = delete;
    ~transform_module() = default;

    static constexpr std::string_view name = "redpanda_transform";

    /**
     * A helper method for correctly adhering to the ABI contract. Given a
     * batch, transform all the records, with a callback everytime a record is
     * consumed by the VM.
     */
    ss::future<> for_each_record_async(model::record_batch, record_callback*);

    /**
     * Start the transform module, marking it that the guest is about to start
     * and look for batches to process.
     */
    void start();

    /**
     * Waits for the guest to enter the runtime and mark that it is ready to
     * recieve batches for processing.
     */
    ss::future<> await_ready();

    /**
     * Stop the module, signalling that the VM guest has exited and is no longer
     * processing batches or that the host is stopping the VM.
     */
    void stop(const std::exception_ptr&);

    // Start ABI exports

    void check_abi_version_1();
    void check_abi_version_2();

    ss::future<int32_t> read_batch_header(
      int64_t* base_offset,
      int32_t* record_count,
      int32_t* partition_leader_epoch,
      int16_t* attributes,
      int32_t* last_offset_delta,
      int64_t* base_timestamp,
      int64_t* max_timestamp,
      int64_t* producer_id,
      int16_t* producer_epoch,
      int32_t* base_sequence);

    ss::future<int32_t> read_next_record(
      uint8_t* attributes,
      int64_t* timestamp_delta,
      model::offset* offset_delta,
      ffi::array<uint8_t>);

    ss::future<int32_t> write_record(ffi::array<uint8_t>);

    ss::future<int32_t>
      write_record_with_options(ffi::array<uint8_t>, ffi::array<uint8_t>);

    // End ABI exports

private:
    ss::future<> guest_wait_for_batch();
    ss::future<> host_wait_for_proccessing();

    // The following condition variables are optional so that they can be reset,
    // as engines can be restarted.

    // The condvar that the VM guest waits upon until it's ready to process
    // another batch.
    std::optional<ss::condition_variable> _guest_cond_var;
    // The condvar that the host (broker) waits upon until the guest has
    // finished processing the batch.
    std::optional<ss::condition_variable> _host_cond_var;

    std::optional<batch_transform_context> _call_ctx;
    wasi::preview1_module* _wasi_module;
};
} // namespace wasm
