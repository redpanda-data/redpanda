/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "bytes/iobuf.h"

#include <arrow/type.h>
#include <parquet/arrow/writer.h>

namespace datalake {

class arrow_to_iobuf {
public:
    explicit arrow_to_iobuf(std::shared_ptr<arrow::Schema> schema);

    void add_arrow_array(std::shared_ptr<arrow::Array> data);

    // Get the current pending data to be written and clear the internal state.
    iobuf take_iobuf();

    // Close the writer and get any remaining data that is pending.
    iobuf close_and_take_iobuf();

private:
    class iobuf_output_stream;

    std::shared_ptr<iobuf_output_stream> _ostream;
    std::unique_ptr<parquet::arrow::FileWriter> _writer;
};

} // namespace datalake
