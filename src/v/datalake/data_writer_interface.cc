/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "datalake/data_writer_interface.h"

#include <fmt/core.h>
namespace datalake {
std::string data_writer_error_category::message(int ev) const {
    switch (static_cast<data_writer_error>(ev)) {
    case data_writer_error::ok:
        return "Ok";
    case data_writer_error::parquet_conversion_error:
        return "Parquet Conversion Error";
    case data_writer_error::file_io_error:
        return "File IO Error";
    case data_writer_error::no_data:
        return "No data";
    }
}

} // namespace datalake
