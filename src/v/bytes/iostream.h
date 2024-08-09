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
#include "bytes/iobuf.h"

#include <seastar/core/iostream.hh>

/// \brief wraps an iobuf so it can be used as an input stream data source
ss::input_stream<char> make_iobuf_input_stream(iobuf io);

/// \brief wraps the iobuf to be used as an output stream sink
ss::output_stream<char> make_iobuf_ref_output_stream(iobuf& io);

/// \brief exactly like input_stream<char>::read_exactly but returns iobuf
ss::future<iobuf> read_iobuf_exactly(ss::input_stream<char>& in, size_t n);

ss::future<> write_iobuf_to_output_stream(iobuf, ss::output_stream<char>&);
