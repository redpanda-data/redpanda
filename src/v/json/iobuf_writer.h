// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "json/chunked_buffer.h"
#include "json/writer.h"

#include <rapidjson/rapidjson.h>

namespace json {

///\brief a json::Writer that can accept an iobuf as a String payload.
template<
  typename OutputStream,
  typename SourceEncoding = json::UTF8<>,
  typename TargetEncoding = json::UTF8<>,
  unsigned writeFlags = rapidjson::kWriteDefaultFlags>
class generic_iobuf_writer
  : public Writer<OutputStream, SourceEncoding, TargetEncoding, writeFlags> {
    using Base
      = Writer<OutputStream, SourceEncoding, TargetEncoding, writeFlags>;

public:
    explicit generic_iobuf_writer(OutputStream& os)
      : Base{os} {}

    using Base::String;
    bool String(const iobuf& buf) {
        constexpr bool buffer_is_chunked
          = std::same_as<OutputStream, json::chunked_buffer>;
        if constexpr (buffer_is_chunked) {
            return write_chunked_string(buf);
        } else {
            iobuf_const_parser p{buf};
            auto str = p.read_string(p.bytes_left());
            return this->String(str.data(), str.size(), true);
        }
    }

private:
    bool write_chunked_string(const iobuf& buf) {
        const auto last_frag = [this]() {
            return std::prev(this->os_->_impl.end());
        };
        using Ch = Base::Ch;
        this->Prefix(rapidjson::kStringType);
        const auto beg = buf.begin();
        const auto end = buf.end();
        const auto last = std::prev(end);
        Ch stashed{};
        Ch* stash_pos{};
        // Base::WriteString is used to JSON encode the string, and requires a
        // contiguous range (pointer, len), so we pass it each fragment.
        //
        // Unfortunately it also encloses the encoded fragment with double
        // quotes:
        //     R"("A string made of ""fragments will need ""fixing")"
        //
        // This algorithm efficiently removes the extra quotes without
        // additional copying:
        // For each encoded fragment that is written (except the last one):
        //   1. Trim the suffix quote
        //   2. Stash the final character, and where it is to be written
        //   3. Drop the final character
        // For each encoded fragment that is written (except the first one):
        //   4. Restore the stashed character over the prefix-quote
        for (auto i = beg; i != end; ++i) {
            if (!Base::WriteString(i->get(), i->size())) {
                return false;
            }
            if (i != beg) {
                // 4. Restore the stashed character over the prefix-quote
                *stash_pos = stashed;
            }
            if (i != last) {
                // 1. Trim the suffix quote
                this->os_->_impl.trim_back(1);

                // 2. Stash the final character, ...
                auto last = last_frag();
                stashed = *std::prev(last->get_current());
                // 3. Drop the final character
                this->os_->_impl.trim_back(1);

                // Ensure a stable address to restore the stashed character
                if (last != last_frag()) {
                    this->os_->_impl.reserve_memory(1);
                }
                // 2. ...and where it is to be written.
                stash_pos = last_frag()->get_current();
            }
        }
        return this->EndValue(true);
    }
};

template<
  typename OutputStream,
  typename SourceEncoding = json::UTF8<>,
  typename TargetEncoding = json::UTF8<>,
  unsigned writeFlags = rapidjson::kWriteDefaultFlags>
using iobuf_writer = generic_iobuf_writer<
  OutputStream,
  SourceEncoding,
  TargetEncoding,
  writeFlags>;

} // namespace json
