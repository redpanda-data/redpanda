#pragma once

#pragma once
#include <arrow/api.h>
#include <google/protobuf/message.h>

#include <memory>
#include <stdexcept>

namespace datalake::detail {
class proto_to_arrow_interface {
public:
    virtual ~proto_to_arrow_interface() {}

    // Pure virtual methods
    virtual arrow::Status
    add_value(const google::protobuf::Message*, int field_idx)
      = 0;

    /// Return an Arrow field descriptor for this Array. Used for building
    /// A schema.
    virtual std::shared_ptr<arrow::Field> field(const std::string& name) = 0;

    /// Return the underlying ArrayBuilder. Used when this is a child of another
    /// Builder
    virtual std::shared_ptr<arrow::ArrayBuilder> builder() = 0;

    // Methods with defaults
    virtual arrow::Status finish_batch() { return arrow::Status::OK(); }
    std::shared_ptr<arrow::ChunkedArray> finish() {
        return std::make_shared<arrow::ChunkedArray>(_values);
    }

protected:
    arrow::Status _arrow_status;
    arrow::ArrayVector _values;
};

} // namespace datalake::detail
