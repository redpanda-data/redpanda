# Redpanda Datalake Protocol Buffers

This package contains definitions of types for datalake integration
(i.e. Iceberg) that are otherwise impossible to express with built-in
types or existing protobuf well known types.

Note: These definitions should be usable by end-users as-is! They shouldn't
contain Redpanda specific implementation details (like customization of the
C++ namespace).
