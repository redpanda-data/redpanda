"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(_runtime_version.Domain.PUBLIC, 5, 29, 0, '', 'proto/redpanda/core/pbgen/rpc.proto')
_sym_db = _symbol_database.Default()
from google.protobuf import descriptor_pb2 as google_dot_protobuf_dot_descriptor__pb2
DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n#proto/redpanda/core/pbgen/rpc.proto\x12\x13redpanda.core.pbgen\x1a google/protobuf/descriptor.proto"S\n\nRPCOptions\x121\n\x05authz\x18\x02 \x01(\x0e2".redpanda.core.pbgen.RPCAuthZLevel\x12\x12\n\nhttp_route\x18\x03 \x01(\t*K\n\rRPCAuthZLevel\x12\x15\n\x11LEVEL_UNSPECIFIED\x10\x00\x12\n\n\x06PUBLIC\x10\x01\x12\x08\n\x04USER\x10\x02\x12\r\n\tSUPERUSER\x10\x03:N\n\x03rpc\x12\x1e.google.protobuf.MethodOptions\x18\xad\x92\x03 \x01(\x0b2\x1f.redpanda.core.pbgen.RPCOptionsB=Z;github.com/redpanda-data/redpanda/proto/redpanda/core/pbgenb\x06proto3')
_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'proto.redpanda.core.pbgen.rpc_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
    _globals['DESCRIPTOR']._loaded_options = None
    _globals['DESCRIPTOR']._serialized_options = b'Z;github.com/redpanda-data/redpanda/proto/redpanda/core/pbgen'
    _globals['_RPCAUTHZLEVEL']._serialized_start = 179
    _globals['_RPCAUTHZLEVEL']._serialized_end = 254
    _globals['_RPCOPTIONS']._serialized_start = 94
    _globals['_RPCOPTIONS']._serialized_end = 177