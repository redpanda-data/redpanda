"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(_runtime_version.Domain.PUBLIC, 5, 29, 0, '', 'proto/redpanda/pbgen/rpc.proto')
_sym_db = _symbol_database.Default()
from google.protobuf import descriptor_pb2 as google_dot_protobuf_dot_descriptor__pb2
DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x1eproto/redpanda/pbgen/rpc.proto\x12\x0eredpanda.pbgen\x1a google/protobuf/descriptor.proto"N\n\nRPCOptions\x12,\n\x05authz\x18\x02 \x01(\x0e2\x1d.redpanda.pbgen.RPCAuthZLevel\x12\x12\n\nhttp_route\x18\x03 \x01(\t*K\n\rRPCAuthZLevel\x12\x15\n\x11LEVEL_UNSPECIFIED\x10\x00\x12\n\n\x06PUBLIC\x10\x01\x12\x08\n\x04USER\x10\x02\x12\r\n\tSUPERUSER\x10\x03:I\n\x03rpc\x12\x1e.google.protobuf.MethodOptions\x18\xad\x92\x03 \x01(\x0b2\x1a.redpanda.pbgen.RPCOptionsB8Z6github.com/redpanda-data/redpanda/proto/redpanda/pbgenb\x06proto3')
_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'proto.redpanda.pbgen.rpc_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
    _globals['DESCRIPTOR']._loaded_options = None
    _globals['DESCRIPTOR']._serialized_options = b'Z6github.com/redpanda-data/redpanda/proto/redpanda/pbgen'
    _globals['_RPCAUTHZLEVEL']._serialized_start = 164
    _globals['_RPCAUTHZLEVEL']._serialized_end = 239
    _globals['_RPCOPTIONS']._serialized_start = 84
    _globals['_RPCOPTIONS']._serialized_end = 162