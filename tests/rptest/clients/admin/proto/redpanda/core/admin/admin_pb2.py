"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(_runtime_version.Domain.PUBLIC, 5, 29, 0, '', 'proto/redpanda/core/admin/admin.proto')
_sym_db = _symbol_database.Default()
from .....proto.redpanda.pbgen import options_pb2 as proto_dot_redpanda_dot_pbgen_dot_options__pb2
from .....proto.redpanda.pbgen import rpc_pb2 as proto_dot_redpanda_dot_pbgen_dot_rpc__pb2
DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n%proto/redpanda/core/admin/admin.proto\x12\x13redpanda.core.admin\x1a"proto/redpanda/pbgen/options.proto\x1a\x1eproto/redpanda/pbgen/rpc.proto"\x15\n\x13GetBuildInfoRequest"/\n\tBuildInfo\x12\x0f\n\x07version\x18\x01 \x01(\t\x12\x11\n\tbuild_sha\x18\x02 \x01(\t",\n\x08RPCRoute\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\x12\n\nhttp_route\x18\x02 \x01(\t"\x16\n\x14ListRPCRoutesRequest"F\n\x15ListRPCRoutesResponse\x12-\n\x06routes\x18\x01 \x03(\x0b2\x1d.redpanda.core.admin.RPCRoute2\xe0\x01\n\x0cAdminService\x12`\n\x0cGetBuildInfo\x12(.redpanda.core.admin.GetBuildInfoRequest\x1a\x1e.redpanda.core.admin.BuildInfo"\x06\xea\x92\x19\x02\x10\x03\x12n\n\rListRPCRoutes\x12).redpanda.core.admin.ListRPCRoutesRequest\x1a*.redpanda.core.admin.ListRPCRoutesResponse"\x06\xea\x92\x19\x02\x10\x03B\x10\xea\x92\x19\x0cproto::adminb\x06proto3')
_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'proto.redpanda.core.admin.admin_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
    _globals['DESCRIPTOR']._loaded_options = None
    _globals['DESCRIPTOR']._serialized_options = b'\xea\x92\x19\x0cproto::admin'
    _globals['_ADMINSERVICE'].methods_by_name['GetBuildInfo']._loaded_options = None
    _globals['_ADMINSERVICE'].methods_by_name['GetBuildInfo']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_ADMINSERVICE'].methods_by_name['ListRPCRoutes']._loaded_options = None
    _globals['_ADMINSERVICE'].methods_by_name['ListRPCRoutes']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_GETBUILDINFOREQUEST']._serialized_start = 130
    _globals['_GETBUILDINFOREQUEST']._serialized_end = 151
    _globals['_BUILDINFO']._serialized_start = 153
    _globals['_BUILDINFO']._serialized_end = 200
    _globals['_RPCROUTE']._serialized_start = 202
    _globals['_RPCROUTE']._serialized_end = 246
    _globals['_LISTRPCROUTESREQUEST']._serialized_start = 248
    _globals['_LISTRPCROUTESREQUEST']._serialized_end = 270
    _globals['_LISTRPCROUTESRESPONSE']._serialized_start = 272
    _globals['_LISTRPCROUTESRESPONSE']._serialized_end = 342
    _globals['_ADMINSERVICE']._serialized_start = 345
    _globals['_ADMINSERVICE']._serialized_end = 569