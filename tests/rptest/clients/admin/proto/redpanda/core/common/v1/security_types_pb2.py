"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(_runtime_version.Domain.PUBLIC, 5, 29, 0, '', 'proto/redpanda/core/common/v1/security_types.proto')
_sym_db = _symbol_database.Default()
from ......proto.redpanda.core.pbgen import options_pb2 as proto_dot_redpanda_dot_core_dot_pbgen_dot_options__pb2
DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b"\n2proto/redpanda/core/common/v1/security_types.proto\x12\x17redpanda.core.common.v1\x1a'proto/redpanda/core/pbgen/options.proto*w\n\x0eScramMechanism\x12\x1f\n\x1bSCRAM_MECHANISM_UNSPECIFIED\x10\x00\x12!\n\x1dSCRAM_MECHANISM_SCRAM_SHA_256\x10\x01\x12!\n\x1dSCRAM_MECHANISM_SCRAM_SHA_512\x10\x02B\x11\xea\x92\x19\rproto::commonb\x06proto3")
_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'proto.redpanda.core.common.v1.security_types_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
    _globals['DESCRIPTOR']._loaded_options = None
    _globals['DESCRIPTOR']._serialized_options = b'\xea\x92\x19\rproto::common'
    _globals['_SCRAMMECHANISM']._serialized_start = 120
    _globals['_SCRAMMECHANISM']._serialized_end = 239