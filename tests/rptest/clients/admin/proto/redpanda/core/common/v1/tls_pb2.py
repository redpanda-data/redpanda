"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(_runtime_version.Domain.PUBLIC, 5, 29, 0, '', 'proto/redpanda/core/common/v1/tls.proto')
_sym_db = _symbol_database.Default()
from ......proto.redpanda.core.pbgen import options_pb2 as proto_dot_redpanda_dot_core_dot_pbgen_dot_options__pb2
from ......google.api import field_behavior_pb2 as google_dot_api_dot_field__behavior__pb2
DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\'proto/redpanda/core/common/v1/tls.proto\x12\x17redpanda.core.common.v1\x1a\'proto/redpanda/core/pbgen/options.proto\x1a\x1fgoogle/api/field_behavior.proto"\xdb\x01\n\x0bTLSSettings\x12\x0f\n\x07enabled\x18\x03 \x01(\x08\x12E\n\x11tls_file_settings\x18\x01 \x01(\x0b2(.redpanda.core.common.v1.TLSFileSettingsH\x00\x12C\n\x10tls_pem_settings\x18\x02 \x01(\x0b2\'.redpanda.core.common.v1.TLSPEMSettingsH\x00\x12\x1f\n\x17do_not_set_sni_hostname\x18\x04 \x01(\x08B\x0e\n\x0ctls_settings"G\n\x0fTLSFileSettings\x12\x0f\n\x07ca_path\x18\x01 \x01(\t\x12\x10\n\x08key_path\x18\x02 \x01(\t\x12\x11\n\tcert_path\x18\x03 \x01(\t"m\n\x0eTLSPEMSettings\x12\x10\n\x02ca\x18\x01 \x01(\tB\x04\xe8\x92\x19\x01\x12\x17\n\x03key\x18\x02 \x01(\tB\n\x80\x01\x01\xe0A\x04\xe8\x92\x19\x01\x12\x1c\n\x0fkey_fingerprint\x18\x03 \x01(\tB\x03\xe0A\x03\x12\x12\n\x04cert\x18\x04 \x01(\tB\x04\xe8\x92\x19\x01B\x11\xea\x92\x19\rproto::commonb\x06proto3')
_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'proto.redpanda.core.common.v1.tls_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
    _globals['DESCRIPTOR']._loaded_options = None
    _globals['DESCRIPTOR']._serialized_options = b'\xea\x92\x19\rproto::common'
    _globals['_TLSPEMSETTINGS'].fields_by_name['ca']._loaded_options = None
    _globals['_TLSPEMSETTINGS'].fields_by_name['ca']._serialized_options = b'\xe8\x92\x19\x01'
    _globals['_TLSPEMSETTINGS'].fields_by_name['key']._loaded_options = None
    _globals['_TLSPEMSETTINGS'].fields_by_name['key']._serialized_options = b'\x80\x01\x01\xe0A\x04\xe8\x92\x19\x01'
    _globals['_TLSPEMSETTINGS'].fields_by_name['key_fingerprint']._loaded_options = None
    _globals['_TLSPEMSETTINGS'].fields_by_name['key_fingerprint']._serialized_options = b'\xe0A\x03'
    _globals['_TLSPEMSETTINGS'].fields_by_name['cert']._loaded_options = None
    _globals['_TLSPEMSETTINGS'].fields_by_name['cert']._serialized_options = b'\xe8\x92\x19\x01'
    _globals['_TLSSETTINGS']._serialized_start = 143
    _globals['_TLSSETTINGS']._serialized_end = 362
    _globals['_TLSFILESETTINGS']._serialized_start = 364
    _globals['_TLSFILESETTINGS']._serialized_end = 435
    _globals['_TLSPEMSETTINGS']._serialized_start = 437
    _globals['_TLSPEMSETTINGS']._serialized_end = 546