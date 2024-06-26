_NON_PREFIXED_HEADERS = [
    "AvroParse.hh",
    "AvroSerialize.hh",
    "AvroTraits.hh",
    "Compiler.hh",
    "Config.hh",
    "CustomAttributes.hh",
    "DataFile.hh",
    "Decoder.hh",
    "Encoder.hh",
    "Exception.hh",
    "Generic.hh",
    "GenericDatum.hh",
    "Layout.hh",
    "LogicalType.hh",
    "Node.hh",
    "NodeConcepts.hh",
    "NodeImpl.hh",
    "Parser.hh",
    "Reader.hh",
    "Resolver.hh",
    "ResolverSchema.hh",
    "ResolvingReader.hh",
    "Schema.hh",
    "SchemaResolution.hh",
    "Serializer.hh",
    "Specific.hh",
    "Stream.hh",
    "Types.hh",
    "ValidSchema.hh",
    "Validator.hh",
    "Writer.hh",
    "Zigzag.hh",
]

_NON_PREFIXED_BUFFER_HEADERS = [
    "buffer/Buffer.hh",
    "buffer/BufferPrint.hh",
    "buffer/BufferReader.hh",
    "buffer/BufferStream.hh",
    "buffer/BufferStreambuf.hh",
]

_NON_PREFIXED_BUFFER_DETAIL_HEADERS = [
    "buffer/detail/BufferDetail.hh",
    "buffer/detail/BufferDetailIterator.hh",
]

_PREFIXED_HEADERS = ["lang/c++/include/avro/" + hdr for hdr in _NON_PREFIXED_HEADERS]

_PREFIXED_BUFFER_HEADERS = ["lang/c++/include/avro/" + hdr for hdr in _NON_PREFIXED_BUFFER_HEADERS]

_PREFIXED_BUFFER_DETAIL_HEADERS = ["lang/c++/include/avro/" + hdr for hdr in _NON_PREFIXED_BUFFER_DETAIL_HEADERS]

_ALL_NON_PREFIXED_HEADERS = _NON_PREFIXED_HEADERS + _NON_PREFIXED_BUFFER_HEADERS + _NON_PREFIXED_BUFFER_DETAIL_HEADERS

_ALL_PREFIXED_HEADERS = _PREFIXED_HEADERS + _PREFIXED_BUFFER_HEADERS + _PREFIXED_BUFFER_DETAIL_HEADERS

genrule(
    name = "copy_headers",
    srcs = _PREFIXED_HEADERS,
    outs = _NON_PREFIXED_HEADERS,
    cmd_bash = "cp $(SRCS) $(@D)/",
)

genrule(
    name = "copy_buffer_headers",
    srcs = _PREFIXED_BUFFER_HEADERS,
    outs = _NON_PREFIXED_BUFFER_HEADERS,
    cmd_bash = "cp $(SRCS) $(@D)/buffer/",
)

genrule(
    name = "copy_buffer_detail_headers",
    srcs = _PREFIXED_BUFFER_DETAIL_HEADERS,
    outs = _NON_PREFIXED_BUFFER_DETAIL_HEADERS,
    cmd_bash = "cp $(SRCS) $(@D)/buffer/detail/",
)

cc_library(
    name = "avro",
    srcs = [
        "lang/c++/impl/BinaryDecoder.cc",
        "lang/c++/impl/BinaryEncoder.cc",
        "lang/c++/impl/Compiler.cc",
        "lang/c++/impl/CustomAttributes.cc",
        "lang/c++/impl/DataFile.cc",
        "lang/c++/impl/FileStream.cc",
        "lang/c++/impl/Generic.cc",
        "lang/c++/impl/GenericDatum.cc",
        "lang/c++/impl/LogicalType.cc",
        "lang/c++/impl/Node.cc",
        "lang/c++/impl/NodeImpl.cc",
        "lang/c++/impl/Resolver.cc",
        "lang/c++/impl/ResolverSchema.cc",
        "lang/c++/impl/Schema.cc",
        "lang/c++/impl/Stream.cc",
        "lang/c++/impl/Types.cc",
        "lang/c++/impl/ValidSchema.cc",
        "lang/c++/impl/Validator.cc",
        "lang/c++/impl/Zigzag.cc",
        "lang/c++/impl/avrogencpp.cc",
        "lang/c++/impl/json/JsonDom.cc",
        "lang/c++/impl/json/JsonDom.hh",
        "lang/c++/impl/json/JsonIO.cc",
        "lang/c++/impl/json/JsonIO.hh",
        "lang/c++/impl/parsing/JsonCodec.cc",
        "lang/c++/impl/parsing/ResolvingDecoder.cc",
        "lang/c++/impl/parsing/Symbol.cc",
        "lang/c++/impl/parsing/Symbol.hh",
        "lang/c++/impl/parsing/ValidatingCodec.cc",
        "lang/c++/impl/parsing/ValidatingCodec.hh",
    ] + _ALL_NON_PREFIXED_HEADERS,
    hdrs = _ALL_PREFIXED_HEADERS,
    copts = [
        "-Wno-unused-but-set-variable",
    ],
    defines = ["SNAPPY_CODEC_AVAILABLE"],
    includes = [
        "lang/c++/include",
    ],
    local_defines = ["AVRO_VERSION=1"],
    visibility = [
        "//visibility:public",
    ],
    deps = [
        "@boost//:algorithm",
    ],
)
