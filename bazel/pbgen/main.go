package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"

	pbgen "github.com/redpanda-data/redpanda/proto/redpanda/core/pbgen/options"
	rpcgen "github.com/redpanda-data/redpanda/proto/redpanda/core/pbgen/rpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/pluginpb"
)

func main() {
	if err := Run(); err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", filepath.Base(os.Args[0]), err)
		os.Exit(1)
	}
}

func Run() error {
	if len(os.Args) > 1 {
		return fmt.Errorf("unknown argument %q (this program should be run by protoc, not directly)", os.Args[1])
	}
	in, err := io.ReadAll(os.Stdin)
	if err != nil {
		return err
	}
	req := &pluginpb.CodeGeneratorRequest{}
	if err := proto.Unmarshal(in, req); err != nil {
		return err
	}
	files := &protoregistry.Files{}
	var toGenerate []protoreflect.FileDescriptor
	for _, f := range req.ProtoFile {
		desc, err := protodesc.NewFile(f, files)
		if err != nil {
			return err
		}
		if err := files.RegisterFile(desc); err != nil {
			return err
		}
		if slices.Contains(req.FileToGenerate, f.GetName()) {
			toGenerate = append(toGenerate, desc)
		}
	}
	resp := &pluginpb.CodeGeneratorResponse{}
	resp.SupportedFeatures = proto.Uint64(uint64(pluginpb.CodeGeneratorResponse_FEATURE_PROTO3_OPTIONAL))
	var errs []error
	for _, f := range toGenerate {
		var w codewriter
		{
			headerGenerator := &headerGenerator{
				baseGenerator: baseGenerator{
					emitError: func(err error) {
						errs = append(errs, err)
					},
					file: f,
				},
			}
			headerGenerator.generateFile(&w)
			headerFilepath := filepath.Base(strings.ReplaceAll(f.Path(), ".proto", ".proto.h"))
			headerContents := w.String()
			resp.File = append(resp.File, &pluginpb.CodeGeneratorResponse_File{
				Name:    proto.String(headerFilepath),
				Content: proto.String(headerContents),
			})
		}
		w.Reset()
		{
			implGenerator := &implGenerator{
				baseGenerator: baseGenerator{
					emitError: func(err error) {
						errs = append(errs, err)
					},
					file: f,
				},
			}
			implGenerator.generateFile(&w)
			implFilepath := filepath.Base(strings.ReplaceAll(f.Path(), ".proto", ".proto.cc"))
			implContents := w.String()
			resp.File = append(resp.File, &pluginpb.CodeGeneratorResponse_File{
				Name:    proto.String(implFilepath),
				Content: proto.String(implContents),
			})
		}
	}
	if err := errors.Join(errs...); err != nil {
		resp.Error = proto.String(err.Error())
	}
	out, err := proto.Marshal(resp)
	if err != nil {
		return err
	}
	if _, err := os.Stdout.Write(out); err != nil {
		return err
	}
	return nil
}

// ----------------------------------------------------------

type codewriter struct {
	prelude strings.Builder
	content strings.Builder
	indent  string
}

func (b *codewriter) Indent() {
	b.indent += "  "
}
func (b *codewriter) Dedent() {
	b.indent = b.indent[:len(b.indent)-2]
}

func (b *codewriter) Printf(msg string, args ...any) {
	_, _ = b.content.WriteString(b.indent + fmt.Sprintf(msg, args...))
}

func (b *codewriter) Println(args ...any) {
	if len(args) == 0 {
		// Don't add indentation for blank lines.
		b.content.WriteString("\n")
	} else {
		_, _ = b.content.WriteString(b.indent + fmt.Sprintln(args...))
	}
}

func (b *codewriter) PreludePrintf(msg string, args ...any) {
	_, _ = b.prelude.WriteString(fmt.Sprintf(msg, args...))
}

func (b *codewriter) PreludePrintln(args ...any) {
	_, _ = b.prelude.WriteString(fmt.Sprintln(args...))
}

func (b *codewriter) String() string {
	return b.prelude.String() + b.content.String()
}

func (b *codewriter) Reset() {
	b.prelude.Reset()
	b.content.Reset()
}

// ----------------------------------------------------------

func isDebugRedacted(f protoreflect.FieldDescriptor) bool {
	opts := f.Options().(*descriptorpb.FieldOptions)
	return opts.GetDebugRedact()
}

func isPtr(f protoreflect.FieldDescriptor) bool {
	opts := f.Options().(*descriptorpb.FieldOptions)
	if opts == nil {
		return false
	}
	return proto.GetExtension(opts, pbgen.E_Ptr).(bool)
}

func isIOBuf(f protoreflect.FieldDescriptor) bool {
	opts := f.Options().(*descriptorpb.FieldOptions)
	if opts == nil {
		return false
	}
	return proto.GetExtension(opts, pbgen.E_Iobuf).(bool)
}

func customNamespace(f protoreflect.FileDescriptor) (ns string, ok bool) {
	opts := f.Options().(*descriptorpb.FileOptions)
	if !proto.HasExtension(opts, pbgen.E_CppNamespace) {
		return "", false
	}
	str := proto.GetExtension(opts, pbgen.E_CppNamespace).(string)
	return str, true
}

type rpcAuthZLevel string

const (
	rpcAuthZLevelMissing   rpcAuthZLevel = "!!MISSING!!"
	rpcAuthZLevelPublic    rpcAuthZLevel = "unauthenticated"
	rpcAuthZLevelUser      rpcAuthZLevel = "user"
	rpcAuthZLevelSuperuser rpcAuthZLevel = "superuser"
)

func rpcAuthzLevel(f protoreflect.MethodDescriptor) rpcAuthZLevel {
	opts := f.Options().(*descriptorpb.MethodOptions)
	if !proto.HasExtension(opts, rpcgen.E_Rpc) {
		return rpcAuthZLevelMissing
	}
	rpcOpts := proto.GetExtension(opts, rpcgen.E_Rpc).(*rpcgen.RPCOptions)
	switch rpcOpts.Authz {
	case rpcgen.RPCAuthZLevel_PUBLIC:
		return rpcAuthZLevelPublic
	case rpcgen.RPCAuthZLevel_USER:
		return rpcAuthZLevelUser
	case rpcgen.RPCAuthZLevel_SUPERUSER:
		return rpcAuthZLevelSuperuser
	default:
		return rpcAuthZLevelMissing
	}
}

func rpcAlternativeRoute(f protoreflect.MethodDescriptor) string {
	opts := f.Options().(*descriptorpb.MethodOptions)
	if !proto.HasExtension(opts, rpcgen.E_Rpc) {
		return ""
	}
	rpcOpts := proto.GetExtension(opts, rpcgen.E_Rpc).(*rpcgen.RPCOptions)
	return rpcOpts.HttpRoute
}

// ----------------------------------------------------------

type baseGenerator struct {
	needsChunkedHashMap bool
	needsChunkedVector  bool
	needsRpcs           bool

	emitError func(error)
	file      protoreflect.FileDescriptor
}

func (g *baseGenerator) translateType(f protoreflect.FieldDescriptor) (typ string, isTriviallyCopyable bool) {
	if (f.Kind() != protoreflect.MessageKind || f.IsMap()) && isPtr(f) {
		g.emitError(fmt.Errorf("rp.core.pbgen.ptr is not supported for %s", f.FullName()))
	}
	if f.IsMap() {
		g.needsChunkedHashMap = true
		key, _ := g.translateType(f.MapKey())
		value, _ := g.translateType(f.MapValue())
		return fmt.Sprintf("chunked_hash_map<%s, %s>", key, value), false
	}
	typ = g.translateBaseType(f)
	if isPtr(f) {
		typ = fmt.Sprintf("std::unique_ptr<%s>", typ)
	}
	isTriviallyCopyable = !slices.Contains([]protoreflect.Kind{protoreflect.BytesKind, protoreflect.MessageKind, protoreflect.StringKind}, f.Kind())
	if f.IsList() {
		g.needsChunkedVector = true
		typ = "chunked_vector<" + typ + ">"
		isTriviallyCopyable = false
	}
	return
}

func (g *baseGenerator) translateBaseType(f protoreflect.FieldDescriptor) (typ string) {
	switch f.Kind() {
	case protoreflect.BoolKind:
		return "bool"
	case protoreflect.BytesKind:
		return "iobuf"
	case protoreflect.DoubleKind:
		return "double"
	case protoreflect.Fixed32Kind, protoreflect.Uint32Kind:
		return "uint32_t"
	case protoreflect.Int32Kind, protoreflect.Sfixed32Kind, protoreflect.Sint32Kind:
		return "int32_t"
	case protoreflect.Fixed64Kind, protoreflect.Uint64Kind:
		return "uint64_t"
	case protoreflect.Int64Kind, protoreflect.Sfixed64Kind, protoreflect.Sint64Kind:
		return "int64_t"
	case protoreflect.FloatKind:
		return "float"
	case protoreflect.GroupKind:
		g.emitError(fmt.Errorf("groups are not supported: %s", f.FullName()))
		return "GROUPS_NOT_SUPPORTED"
	case protoreflect.EnumKind:
		return g.cppTypeName(f.Enum())
	case protoreflect.MessageKind:
		return g.cppTypeName(f.Message())
	case protoreflect.StringKind:
		if isIOBuf(f) {
			return "iobuf"
		}
		return "ss::sstring"
	default:
		panic(fmt.Sprintf("unexpected protoreflect.Kind: %#v", f.Kind()))
	}
}

func (g *baseGenerator) cppTypeName(d protoreflect.Descriptor) string {
	switch d.FullName() {
	case "google.protobuf.Duration":
		return "absl::Duration"
	case "google.protobuf.Timestamp":
		return "absl::Time"
	case "google.protobuf.FieldMask":
		return "serde::pb::field_mask"
	default:
		if isWellKnownType(d) {
			log.Fatalf("well-known types need an entry above: %s\n", d.FullName())
		}
	}
	pkg := d.ParentFile().Package()
	name := d.FullName()
	path := strings.TrimPrefix(string(name), string(pkg))
	path = strings.TrimPrefix(path, ".")
	typeName := pascalToSnakeCase(strings.ReplaceAll(path, ".", "_"))
	if d.ParentFile().Package() == g.file.FullName() {
		return typeName
	}
	ns := nameToCppNamespace(d.ParentFile())
	return "::" + ns + "::" + typeName
}

func (g *baseGenerator) enumMemberName(val protoreflect.EnumValueDescriptor) string {
	fullName := strings.ToLower(string(val.Name()))
	strippedName := strings.TrimPrefix(fullName, g.cppTypeName(val.Parent())+"_")
	return strippedName
}

// ----------------------------------------------------------

type headerGenerator struct {
	baseGenerator
	needsVariant bool
}

func (g *headerGenerator) source(msg protoreflect.Descriptor) protoreflect.SourceLocation {
	return g.file.SourceLocations().ByDescriptor(msg)
}

func (g *headerGenerator) leadingComments(msg protoreflect.Descriptor, w *codewriter) {
	src := g.source(msg)
	// Remove * and spaces to handle comments like: /** */
	comments := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(src.LeadingComments), "*"))
	if comments == "" {
		return
	}
	for comment := range strings.SplitSeq(comments, "\n") {
		w.Printf("// %s\n", strings.TrimSpace(comment))
	}
}

func (g *headerGenerator) generateFile(w *codewriter) {
	imports := g.file.Imports()
	defer func() {
		w.PreludePrintln("// Code generated by bazel/pbgen. DO NOT EDIT.")
		w.PreludePrintln("// clang-format off")
		w.PreludePrintln()
		w.PreludePrintln(`#pragma once`)
		w.PreludePrintln()
		w.PreludePrintln(`#include "base/format_to.h"`)
		w.PreludePrintln(`#include "bytes/iobuf.h"`)
		w.PreludePrintln(`#include "serde/protobuf/base.h"`)
		w.PreludePrintln(`#include "strings/static_str.h"`)
		if g.needsChunkedHashMap {
			w.PreludePrintln(`#include "container/chunked_hash_map.h"`)
		}
		if g.needsChunkedVector {
			w.PreludePrintln(`#include "container/chunked_vector.h"`)
		}
		if g.needsRpcs {
			w.PreludePrintln(`#include "serde/protobuf/rpc.h"`)
		}
		includes := []string{}
		for i := range imports.Len() {
			f := imports.Get(i)
			if path := mapImport(f.Path()); path != "" {
				includes = append(includes, path)
			}
		}
		slices.Sort(includes)
		includes = slices.Compact(includes)
		for _, i := range includes {
			w.PreludePrintf("#include %q\n", i)
		}
		w.PreludePrintln()
		w.PreludePrintln("#include <seastar/core/future.hh>")
		w.PreludePrintln("#include <seastar/core/sstring.hh>")
		if g.needsVariant {
			w.PreludePrintln("#include <seastar/util/variant_utils.hh>")
		}
		w.PreludePrintln("#include <span>")
		w.PreludePrintln()
		w.PreludePrintln("class iobuf_parser;")
		w.PreludePrintln()
		w.PreludePrintln("namespace serde::pb {")
		w.PreludePrintln("class wire_format_parser;")
		w.PreludePrintln("}")
		w.PreludePrintln("namespace serde::pb::json {")
		w.PreludePrintln("class peekable_parser;")
		w.PreludePrintln("}")
		w.PreludePrintln()
	}()
	namespace := nameToCppNamespace(g.file)
	w.Printf("namespace %s {\n\n", namespace)
	defer w.Printf("} // %s\n", namespace)
	msgs, enums := collectDescriptors(g.file)
	// Forward declare all messages.
	for _, d := range msgs {
		w.Printf("class %s;\n", g.cppTypeName(d))
	}
	w.Println()
	// Emit enums first, since we don't forward declare them.
	for _, enum := range enums {
		if enum.Values().ByNumber(0) == nil {
			g.emitError(fmt.Errorf("enum %s does not have a zero value, see https://protobuf.dev/best-practices/dos-donts/#unspecified-enum for why it should.", enum.FullName()))
		}
		g.generateEnum(enum, w)
		g.generateEnumSerde(enum, w)
		w.Println()
	}
	// Now emit all messages, but do it in a best effort order to avoid
	// circular dependencies. If you are still seeing circular dependencies,
	// this sort is stable, so you can reorder messages to get the order you want.
	for _, msg := range sortMessages(msgs) {
		g.generateMessage(msg, w)
		w.Println()
	}
	// Last emit services
	for i := range g.file.Services().Len() {
		service := g.file.Services().Get(i)
		g.generateService(service, w)
		g.generateClient(service, w)
	}
}

func (g *headerGenerator) generateService(service protoreflect.ServiceDescriptor, w *codewriter) {
	g.needsRpcs = true
	g.leadingComments(service, w)
	cppName := g.cppTypeName(service)
	w.Printf("class %s : public ::serde::pb::rpc::base_service {\n", cppName)
	defer w.Println("};")
	w.Println("public:")
	w.Indent()
	defer w.Dedent()
	w.Printf("%s() = default;\n", cppName)
	w.Printf("%s& operator=(const %s&) noexcept = delete;\n", cppName, cppName)
	w.Printf("%s(const %s&) noexcept = delete;\n", cppName, cppName)
	w.Printf("%s& operator=(%s&&) noexcept = delete;\n", cppName, cppName)
	w.Printf("%s(%s&&) noexcept = delete;\n", cppName, cppName)
	w.Printf("virtual ~%s() noexcept = default;\n", cppName)
	w.Println()
	w.Println("// Return the name of this RPC service")
	w.Printf("std::string_view name() const override { return %q; }\n", service.FullName())
	w.Println("// Call this to get all the routes defined for this service.")
	w.Println("//")
	w.Println("// NOTE: The service must outlive anything returned from this method.")
	w.Println("std::vector<serde::pb::rpc::route_descriptor> all_routes() override;")
	w.Println()
	for i := range service.Methods().Len() {
		method := service.Methods().Get(i)
		g.leadingComments(method, w)
		w.Printf(
			"virtual seastar::future<%s> %s(serde::pb::rpc::context, %s) = 0;\n",
			g.cppTypeName(method.Output()),
			pascalToSnakeCase(string(method.Name())),
			g.cppTypeName(method.Input()),
		)
	}
	w.Dedent()
	w.Println()
	w.Println("private:")
	w.Indent()
	for i := range service.Methods().Len() {
		method := service.Methods().Get(i)
		w.Printf(
			"seastar::future<iobuf> %s_handler_impl(serde::pb::rpc::context, iobuf);\n",
			pascalToSnakeCase(string(method.Name())),
		)
	}
}

func (g *headerGenerator) generateClient(service protoreflect.ServiceDescriptor, w *codewriter) {
	g.needsRpcs = true
	g.leadingComments(service, w)
	cppName := g.cppTypeName(service)
	w.Printf("class %s_client {\n", cppName)
	defer w.Println("};")
	w.Println("public:")
	w.Indent()
	defer w.Dedent()
	w.Println("// This defines the transport layer for the RPC client.")
	w.Println("//")
	w.Println("// This function should take a context with the RPC metadata and the iobuf containing the request.")
	w.Println("// It should only throw subclasses of serde::pb::rpc::base_exception.")
	w.Println("// NOTE: This RPC client only uses protobuf serialization at the moment.")
	w.Println("using send_rpc_fn_t = std::function<seastar::future<iobuf>(serde::pb::rpc::context, iobuf)>;")
	w.Println()
	w.Printf("%s_client(send_rpc_fn_t fn) : send_rpc_fn_(std::move(fn)) {}\n", cppName)
	w.Printf("%s_client& operator=(const %s_client&) noexcept = delete;\n", cppName, cppName)
	w.Printf("%s_client(const %s_client&) noexcept = delete;\n", cppName, cppName)
	w.Printf("%s_client& operator=(%s_client&&) noexcept = default;\n", cppName, cppName)
	w.Printf("%s_client(%s_client&&) noexcept = default;\n", cppName, cppName)
	w.Printf("virtual ~%s_client() noexcept = default;\n", cppName)
	w.Println()
	w.Println("// Return the name of this RPC service")
	w.Printf("std::string_view name() const { return %q; }\n", service.FullName())
	w.Println()
	for i := range service.Methods().Len() {
		method := service.Methods().Get(i)
		g.leadingComments(method, w)
		w.Printf(
			"seastar::future<%s> %s(serde::pb::rpc::context, %s);\n",
			g.cppTypeName(method.Output()),
			pascalToSnakeCase(string(method.Name())),
			g.cppTypeName(method.Input()),
		)
	}
	w.Dedent()
	w.Println()
	w.Println("private:")
	w.Indent()
	w.Println("send_rpc_fn_t send_rpc_fn_;")
}

func (g *headerGenerator) generateEnumSerde(msg protoreflect.EnumDescriptor, w *codewriter) {
	cppName := g.cppTypeName(msg)
	w.Printf("void enum_to_proto(const %s&, iobuf*);\n", cppName)
	w.Printf("void enum_from_proto(iobuf_parser*, %s*);\n", cppName)
	w.Println("// Returns the name of the enum value")
	w.Printf("static_str enum_to_string(const %s&);\n", cppName)
	w.Printf("void enum_from_json(serde::pb::json::peekable_parser*, %s*);\n", cppName)
	// TODO: When we've upgraded to libfmt 11 we can change this to return std::string_view
	w.Printf("int32_t format_as(%s);\n", cppName)
}

func (g *headerGenerator) generateEnum(enum protoreflect.EnumDescriptor, w *codewriter) {
	g.leadingComments(enum, w)
	var smallest protoreflect.EnumNumber
	var largest protoreflect.EnumNumber
	for i := range enum.Values().Len() {
		val := enum.Values().Get(i)
		smallest = min(val.Number(), smallest)
		largest = max(val.Number(), largest)
	}
	var width int
	if largest < math.MaxInt8 && smallest > math.MinInt8 {
		width = 8
	} else if largest < math.MaxInt16 && smallest > math.MinInt16 {
		width = 16
	} else {
		width = 32
	}
	sign := "uint"
	if smallest < 0 {
		sign = "int"
	}
	w.Printf("enum class %s : %s%d_t {\n", g.cppTypeName(enum), sign, width)
	defer w.Println("};")
	w.Indent()
	defer w.Dedent()
	for i := range enum.Values().Len() {
		val := enum.Values().Get(i)
		w.Printf("%s = %d,\n", g.enumMemberName(val), val.Number())
	}
}

func (g *headerGenerator) generateMessage(msg protoreflect.MessageDescriptor, w *codewriter) {
	g.leadingComments(msg, w)
	typeName := g.cppTypeName(msg)
	w.Printf("class %s : public serde::pb::base_message {\n", typeName)
	defer w.Println("};")
	w.Println("public:")
	w.Indent()
	defer w.Dedent()
	w.Printf("%s() noexcept;\n", typeName)
	w.Printf("%s(const %s&) = delete;\n", typeName, typeName)
	w.Printf("%s& operator=(const %s&) = delete;\n", typeName, typeName)
	w.Printf("%s(%s&&) noexcept;\n", typeName, typeName)
	w.Printf("%s& operator=(%s&&) noexcept;\n", typeName, typeName)
	w.Printf("~%s() noexcept;\n", typeName)
	w.Println()
	w.Printf("bool operator==(const %s&) const;\n", typeName)
	w.Println("fmt::iterator format_to(fmt::iterator) const;")
	w.Println()
	w.Printf("// Serializes %s into a protocol buffer, in a way that will not cause stalls for large messages.\n", msg.FullName())
	w.Println("seastar::future<iobuf> to_proto() const;")
	w.Printf("// Serializes %s into proto3 JSON, in a way that will not cause stalls for large messages.\n", msg.FullName())
	w.Println("seastar::future<iobuf> to_json() const;")
	w.Printf("// Deserializes %s from a protocol buffer, in a way that will not cause stalls for large messages.\n", msg.FullName())
	w.Printf("static seastar::future<%s> from_proto(iobuf);\n", typeName)
	w.Println("// Note: This factory function should not be used directly, it's exposed for other protobuf parsers to use.")
	w.Println("// Use the iobuf version instead.")
	w.Printf("static seastar::future<> from_proto(serde::pb::wire_format_parser*, %s*);\n", typeName)
	w.Printf("// Deserializes %s from json, in a way that will not cause stalls for large messages.\n", msg.FullName())
	w.Printf("static seastar::future<%s> from_json(iobuf);\n", typeName)
	w.Println("// Note: This factory function should not be used directly, it's exposed for other protobuf parsers to use.")
	w.Println("// Use the iobuf version instead.")
	w.Printf("static seastar::future<> from_json(serde::pb::json::peekable_parser*, %s*);\n", typeName)
	w.Println()
	oneofs := map[protoreflect.Name]bool{}
	for i := range msg.Fields().Len() {
		field := msg.Fields().Get(i)
		typ, trivial := g.translateType(field)
		if oneof := field.ContainingOneof(); oneof != nil {
			g.needsVariant = true
			if !oneofs[oneof.Name()] {
				oneofs[oneof.Name()] = true
				g.leadingComments(oneof, w)
				if !oneof.IsSynthetic() {
					// Deducing this for the win!
					w.Println("template<typename Self, typename... Args>")
					w.Printf("auto visit_%s(this Self&& self, Args&&... args) { return seastar::visit(std::forward<Self>(self).%s_, std::forward<Args>(args)...); }\n", oneof.Name(), oneof.Name())
					w.Printf("bool has_%s() const;\n", oneof.Name())
					w.Printf("void clear_%s();\n", oneof.Name())
				} else {
					w.Printf("void clear_%s();\n", field.Name())
				}
			}
			g.leadingComments(field, w)
			w.Printf("bool has_%s() const;\n", field.Name())
			if trivial {
				w.Printf("%s get_%s() const;\n", typ, field.Name())
				w.Printf("void set_%s(%s v);\n", field.Name(), typ)
			} else {
				w.Printf("%s& get_%s();\n", typ, field.Name())
				w.Printf("const %s& get_%s() const;\n", typ, field.Name())
				w.Printf("void set_%s(%s&& v);\n", field.Name(), typ)
			}
		} else {
			g.leadingComments(field, w)
			if trivial {
				w.Printf("%s get_%s() const;\n", typ, field.Name())
				w.Printf("void set_%s(%s v);\n", field.Name(), typ)
			} else {
				w.Printf("%s& get_%s();\n", typ, field.Name())
				w.Printf("const %s& get_%s() const;\n", typ, field.Name())
				w.Printf("void set_%s(%s&& v);\n", field.Name(), typ)
			}
		}
	}
	w.Println()
	w.Printf("std::string_view full_name() const override { return %q; }\n", msg.FullName())
	w.Printf("static constexpr size_t field_count = %d;\n", msg.Fields().Len())
	w.Println("// Convert a field path into a path of field numbers.")
	w.Println("static bool convert_field_path_to_numbers(std::span<std::string_view> field_path, std::vector<int32_t>* out);")
	w.Println("// Convert a field path into a path of field numbers.")
	w.Println("std::optional<std::vector<int32_t>> convert_field_path_to_numbers(std::span<std::string_view> field_path) const override;")
	w.Println("// Look up a field based on the field numbers.")
	w.Println("std::optional<serde::pb::field> lookup_field(std::span<const int32_t> field_numbers) override;")
	w.Println()
	w.Println("// NOTE: This is intended to be used by field_mask only. Do not use directly.")
	w.Println("static bool is_valid_field_path(std::span<const ss::sstring> path);")
	w.Println("// NOTE: This is intended to be used by field_mask only. Do not use directly.")
	w.Printf("void apply_field_path_from(std::span<const ss::sstring> path, %s* update);\n", typeName)
	w.Println()
	w.Dedent()
	w.Println("private:")
	w.Indent()
	clear(oneofs)
	for i := range msg.Fields().Len() {
		field := msg.Fields().Get(i)
		if oneof := field.ContainingOneof(); oneof != nil {
			if oneofs[oneof.Name()] {
				continue
			}
			oneofs[oneof.Name()] = true
			types := make([]string, 0, oneof.Fields().Len())
			for i := range oneof.Fields().Len() {
				subField := oneof.Fields().Get(i)
				subTyp, _ := g.translateType(subField)
				types = append(types, subTyp)
			}
			w.Printf(
				"std::variant<std::monostate, %s> %s_;\n",
				strings.Join(types, ", "),
				oneof.Name(),
			)
		} else {
			typ, isTrivial := g.translateType(field)
			if isTrivial {
				w.Printf("%s %s_{};\n", typ, field.Name())
			} else {
				w.Printf("%s %s_;\n", typ, field.Name())
			}
		}
	}
}

// ----------------------------------------------------------

type implGenerator struct {
	baseGenerator
}

func (g *implGenerator) generateFile(w *codewriter) {
	defer func() {
		w.PreludePrintln("// Code generated by bazel/pbgen. DO NOT EDIT.")
		w.PreludePrintln("// clang-format off")
		w.PreludePrintln("// NOLINTBEGIN(*-avoid-magic-numbers)")
		w.Println("// NOLINTEND(*-avoid-magic-numbers)")
		w.PreludePrintln()
		headerPath := strings.ReplaceAll(g.file.Path(), ".proto", ".proto.h")
		w.PreludePrintf("#include %q\n", headerPath)
		w.PreludePrintln()
		w.PreludePrintln(`#include "bytes/iobuf_parser.h"`)
		w.PreludePrintln(`#include "serde/protobuf/wire_format.h"`)
		w.PreludePrintln(`#include "serde/protobuf/support.h"`)
		w.PreludePrintln(`#include "serde/protobuf/json.h"`)
		w.PreludePrintln(`#include "serde/json/writer.h"`)
		w.PreludePrintln(`#include "serde/json/parser.h"`)
		w.PreludePrintln(`#include "utils/to_string.h"`)
		if g.needsRpcs {
			w.PreludePrintln(`#include "bytes/iostream.h"`)
			w.PreludePrintln(`#include "base/units.h"`)
			w.PreludePrintln(`#include <seastar/http/request.hh>`)
			w.PreludePrintln(`#include <seastar/http/reply.hh>`)
		}
		w.PreludePrintln()
		w.PreludePrintln("#include <algorithm>")
		w.PreludePrintln("#include <fmt/format.h>")
		w.PreludePrintln("#include <seastar/coroutine/maybe_yield.hh>")
		w.PreludePrintln()
	}()
	namespace := nameToCppNamespace(g.file)
	w.Printf("namespace %s {\n\n", namespace)
	defer w.Printf("} // %s\n", namespace)
	msgs, enums := collectDescriptors(g.file)
	for _, msg := range msgs {
		g.generateMessage(msg, w)
		g.generateMessageRead(msg, w)
		g.generateMessageReadHelper(msg, w)
		g.generateMessageWrite(msg, w)
		g.generateMessageWriteJson(msg, w)
		g.generateMessageReadJson(msg, w)
		g.generateMessageReadJsonHelper(msg, w)
		g.generateMessageFieldMaskIsValidHelper(msg, w)
		g.generateMessageFieldMaskApplyHelper(msg, w)
		g.generateMessagePathToNumbersHelper(msg, w)
		g.generateMessageTraversalHelper(msg, w)
		w.Println()
	}
	for _, enum := range enums {
		g.generateEnumRead(enum, w)
		g.generateEnumWrite(enum, w)
		g.generateEnumToString(enum, w)
		g.generateEnumFromJson(enum, w)
		w.Printf("int32_t format_as(%s e) { return std::to_underlying(e); }\n", g.cppTypeName(enum))
		w.Println()
	}
	for i := range g.file.Services().Len() {
		service := g.file.Services().Get(i)
		g.generateServiceRoutes(service, w)
		g.generateServiceHandlers(service, w)
		g.generateServiceClient(service, w)
		w.Println()
	}
}

func (g *implGenerator) generateMessagePathToNumbersHelper(msg protoreflect.MessageDescriptor, w *codewriter) {
	w.Printf("std::optional<std::vector<int32_t>> %s::convert_field_path_to_numbers(std::span<std::string_view> field_path) const {\n", g.cppTypeName(msg))
	w.Indent()
	w.Println("std::vector<int32_t> numbers;")
	w.Println("if (convert_field_path_to_numbers(field_path, &numbers)) { return numbers; }")
	w.Println("return std::nullopt;")
	w.Dedent()
	w.Println("}")
	w.Printf("bool %s::convert_field_path_to_numbers(std::span<std::string_view> field_path, std::vector<int32_t>* out) {\n", g.cppTypeName(msg))
	defer w.Println("}")
	w.Indent()
	defer w.Dedent()
	if msg.Fields().Len() == 0 {
		w.Println("std::ignore = out;")
		w.Println("return field_path.empty();")
		return
	}
	w.Println("if (field_path.empty()) {")
	w.Indent()
	w.Println("return true;")
	w.Dedent()
	w.Println("}")
	w.Printf("constexpr static auto key_to_field_number = std::to_array<std::pair<std::string_view, bool(*)(decltype(field_path), decltype(out))>>({\n")
	w.Indent()
	pairs := make([]string, 0, msg.Fields().Len()*2)
	for i := range msg.Fields().Len() {
		f := msg.Fields().Get(i)
		msg := f.Message()
		var lambda string
		if f.Cardinality() == protoreflect.Repeated || msg == nil || isWellKnownType(msg) {
			lambda = fmt.Sprintf("[](auto path, auto* out) { out->push_back(%d); return path.empty(); }", f.Number())
		} else {
			lambda = strings.Join([]string{
				"[](auto path, auto* out) {",
				fmt.Sprintf("out->push_back(%d);", f.Number()),
				fmt.Sprintf("return %s::convert_field_path_to_numbers(path, out);", g.cppTypeName(msg)),
				"}",
			}, " ")
		}
		pairs = append(pairs, fmt.Sprintf("{%q, %s},", f.Name(), lambda))
		if string(f.Name()) != f.JSONName() {
			pairs = append(pairs, fmt.Sprintf("{%q, %s},", f.JSONName(), lambda))
		}
	}
	// Sort the pairs for binary search.
	slices.Sort(pairs)
	for _, pair := range pairs {
		w.Println(pair)
	}
	w.Dedent()
	w.Println("});")
	w.Println("auto fields = std::ranges::equal_range(key_to_field_number, field_path.front(), std::less<>(), [](const auto& pair) { return pair.first; });")
	w.Println("if (fields.empty()) {")
	w.Indent()
	w.Println("return false;")
	w.Dedent()
	w.Println("}")
	w.Println("return fields.front().second(field_path.subspan(1), out);")
}

func (g *implGenerator) generateMessageTraversalHelper(msg protoreflect.MessageDescriptor, w *codewriter) {
	w.Printf("std::optional<serde::pb::field> %s::lookup_field(std::span<const int32_t> field_numbers) {\n", g.cppTypeName(msg))
	defer w.Println("}")
	w.Indent()
	defer w.Dedent()
	w.Println()
	w.Println("if (field_numbers.empty()) {")
	w.Indent()
	w.Println("return serde::pb::field{.value = static_cast<serde::pb::base_message*>(this)};")
	w.Dedent()
	w.Println("}")
	w.Println("serde::pb::field found;")
	w.Println("switch (field_numbers.front()) {")
	for i := range msg.Fields().Len() {
		f := msg.Fields().Get(i)
		func() {
			w.Printf("case %v: { // %s\n", f.Number(), f.Name())
			defer w.Println("}")
			w.Indent()
			defer w.Dedent()
			defer w.Println("break;")
			if f.IsMap() || f.IsList() {
				base := "repeated_value"
				if f.IsMap() {
					base = "map_value"
				}
				w.Printf("struct %s_field_value : public serde::pb::field::%s {\n", f.Name(), base)
				w.Indent()
				typ, _ := g.translateType(f)
				w.Printf("%s* value;\n", typ)
				w.Dedent()
				w.Println("};")
				w.Printf("auto value = std::make_unique<%s_field_value>();\n", f.Name())
				w.Printf("value->value = &get_%s();\n", f.Name())
				w.Println("found.value = std::move(value);")
				return
			}
			if oneof := f.ContainingOneof(); oneof != nil {
				w.Printf("if (!has_%s()) {\n", f.Name())
				w.Indent()
				w.Printf("set_%s({});\n", f.Name())
				w.Dedent()
				w.Println("}")
			}
			if msg := f.Message(); msg != nil && !isWellKnownType(msg) {
				if isPtr(f) {
					w.Printf("if (!get_%s()) { set_%s(std::make_unique<%s>()); }\n", f.Name(), f.Name(), g.cppTypeName(msg))
					w.Printf("found.value = get_%s().get();\n", f.Name())
				} else {
					w.Printf("found.value = &get_%s();\n", f.Name())
				}
			} else if enum := f.Enum(); enum != nil {
				w.Println("found.value = serde::pb::raw_enum_value{")
				w.Indent()
				w.Printf(".number = static_cast<int32_t>(get_%s()),\n", f.Name())
				w.Printf(".name = enum_to_string(get_%s()),\n", f.Name())
				w.Dedent()
				w.Println("};")
			} else if f.Kind() == protoreflect.BytesKind || isIOBuf(f) {
				w.Printf("found.value = get_%s().share();\n", f.Name())
			} else {
				w.Printf("found.value = get_%s();\n", f.Name())
			}
		}()
	}
	w.Println("default:")
	w.Indent()
	w.Println("return std::nullopt;")
	w.Dedent()
	w.Println("}")
	w.Println("if (field_numbers.size() > 1) {")
	w.Indent()
	w.Println("if (!std::holds_alternative<serde::pb::base_message*>(found.value)) { return std::nullopt; }")
	w.Println("return std::get<serde::pb::base_message*>(found.value)->lookup_field(field_numbers.subspan(1));")
	w.Dedent()
	w.Println("}")
	w.Println("return found;")
}

func (g *implGenerator) generateMessageFieldMaskIsValidHelper(msg protoreflect.MessageDescriptor, w *codewriter) {
	w.Printf("bool %s::is_valid_field_path(std::span<const ss::sstring> path) {\n", g.cppTypeName(msg))
	defer w.Println("}")
	w.Indent()
	defer w.Dedent()
	if msg.Fields().Len() == 0 {
		w.Println("return path.empty();")
		return
	}
	w.Println("if (path.empty()) { return true; }")
	w.Println("constexpr auto fields = std::to_array<std::pair<std::string_view, bool(*)(decltype(path))>>({")
	w.Indent()
	for i := range msg.Fields().Len() {
		field := msg.Fields().Get(i)
		if field.Cardinality() == protoreflect.Repeated {
			// NOTE: This is the most strict and minimal support
			// There are options to support wildcards or support updating map entries,
			// but that is much more complex, so we don't support it.
			w.Printf("{%q, [](auto path) { return path.empty(); }},\n", field.Name())
		} else if msg := field.Message(); msg != nil && !isWellKnownType(msg) {
			w.Printf("{%q, %s::is_valid_field_path},\n", field.Name(), g.cppTypeName(msg))
		} else {
			w.Printf("{%q, [](auto path) { return path.empty(); }},\n", field.Name())
		}
	}
	w.Dedent()
	w.Println("});")
	w.Println("for (const auto& [name, is_valid] : fields) {")
	w.Indent()
	w.Println("if (path.front() == name) {")
	w.Indent()
	w.Println("return is_valid(path.subspan(1));")
	w.Dedent()
	w.Println("}")
	w.Dedent()
	w.Println("}")
	w.Println("return false;")
}

func (g *implGenerator) generateMessageFieldMaskApplyHelper(msg protoreflect.MessageDescriptor, w *codewriter) {
	w.Printf("void %s::apply_field_path_from(std::span<const ss::sstring> path, %s* update) {\n", g.cppTypeName(msg), g.cppTypeName(msg))
	defer w.Println("}")
	w.Indent()
	defer w.Dedent()
	w.Println("if (path.empty()) {")
	w.Indent()
	w.Println("*this = std::move(*update);")
	w.Println("return;")
	w.Dedent()
	w.Println("}")
	if msg.Fields().Len() == 0 {
		return
	}
	w.Println("constexpr auto fields = std::to_array<std::pair<std::string_view, void(*)(decltype(path), decltype(this), decltype(update))>>({")
	w.Indent()
	for i := range msg.Fields().Len() {
		field := msg.Fields().Get(i)
		if field.IsList() {
			w.Printf("{%q, [](auto, auto* self, auto* update) {\n", field.Name())
			w.Indent()
			w.Printf("std::ranges::move(update->get_%s(), std::back_inserter(self->get_%s()));\n", field.Name(), field.Name())
			w.Printf("update->get_%s().clear();\n", field.Name())
			w.Dedent()
			w.Println("}},")
		} else if field.IsMap() {
			w.Printf("{%q, [](auto, auto* self, auto* update) {\n", field.Name())
			w.Indent()
			w.Printf("for (auto& [k, v] : update->get_%s()) {\n", field.Name())
			w.Indent()
			w.Printf("self->get_%s().insert_or_assign(std::move(k), std::move(v));\n", field.Name())
			w.Dedent()
			w.Println("}")
			w.Printf("update->get_%s().clear();\n", field.Name())
			w.Dedent()
			w.Println("}},")
		} else {
			printApplyFn := func() {
				if isPtr(field) {
					w.Printf("auto* self_field = self->get_%s().get();\n", field.Name())
					w.Printf("auto* update_field = update->get_%s().get();\n", field.Name())
					w.Println("if (!self_field && !update_field) return;")
					w.Println("if (!self_field) {")
					w.Indent()
					w.Printf("self->set_%s(std::make_unique<%s>());\n", field.Name(), g.cppTypeName(field.Message()))
					w.Printf("self_field = self->get_%s().get();\n", field.Name())
					w.Dedent()
					w.Println("}")
					w.Println("if (!update_field && path.empty()) {")
					w.Indent()
					w.Printf("self->set_%s(nullptr);\n", field.Name())
					w.Println("return;")
					w.Dedent()
					w.Println("} else if (!update_field) {")
					w.Indent()
					w.Printf("update->set_%s(std::make_unique<%s>());\n", field.Name(), g.cppTypeName(field.Message()))
					w.Printf("update_field = update->get_%s().get();\n", field.Name())
					w.Dedent()
					w.Println("}")
					w.Println("self_field->apply_field_path_from(path, update_field);")
				} else if msg := field.Message(); msg != nil && !isWellKnownType(msg) {
					if field.ContainingOneof() != nil {
						w.Printf("if (!self->has_%s()) {\n", field.Name())
						w.Indent()
						w.Printf("self->set_%s({});\n", field.Name())
						w.Dedent()
						w.Println("}")
					}
					w.Printf("self->get_%s().apply_field_path_from(path, &update->get_%s());\n", field.Name(), field.Name())
				} else {
					w.Printf("self->set_%s(std::move(update->get_%s()));\n", field.Name(), field.Name())
				}
			}
			if oneof := field.ContainingOneof(); oneof != nil {
				w.Printf("{%q, []([[maybe_unused]] auto path, auto* self, auto* update) {\n", field.Name())
				w.Indent()
				w.Printf("if (update->has_%s()) {\n", field.Name())
				w.Indent()
				printApplyFn()
				w.Dedent()
				w.Println("} else {")
				w.Indent()
				if oneof.IsSynthetic() {
					w.Printf("self->clear_%s();\n", field.Name())
				} else {
					w.Printf("self->clear_%s();\n", oneof.Name())
				}
				w.Dedent()
				w.Println("}")
				w.Dedent()
				w.Println("}},")
			} else {
				w.Printf("{%q, []([[maybe_unused]] auto path, auto* self, auto* update) {\n", field.Name())
				w.Indent()
				printApplyFn()
				w.Dedent()
				w.Println("}},")
			}
		}
	}
	w.Dedent()
	w.Println("});")
	w.Println("for (const auto& [name, apply] : fields) {")
	w.Indent()
	w.Println("if (path.front() == name) {")
	w.Indent()
	w.Println("return apply(path.subspan(1), this, update);")
	w.Dedent()
	w.Println("}")
	w.Dedent()
	w.Println("}")
}

func (g *implGenerator) generateServiceRoutes(service protoreflect.ServiceDescriptor, w *codewriter) {
	g.needsRpcs = true
	w.Printf("std::vector<serde::pb::rpc::route_descriptor> %s::all_routes() {\n", g.cppTypeName(service))
	defer w.Println("}")
	w.Indent()
	defer w.Dedent()
	w.Println("return {")
	defer w.Println("};")
	w.Indent()
	defer w.Dedent()
	for i := range service.Methods().Len() {
		method := service.Methods().Get(i)
		path := fmt.Sprintf("/%s", filepath.Join(string(service.FullName()), string(method.Name())))
		paths := []string{path}
		if alt := rpcAlternativeRoute(method); alt != "" {
			paths = append(paths, alt)
		}
		for _, path := range paths {
			w.Println("{")
			w.Indent()
			w.Printf(".service_name = %q,\n", service.Name())
			w.Printf(".method_name = %q,\n", method.Name())
			w.Printf(".path = %q,\n", path)
			w.Printf(".authz_level = serde::pb::rpc::authz_level::%s,\n", rpcAuthzLevel(method))
			w.Printf(".handler = std::bind_front(&%s::%s_handler_impl, this),\n", g.cppTypeName(service), pascalToSnakeCase(string(method.Name())))
			w.Dedent()
			w.Println("},")
		}
	}
}

func (g *implGenerator) generateServiceHandlers(service protoreflect.ServiceDescriptor, w *codewriter) {
	for i := range service.Methods().Len() {
		method := service.Methods().Get(i)
		w.Printf(
			"seastar::future<iobuf> %s::%s_handler_impl(serde::pb::rpc::context ctx, iobuf payload) {\n",
			g.cppTypeName(service),
			pascalToSnakeCase(string(method.Name())),
		)
		w.Indent()
		w.Println("bool is_json = ctx.content_type == serde::pb::rpc::content_type::json;")
		w.Printf("%s input;\n", g.cppTypeName(method.Input()))
		w.Println("try {")
		w.Indent()
		w.Println(`if (is_json) {`)
		w.Indent()
		w.Printf("input = co_await %s::from_json(std::move(payload));\n", g.cppTypeName(method.Input()))
		w.Dedent()
		w.Println("} else {")
		w.Indent()
		w.Printf("input = co_await %s::from_proto(std::move(payload));\n", g.cppTypeName(method.Input()))
		w.Dedent()
		w.Println("}")
		w.Dedent()
		w.Println("} catch (...) {")
		w.Indent()
		w.Println(`serde::pb::rpc::logger.debug("error parsing request`, method.FullName(), `RPC: {}", std::current_exception());`)
		w.Println(`throw serde::pb::rpc::invalid_argument_exception("invalid request input");`)
		w.Dedent()
		w.Println("}")
		w.Printf("%s output;\n", g.cppTypeName(method.Output()))
		w.Println("try {")
		w.Indent()
		w.Printf("output = co_await this->%s(std::move(ctx), std::move(input));\n", pascalToSnakeCase(string(method.Name())))
		w.Dedent()
		w.Println("} catch (const serde::pb::rpc::base_exception& e) {")
		w.Indent()
		w.Println("throw;")
		w.Dedent()
		w.Println("} catch (...) {")
		w.Indent()
		w.Println(`serde::pb::rpc::logger.warn("unhandled exception in`, method.FullName(), `RPC: {}", std::current_exception());`)
		w.Println(`throw serde::pb::rpc::internal_exception();`)
		w.Dedent()
		w.Println("}")
		w.Println("co_return is_json ? co_await output.to_json() : co_await output.to_proto();")
		w.Dedent()
		w.Println("}\n")
	}
}

func (g *implGenerator) generateServiceClient(service protoreflect.ServiceDescriptor, w *codewriter) {
	for i := range service.Methods().Len() {
		method := service.Methods().Get(i)
		w.Printf(
			"seastar::future<%s> %s_client::%s(serde::pb::rpc::context ctx, %s input) {\n",
			g.cppTypeName(method.Output()),
			g.cppTypeName(service),
			pascalToSnakeCase(string(method.Name())),
			g.cppTypeName(method.Input()),
		)
		w.Indent()
		w.Println("iobuf payload = co_await input.to_proto();")
		w.Printf("ctx.service_name = %q;\n", service.FullName())
		w.Printf("ctx.method_name = %q;\n", method.Name())
		w.Println("ctx.content_type = serde::pb::rpc::content_type::proto;")
		w.Println("payload = co_await send_rpc_fn_(std::move(ctx), std::move(payload));")
		w.Printf("co_return co_await %s::from_proto(std::move(payload));\n", g.cppTypeName(method.Output()))
		w.Dedent()
		w.Println("}")
	}
}

func (g *implGenerator) generateEnumWrite(enum protoreflect.EnumDescriptor, w *codewriter) {
	w.Printf("void enum_to_proto(const %s& e, iobuf* buf) {\n", g.cppTypeName(enum))
	defer w.Println("}")
	w.Indent()
	defer w.Dedent()
	w.Printf("serde::pb::write_varint<int32_t, serde::pb::zigzag::no>(static_cast<int32_t>(e), buf);\n")
}

func (g *implGenerator) generateEnumToString(enum protoreflect.EnumDescriptor, w *codewriter) {
	w.Printf("static_str enum_to_string(const %s& e) {\n", g.cppTypeName(enum))
	defer w.Println("}")
	w.Indent()
	defer w.Dedent()
	w.Println("switch (e) {")
	defer w.Println("}")
	for i := range enum.Values().Len() {
		v := enum.Values().Get(i)
		// Skip aliases
		if enum.Values().ByNumber(v.Number()) != v {
			continue
		}
		w.Printf("case %s::%s:\n", g.cppTypeName(enum), g.enumMemberName(v))
		w.Indent()
		w.Printf("return %q;\n", v.Name())
		w.Dedent()
	}
	w.Println("default:")
	w.Indent()
	w.Printf("return %q;\n", enum.Values().Get(0).Name())
	w.Dedent()
}

func (g *implGenerator) generateEnumFromJson(enum protoreflect.EnumDescriptor, w *codewriter) {
	w.Printf("void enum_from_json(serde::pb::json::peekable_parser* p, %s* e) {\n", g.cppTypeName(enum))
	defer w.Println("}")
	w.Indent()
	defer w.Dedent()
	w.Println("switch (p->token()) {")
	defer w.Println("}")
	func() {
		w.Println("case serde::json::token::value_string: {")
		defer w.Println("}")
		w.Indent()
		defer w.Dedent()
		w.Printf("constexpr static auto values = std::to_array<std::pair<std::string_view, %s>>({\n", g.cppTypeName(enum))
		w.Indent()
		pairs := make([]string, 0, enum.Values().Len())
		for i := range enum.Values().Len() {
			value := enum.Values().Get(i)
			// Skip aliases
			if enum.Values().ByNumber(value.Number()) != value {
				continue
			}
			pair := fmt.Sprintf("{%q, %s::%s},", value.Name(), g.cppTypeName(enum), g.enumMemberName(value))
			pairs = append(pairs, pair)
		}
		// Sort the pairs for binary search.
		slices.Sort(pairs)
		for _, pair := range pairs {
			w.Println(pair)
		}
		w.Dedent()
		w.Println("});")
		w.Println("auto eq = std::ranges::equal_range(values, p->value_string(), std::less<>(), [](const auto& pair) { return pair.first; });")
		w.Println("if (eq.empty()) {")
		w.Indent()
		defaultValue := enum.Values().ByNumber(0)
		w.Printf("*e = %s::%s;\n", g.cppTypeName(enum), g.enumMemberName(defaultValue))
		w.Dedent()
		w.Println("} else {")
		w.Indent()
		w.Println("*e = eq.front().second;")
		w.Dedent()
		w.Println("}")
		w.Println("return;")
	}()
	func() {
		w.Println("case serde::json::token::value_int: {")
		defer w.Println("}")
		w.Indent()
		defer w.Dedent()
		w.Println("switch (p->value_int()) {")
		defer w.Println("}")
		for i := range enum.Values().Len() {
			value := enum.Values().Get(i)
			// Skip aliases
			if enum.Values().ByNumber(value.Number()) != value {
				continue
			}
			w.Printf("case %d:\n", value.Number())
			w.Indent()
			w.Printf("*e = %s::%s;\n", g.cppTypeName(enum), g.enumMemberName(value))
			w.Println("return;")
			w.Dedent()
		}
		value := enum.Values().ByNumber(0)
		w.Println("default:")
		w.Indent()
		w.Printf("*e = %s::%s;\n", g.cppTypeName(enum), g.enumMemberName(value))
		w.Println("return;")
		w.Dedent()
	}()
	w.Println("default:")
	w.Indent()
	w.Println(`throw std::runtime_error(fmt::format("unexpected json token for enum, got: {}", p->token()));`)
	w.Dedent()
}

func (g *implGenerator) generateEnumRead(enum protoreflect.EnumDescriptor, w *codewriter) {
	w.Printf("void enum_from_proto(iobuf_parser* p, %s* e) {\n", g.cppTypeName(enum))
	defer w.Println("}")
	w.Indent()
	defer w.Dedent()
	w.Println("auto v = serde::pb::read_varint<int32_t, serde::pb::zigzag::no>(p);")
	w.Println("constexpr static auto values = std::to_array<int32_t>({")
	w.Indent()
	for i := range enum.Values().Len() {
		value := enum.Values().Get(i)
		w.Printf("%d, // %s\n", value.Number(), value.Name())
	}
	w.Dedent()
	w.Println("});")
	// TODO: if the enum has enough values, use a hash_set or a switch statement.
	// TODO: if the enum has contiguous values, use a bounds check.
	w.Println("if (!std::ranges::contains(values, v)) {")
	w.Indent()
	// This is sort of a controversal decision, but there are likely switch statements
	// that would break if we let enums have unknown values (open enums in proto speak).
	// so instead we use the 0 value like in proto2. See the following for more information:
	// - https://protobuf.dev/best-practices/dos-donts/#unspecified-enum
	// - https://protobuf.dev/programming-guides/enum/
	w.Println("v = 0;")
	w.Dedent()
	w.Println("}")
	w.Printf("*e = static_cast<%s>(v);\n", g.cppTypeName(enum))
}

func (g *implGenerator) generateMessageWriteJson(msg protoreflect.MessageDescriptor, w *codewriter) {
	retType := "seastar::future<iobuf>"
	method := "to_json"
	w.Printf("%s %s::%s() const {\n", retType, g.cppTypeName(msg), method)
	defer w.Println("}")
	w.Indent()
	defer w.Dedent()
	w.Println("serde::json::writer w;")
	defer w.Println("co_return std::move(w).finish();")
	w.Println("w.begin_object();")
	defer w.Println("w.end_object();")
	oneofs := map[protoreflect.Name]bool{}
	for i := range msg.Fields().Len() {
		f := msg.Fields().Get(i)
		if f.IsList() {
			w.Printf("w.key(%q);\n", f.JSONName())
			g.generateRepeatedFieldWriteJson(f, w)
		} else if f.IsMap() {
			w.Printf("w.key(%q);\n", f.JSONName())
			g.generateMapFieldWriteJson(f, w)
		} else if oneof := f.ContainingOneof(); oneof != nil {
			if oneofs[oneof.Name()] {
				continue
			}
			oneofs[oneof.Name()] = true
			g.generateOneofFieldWriteJson(oneof, w)
		} else {
			w.Printf("w.key(%q);\n", f.JSONName())
			g.generateSingularFieldWriteJson(f, w)
		}
	}
}

func (g *implGenerator) generateMapFieldWriteJson(f protoreflect.FieldDescriptor, w *codewriter) {
	w.Println("w.begin_object();")
	defer w.Println("w.end_object();")
	w.Printf("for (const auto& [key, value] : get_%s()) {\n", f.Name())
	defer w.Println("}")
	w.Indent()
	defer w.Dedent()
	switch f.MapKey().Kind() {
	case protoreflect.BoolKind:
		w.Println(`w.key(key ? "true" : "false");`)
	case protoreflect.StringKind:
		w.Println("w.key(key);")
	default:
		// integral scalars are the only other valid type.
		w.Println("w.key(std::to_string(key));")
	}
	switch f.MapValue().Kind() {
	case protoreflect.BoolKind:
		w.Println("w.boolean(value);")
	case protoreflect.BytesKind:
		w.Println("w.base64_string(value);")
	case protoreflect.DoubleKind, protoreflect.FloatKind:
		w.Println("w.number(value);")
	case protoreflect.EnumKind:
		w.Println("w.string(enum_to_string(value));")
	case protoreflect.Fixed32Kind,
		protoreflect.Int32Kind,
		protoreflect.Sint32Kind,
		protoreflect.Uint32Kind,
		protoreflect.Sfixed32Kind:
		w.Println("w.integer(value);")
	case protoreflect.Fixed64Kind,
		protoreflect.Sfixed64Kind,
		protoreflect.Sint64Kind,
		protoreflect.Int64Kind,
		protoreflect.Uint64Kind:
		w.Println("w.integer_string(value);")
	case protoreflect.GroupKind:
		g.emitError(fmt.Errorf("groups are not supported: %s", f.FullName()))
	case protoreflect.StringKind:
		w.Println("w.string(value);")
	case protoreflect.MessageKind:
		switch {
		case isWellKnownType(f.MapValue().Message()):
			w.Printf("w.append_raw_json(serde::pb::json::wellknown::%s_to_json(value));\n", pascalToSnakeCase(string(f.MapValue().Message().Name())))
		default:
			deref := "."
			if isPtr(f.MapValue()) {
				deref = "->"
				w.Println("if (value) {")
				w.Indent()
			}
			w.Printf("w.append_raw_json(co_await value%sto_json());\n", deref)
			if isPtr(f) {
				w.Dedent()
				w.Println("} else {")
				w.Indent()
				w.Println("w.null();")
				w.Dedent()
				w.Println("}")
			}
		}
	default:
		panic("unexpected protoreflect.Kind")
	}
}

func (g *implGenerator) generateRepeatedFieldWriteJson(f protoreflect.FieldDescriptor, w *codewriter) {
	w.Println("w.begin_array();")
	defer w.Println("w.end_array();")
	w.Printf("for (const auto& e : get_%s()) {\n", f.Name())
	defer w.Println("}")
	w.Indent()
	defer w.Dedent()
	switch f.Kind() {
	case protoreflect.BoolKind:
		w.Println("w.boolean(e);")
	case protoreflect.BytesKind:
		w.Println("w.base64_string(e);")
	case protoreflect.DoubleKind, protoreflect.FloatKind:
		w.Println("w.number(e);")
	case protoreflect.EnumKind:
		w.Println("w.string(enum_to_string(e));")
	case protoreflect.Fixed32Kind,
		protoreflect.Int32Kind,
		protoreflect.Sint32Kind,
		protoreflect.Uint32Kind,
		protoreflect.Sfixed32Kind:
		w.Println("w.integer(e);")
	case protoreflect.Fixed64Kind,
		protoreflect.Sfixed64Kind,
		protoreflect.Sint64Kind,
		protoreflect.Int64Kind,
		protoreflect.Uint64Kind:
		w.Println("w.integer_string(e);")
	case protoreflect.GroupKind:
		g.emitError(fmt.Errorf("groups are not supported: %s", f.FullName()))
	case protoreflect.StringKind:
		w.Println("w.string(e);")
	case protoreflect.MessageKind:
		switch {
		case isWellKnownType(f.Message()):
			w.Printf("w.append_raw_json(serde::pb::json::wellknown::%s_to_json(e));\n", pascalToSnakeCase(string(f.Message().Name())))
		default:
			deref := "."
			if isPtr(f) {
				deref = "->"
				w.Println("if (e) {")
				w.Indent()
			}
			w.Printf("w.append_raw_json(co_await e%sto_json());\n", deref)
			if isPtr(f) {
				w.Dedent()
				w.Println("} else {")
				w.Indent()
				w.Println("w.null();")
				w.Dedent()
				w.Println("}")
			}
		}
	default:
		panic("unexpected protoreflect.Kind")
	}
}

func (g *implGenerator) generateOneofFieldWriteJson(o protoreflect.OneofDescriptor, w *codewriter) {
	w.Printf("// %s\n", o.Name())
	w.Printf("switch (%s_.index()) {\n", o.Name())
	defer w.Println("}")
	w.Indent()
	defer w.Dedent()
	defer func() {
		w.Println("default: // std::monostate do nothing")
	}()
	for i := range o.Fields().Len() {
		f := o.Fields().Get(i)
		func() {
			// i+1 because of the default std::monostate case.
			w.Printf("case %v: {\n", i+1)
			defer w.Println("}")
			w.Indent()
			defer w.Dedent()
			defer w.Println("break;")
			w.Printf("w.key(%q);\n", f.JSONName())
			g.generateSingularFieldWriteJson(f, w)
		}()
	}
}

func (g *implGenerator) generateSingularFieldWriteJson(f protoreflect.FieldDescriptor, w *codewriter) {
	switch f.Kind() {
	case protoreflect.BoolKind:
		w.Printf("w.boolean(get_%s());\n", f.Name())
	case protoreflect.BytesKind:
		w.Printf("w.base64_string(get_%s());\n", f.Name())
	case protoreflect.DoubleKind, protoreflect.FloatKind:
		w.Printf("w.number(get_%s());\n", f.Name())
	case protoreflect.EnumKind:
		w.Printf("w.string(enum_to_string(get_%s()));\n", f.Name())
	case protoreflect.Fixed32Kind,
		protoreflect.Int32Kind,
		protoreflect.Sint32Kind,
		protoreflect.Uint32Kind,
		protoreflect.Sfixed32Kind:
		w.Printf("w.integer(get_%s());\n", f.Name())
	case protoreflect.Fixed64Kind,
		protoreflect.Sfixed64Kind,
		protoreflect.Sint64Kind,
		protoreflect.Int64Kind,
		protoreflect.Uint64Kind:
		w.Printf("w.integer_string(get_%s());\n", f.Name())
	case protoreflect.GroupKind:
		g.emitError(fmt.Errorf("groups are not supported: %s", f.FullName()))
	case protoreflect.StringKind:
		w.Printf("w.string(get_%s());\n", f.Name())
	case protoreflect.MessageKind:
		switch {
		case isWellKnownType(f.Message()):
			w.Printf("w.append_raw_json(serde::pb::json::wellknown::%s_to_json(get_%s()));\n", pascalToSnakeCase(string(f.Message().Name())), f.Name())
		default:
			deref := "."
			if isPtr(f) {
				deref = "->"
				w.Printf("if (get_%s()) {\n", f.Name())
				w.Indent()
			}
			w.Printf("w.append_raw_json(co_await get_%s()%sto_json());\n", f.Name(), deref)
			if isPtr(f) {
				w.Dedent()
				w.Println("} else {")
				w.Indent()
				w.Println("w.null();")
				w.Dedent()
				w.Println("}")
			}
		}
	default:
		panic("unexpected protoreflect.Kind")
	}
}

func (g *implGenerator) generateMessageWrite(msg protoreflect.MessageDescriptor, w *codewriter) {
	retType := "seastar::future<iobuf>"
	method := "to_proto"
	w.Printf("%s %s::%s() const {\n", retType, g.cppTypeName(msg), method)
	defer w.Println("}")
	w.Indent()
	defer w.Dedent()
	w.Println("iobuf buf;")
	defer w.Println("co_return buf;")
	oneofs := map[protoreflect.Name]bool{}
	for i := range msg.Fields().Len() {
		f := msg.Fields().Get(i)
		if f.IsList() {
			g.generateRepeatedFieldWrite(f, w)
		} else if f.IsMap() {
			g.generateMapFieldWrite(f, w)
		} else if oneof := f.ContainingOneof(); oneof != nil {
			if oneofs[oneof.Name()] {
				continue
			}
			oneofs[oneof.Name()] = true
			g.generateOneofFieldWrite(oneof, w)
		} else {
			g.generateSingularFieldWrite(f, w)
		}
	}
}

func (g *implGenerator) generateSingularFieldWrite(f protoreflect.FieldDescriptor, w *codewriter) {
	var wireType string
	switch f.Kind() {
	case protoreflect.BoolKind,
		protoreflect.Uint32Kind,
		protoreflect.Uint64Kind,
		protoreflect.EnumKind,
		protoreflect.Int32Kind,
		protoreflect.Int64Kind,
		protoreflect.Sint32Kind,
		protoreflect.Sint64Kind:
		wireType = "varint"
	case protoreflect.FloatKind, protoreflect.Fixed32Kind, protoreflect.Sfixed32Kind:
		wireType = "i32"
	case protoreflect.DoubleKind, protoreflect.Fixed64Kind, protoreflect.Sfixed64Kind:
		wireType = "i64"
	case protoreflect.MessageKind:
		// For message types, we need to serialize an intermediate buffer, so
		// scope those to prevent variable re-declaration issues.
		w.Println("{")
		defer w.Println("}")
		w.Indent()
		defer w.Dedent()
		fallthrough
	case protoreflect.StringKind, protoreflect.BytesKind:
		wireType = "length"
	case protoreflect.GroupKind:
		g.emitError(fmt.Errorf("groups are not supported: %s", f.FullName()))
		return
	default:
		panic(fmt.Sprintf("unexpected protoreflect.Kind: %#v", f.Kind()))
	}
	w.Printf("// %s\n", f.Name())
	if f.Kind() != protoreflect.MessageKind {
		w.Printf("serde::pb::tag::write({.wire_type = serde::pb::wire_type::%s, .field_number = %d}, &buf);\n", wireType, f.Number())
	}
	switch f.Kind() {
	case protoreflect.BoolKind:
		w.Printf("serde::pb::write_varint<int32_t>(static_cast<int32_t>(get_%s()), &buf);\n", f.Name())
	case protoreflect.Int32Kind:
		w.Printf("serde::pb::write_varint<int32_t, serde::pb::zigzag::no>(get_%s(), &buf);\n", f.Name())
	case protoreflect.Int64Kind:
		w.Printf("serde::pb::write_varint<int64_t, serde::pb::zigzag::no>(get_%s(), &buf);\n", f.Name())
	case protoreflect.Sint32Kind:
		w.Printf("serde::pb::write_varint<int32_t>(get_%s(), &buf);\n", f.Name())
	case protoreflect.Sint64Kind:
		w.Printf("serde::pb::write_varint<int64_t>(get_%s(), &buf);\n", f.Name())
	case protoreflect.Uint32Kind:
		w.Printf("serde::pb::write_varint<uint32_t>(get_%s(), &buf);\n", f.Name())
	case protoreflect.Uint64Kind:
		w.Printf("serde::pb::write_varint<uint64_t>(get_%s(), &buf);\n", f.Name())
	case protoreflect.EnumKind:
		w.Printf("enum_to_proto(get_%s(), &buf);\n", f.Name())
	case protoreflect.DoubleKind:
		w.Printf("buf.append(std::bit_cast<std::array<const uint8_t, sizeof(double)>>(get_%s()));\n", f.Name())
	case protoreflect.Fixed32Kind, protoreflect.Sfixed32Kind:
		w.Printf("buf.append(std::bit_cast<std::array<const uint8_t, sizeof(int32_t)>>(get_%s()));\n", f.Name())
	case protoreflect.Fixed64Kind, protoreflect.Sfixed64Kind:
		w.Printf("buf.append(std::bit_cast<std::array<const uint8_t, sizeof(int64_t)>>(get_%s()));\n", f.Name())
	case protoreflect.FloatKind:
		w.Printf("buf.append(std::bit_cast<std::array<const uint8_t, sizeof(float)>>(get_%s()));\n", f.Name())
	case protoreflect.MessageKind:
		deref := "."
		if isPtr(f) {
			deref = "->"
			w.Printf("if (get_%s()) {\n", f.Name())
			w.Indent()
		}
		w.Printf("serde::pb::tag::write({.wire_type = serde::pb::wire_type::%s, .field_number = %d}, &buf);\n", wireType, f.Number())
		switch {
		case isWellKnownType(f.Message()):
			w.Printf("iobuf msg_buf = serde::pb::wellknown::%s_to_proto(get_%s());\n", pascalToSnakeCase(string(f.Message().Name())), f.Name())
		default:
			w.Printf("iobuf msg_buf = co_await get_%s()%sto_proto();\n", f.Name(), deref)
		}
		w.Printf("serde::pb::write_length(static_cast<int32_t>(msg_buf.size_bytes()), &buf);\n")
		w.Println("buf.append(std::move(msg_buf));")
		if isPtr(f) {
			w.Dedent()
			w.Println("}")
		}
	case protoreflect.StringKind:
		if !isIOBuf(f) {
			w.Printf("serde::pb::write_length(static_cast<int32_t>(get_%s().size()), &buf);\n", f.Name())
			w.Printf("buf.append(get_%s().data(), get_%s().size());\n", f.Name(), f.Name())
			break
		}
		fallthrough
	case protoreflect.BytesKind:
		w.Printf("serde::pb::write_length(static_cast<int32_t>(get_%s().size_bytes()), &buf);\n", f.Name())
		w.Printf("buf.append(get_%s().copy());\n", f.Name())
	case protoreflect.GroupKind:
		fallthrough
	default:
		panic(fmt.Sprintf("unexpected protoreflect.Kind: %#v", f.Kind()))
	}
}

func (g *implGenerator) generateOneofFieldWrite(o protoreflect.OneofDescriptor, w *codewriter) {
	w.Printf("// %s\n", o.Name())
	w.Printf("switch (%s_.index()) {\n", o.Name())
	defer w.Println("}")
	w.Indent()
	defer w.Dedent()
	defer func() {
		w.Println("default: // std::monostate do nothing")
	}()
	for i := range o.Fields().Len() {
		f := o.Fields().Get(i)
		func() {
			// i+1 because of the default std::monostate case.
			w.Printf("case %v: {\n", i+1)
			defer w.Println("}")
			w.Indent()
			defer w.Dedent()
			defer w.Println("break;")
			g.generateSingularFieldWrite(f, w)
		}()
	}
}

func (g *implGenerator) generateMapFieldWrite(f protoreflect.FieldDescriptor, w *codewriter) {
	w.Printf("// %s\n", f.Name())
	w.Printf("for (const auto& [key, value] : get_%s()) {\n", f.Name())
	w.Indent()
	w.Println("iobuf& parent_buf = buf;")
	w.Println("iobuf buf;")
	keyType, isKeyTrivial := g.translateType(f.MapKey())
	valueType, isValueTrivial := g.translateType(f.MapValue())
	if isKeyTrivial {
		w.Printf("auto get_key = [key]() -> %s { return key; };\n", keyType)
	} else {
		w.Printf("auto get_key = [&key]() -> const %s& { return key; };\n", keyType)
	}
	if isValueTrivial {
		w.Printf("auto get_value = [value]() -> %s { return value; };\n", valueType)
	} else {
		w.Printf("auto get_value = [&value]() -> const %s& { return value; };\n", valueType)
	}
	g.generateSingularFieldWrite(f.MapKey(), w)
	g.generateSingularFieldWrite(f.MapValue(), w)
	w.Println("// now write the entry submessage")
	w.Printf("serde::pb::tag::write({.wire_type = serde::pb::wire_type::length, .field_number = %d}, &parent_buf);\n", f.Number())
	w.Println("serde::pb::write_length(static_cast<int32_t>(buf.size_bytes()), &parent_buf);")
	w.Println("parent_buf.append(std::move(buf));")
	w.Dedent()
	w.Println("}")
}

func (g *implGenerator) generateRepeatedFieldWrite(f protoreflect.FieldDescriptor, w *codewriter) {
	w.Printf("// %s\n", f.Name())
	generatePackedFieldWrite := func(writeFn string) {
		w.Println("{")
		defer w.Println("}")
		w.Indent()
		defer w.Dedent()
		w.Println("iobuf repeated_buf;")
		w.Printf("for (auto e : get_%s()) {\n", f.Name())
		w.Indent()
		w.Println(writeFn)
		w.Dedent()
		w.Println("}")
		w.Printf("serde::pb::tag::write({.wire_type = serde::pb::wire_type::length, .field_number = %d}, &buf);\n", f.Number())
		w.Println("serde::pb::write_length(static_cast<int32_t>(repeated_buf.size_bytes()), &buf);")
		w.Println("buf.append(std::move(repeated_buf));")
	}
	switch f.Kind() {
	case protoreflect.BoolKind:
		generatePackedFieldWrite("serde::pb::write_varint<int32_t>(static_cast<int32_t>(e), &repeated_buf);")
	case protoreflect.Int32Kind:
		generatePackedFieldWrite("serde::pb::write_varint<int32_t, serde::pb::zigzag::no>(e, &repeated_buf);")
	case protoreflect.Int64Kind:
		generatePackedFieldWrite("serde::pb::write_varint<int64_t, serde::pb::zigzag::no>(e, &repeated_buf);")
	case protoreflect.Sint32Kind:
		generatePackedFieldWrite("serde::pb::write_varint<int32_t>(e, &repeated_buf);")
	case protoreflect.Sint64Kind:
		generatePackedFieldWrite("serde::pb::write_varint<int64_t>(e, &repeated_buf);")
	case protoreflect.Uint32Kind:
		generatePackedFieldWrite("serde::pb::write_varint<uint32_t>(e, &repeated_buf);")
	case protoreflect.Uint64Kind:
		generatePackedFieldWrite("serde::pb::write_varint<uint64_t>(e, &repeated_buf);")
	case protoreflect.EnumKind:
		generatePackedFieldWrite("enum_to_proto(e, &repeated_buf);")
	case protoreflect.DoubleKind:
		generatePackedFieldWrite("repeated_buf.append(std::bit_cast<std::array<const uint8_t, sizeof(double)>>(e));")
	case protoreflect.Fixed32Kind, protoreflect.Sfixed32Kind:
		generatePackedFieldWrite("repeated_buf.append(std::bit_cast<std::array<const uint8_t, sizeof(int32_t)>>(e));")
	case protoreflect.Fixed64Kind, protoreflect.Sfixed64Kind:
		generatePackedFieldWrite("repeated_buf.append(std::bit_cast<std::array<const uint8_t, sizeof(int64_t)>>(e));")
	case protoreflect.FloatKind:
		generatePackedFieldWrite("repeated_buf.append(std::bit_cast<std::array<const uint8_t, sizeof(float)>>(e));")
	case protoreflect.MessageKind:
		w.Printf("for (const auto& e : get_%s()) {\n", f.Name())
		w.Indent()
		deref := "."
		if isPtr(f) {
			deref = "->"
			w.Println("if (e) {")
			w.Indent()
		}
		switch {
		case isWellKnownType(f.Message()):
			w.Printf("iobuf msg_buf = serde::pb::wellknown::%s_to_proto(e);\n", pascalToSnakeCase(string(f.Message().Name())))
		default:
			w.Printf("iobuf msg_buf = co_await e%sto_proto();\n", deref)
		}
		w.Printf("serde::pb::tag::write({.wire_type = serde::pb::wire_type::length, .field_number = %d}, &buf);\n", f.Number())
		w.Println("serde::pb::write_length(static_cast<int32_t>(msg_buf.size_bytes()), &buf);")
		w.Println("buf.append(std::move(msg_buf));")
		if isPtr(f) {
			w.Dedent()
			w.Println("}")
		}
		w.Dedent()
		w.Println("}")
	case protoreflect.StringKind:
		if !isIOBuf(f) {
			w.Printf("for (const auto& e : get_%s()) {\n", f.Name())
			w.Indent()
			w.Printf("serde::pb::tag::write({.wire_type = serde::pb::wire_type::length, .field_number = %d}, &buf);\n", f.Number())
			w.Println("serde::pb::write_length(static_cast<int32_t>(e.size()), &buf);")
			w.Println("buf.append(e.data(), e.size());")
			w.Dedent()
			w.Println("}")
			break
		}
		fallthrough
	case protoreflect.BytesKind:
		w.Printf("for (const auto& e : get_%s()) {\n", f.Name())
		w.Indent()
		w.Printf("serde::pb::tag::write({.wire_type = serde::pb::wire_type::length, .field_number = %d}, &buf);\n", f.Number())
		w.Println("serde::pb::write_length(static_cast<int32_t>(e.size_bytes()), &buf);")
		w.Println("buf.append(e.copy());")
		w.Dedent()
		w.Println("}")
	case protoreflect.GroupKind:
		fallthrough
	default:
		panic(fmt.Sprintf("unexpected protoreflect.Kind: %#v", f.Kind()))
	}
}

func (g *implGenerator) generateMessageReadJsonHelper(msg protoreflect.MessageDescriptor, w *codewriter) {
	cppType := g.cppTypeName(msg)
	w.Printf("seastar::future<%s> %s::from_json(iobuf data) {\n", cppType, cppType)
	defer w.Println("}")
	w.Indent()
	defer w.Dedent()
	w.Printf("%s self;\n", cppType)
	w.Println("serde::pb::json::peekable_parser parser(std::move(data));")
	w.Printf("co_await from_json(&parser, &self);\n")
	w.Println("co_await serde::pb::json::check_next_eof(&parser);")
	w.Println("co_return self;")
}

func (g *implGenerator) generateMessageReadJson(msg protoreflect.MessageDescriptor, w *codewriter) {
	cppType := g.cppTypeName(msg)
	w.Printf("seastar::future<> %s::from_json(serde::pb::json::peekable_parser* parser, %s* self) {\n", cppType, cppType)
	defer w.Println("}")
	w.Indent()
	defer w.Dedent()
	defer w.Println("co_return;")
	if msg.Fields().Len() == 0 {
		w.Println("std::ignore = self;")
		w.Printf("constexpr static std::array<std::pair<std::string_view, int32_t>, 0> key_to_field_number = {};\n")
	} else {
		w.Printf("constexpr static auto key_to_field_number = std::to_array<std::pair<std::string_view, int32_t>>({\n")
		w.Indent()
		pairs := make([]string, 0, msg.Fields().Len()*2)
		for i := range msg.Fields().Len() {
			f := msg.Fields().Get(i)
			pairs = append(pairs, fmt.Sprintf("{%q, %d},", f.Name(), f.Number()))
			if string(f.Name()) != f.JSONName() {
				pairs = append(pairs, fmt.Sprintf("{%q, %d},", f.JSONName(), f.Number()))
			}
		}
		// Sort the pairs for binary search.
		slices.Sort(pairs)
		for _, pair := range pairs {
			w.Println(pair)
		}
		w.Dedent()
		w.Println("});")
	}
	w.Println("auto entries = serde::pb::json::object_key_generator(parser);")
	w.Println("while (auto key = co_await entries()) {")
	defer w.Println("}")
	w.Indent()
	defer w.Dedent()
	w.Println("auto fields = std::ranges::equal_range(key_to_field_number, *key, std::less<>(), [](const auto& pair) { return pair.first; });")
	w.Println("if (fields.empty()) {")
	w.Indent()
	w.Println("co_await parser->skip_value();")
	w.Println("continue;")
	w.Dedent()
	w.Println("}")
	w.Println("switch (fields.front().second) {")
	defer w.Println("}")
	generateMessageFieldRead := func(f protoreflect.FieldDescriptor, setter string) {
		typ := g.translateBaseType(f)
		w.Println("if (co_await parser->peek() == serde::json::token::value_null) {")
		w.Indent()
		w.Println("co_await parser->next();")
		w.Dedent()
		w.Println("} else {")
		w.Indent()
		switch {
		case isWellKnownType(f.Message()):
			w.Printf("auto v = co_await serde::pb::json::wellknown::%s_from_json(parser);\n", pascalToSnakeCase(string(f.Message().Name())))
		case isPtr(f):
			w.Printf("auto v = std::make_unique<%s>();\n", typ)
			w.Printf("co_await %s::from_json(parser, v.get());\n", typ)
		default:
			w.Printf("%s v{};\n", typ)
			w.Printf("co_await %s::from_json(parser, &v);\n", typ)
		}
		w.Println(fmt.Sprintf(setter, "std::move(v)"))
		w.Dedent()
		w.Println("}")
	}
	generateEnumFieldRead := func(f protoreflect.FieldDescriptor, setter string) {
		w.Println("co_await parser->next();")
		typ := g.translateBaseType(f)
		w.Printf("%s v{};\n", typ)
		w.Printf("enum_from_json(parser, &v);\n")
		w.Println(fmt.Sprintf(setter, "v"))
	}
	generateScalarFieldRead := func(f protoreflect.FieldDescriptor, setter string) {
		w.Println("co_await parser->next();")
		scalarMethod := "impossible"
		switch f.Kind() {
		case protoreflect.BoolKind:
			scalarMethod = "bool"
		case protoreflect.BytesKind:
			scalarMethod = "base64_encoded_bytes"
		case protoreflect.DoubleKind:
			scalarMethod = "double"
		case protoreflect.FloatKind:
			scalarMethod = "float"
		case protoreflect.StringKind:
			if isIOBuf(f) {
				scalarMethod = "string_as_bytes"
			} else {
				scalarMethod = "string"
			}
		case protoreflect.Fixed32Kind, protoreflect.Uint32Kind:
			scalarMethod = "uint32"
		case protoreflect.Int32Kind, protoreflect.Sfixed32Kind, protoreflect.Sint32Kind:
			scalarMethod = "int32"
		case protoreflect.Fixed64Kind, protoreflect.Uint64Kind:
			scalarMethod = "uint64"
		case protoreflect.Int64Kind, protoreflect.Sfixed64Kind, protoreflect.Sint64Kind:
			scalarMethod = "int64"
		}
		method := fmt.Sprintf("serde::pb::json::read_%s(parser)", scalarMethod)
		w.Println(fmt.Sprintf(setter, method))
	}
	for i := range msg.Fields().Len() {
		f := msg.Fields().Get(i)
		w.Printf("case %d: { // %s\n", f.Number(), f.Name())
		w.Indent()
		if f.IsList() {
			w.Println("if (co_await parser->peek() == serde::json::token::value_null) {")
			w.Indent()
			w.Println("co_await parser->next();")
			w.Dedent()
			w.Println("} else {")
			w.Indent()
			w.Println("auto elements = serde::pb::json::array_element_generator(parser);")
			w.Println("while (co_await elements()) {")
			w.Indent()
			setter := "self->get_" + string(f.Name()) + "().push_back(%v);"
			if f.Kind() == protoreflect.MessageKind {
				generateMessageFieldRead(f, setter)
			} else if f.Kind() == protoreflect.EnumKind {
				generateEnumFieldRead(f, setter)
			} else {
				generateScalarFieldRead(f, setter)
			}
			w.Dedent()
			w.Println("}")
			w.Dedent()
			w.Println("}")
		} else if f.IsMap() {
			w.Println("if (co_await parser->peek() == serde::json::token::value_null) {")
			w.Indent()
			w.Println("co_await parser->next();")
			w.Dedent()
			w.Println("} else {")
			w.Indent()
			w.Println("auto map_entries = serde::pb::json::object_key_generator(parser);")
			w.Println("while (auto map_key = co_await map_entries()) {")
			w.Indent()
			keyType, isTrivial := g.translateType(f.MapKey())
			w.Printf("auto k = serde::pb::json::transform_map_key<%s>(std::move(*map_key));\n", keyType)
			keyMove := "std::move(k)"
			if isTrivial {
				keyMove = "k"
			}
			setter := "self->get_" + string(f.Name()) + "().insert_or_assign(" + keyMove + ", %v);"
			if f.MapValue().Kind() == protoreflect.MessageKind {
				generateMessageFieldRead(f.MapValue(), setter)
			} else if f.MapValue().Kind() == protoreflect.EnumKind {
				generateEnumFieldRead(f.MapValue(), setter)
			} else {
				generateScalarFieldRead(f.MapValue(), setter)
			}
			w.Dedent()
			w.Println("}")
			w.Dedent()
			w.Println("}")
		} else if f.Kind() == protoreflect.MessageKind {
			generateMessageFieldRead(f, "self->set_"+string(f.Name())+"(%v);")
		} else if f.Kind() == protoreflect.EnumKind {
			generateEnumFieldRead(f, "self->set_"+string(f.Name())+"(%v);")
		} else {
			generateScalarFieldRead(f, "self->set_"+string(f.Name())+"(%v);")
		}
		w.Println("break;")
		w.Dedent()
		w.Println("}")
	}
	w.Println("default:")
	w.Indent()
	w.Println(`vunreachable("codegen error unexpected field number: {}", fields.front().second);`)
	w.Dedent()
}

func (g *implGenerator) generateMessageReadHelper(msg protoreflect.MessageDescriptor, w *codewriter) {
	cppType := g.cppTypeName(msg)
	retType := fmt.Sprintf("seastar::future<%s>", cppType)
	method := "from_proto"
	w.Printf("%s %s::%s(iobuf buf) {\n", retType, cppType, method)
	defer w.Println("}")
	w.Indent()
	defer w.Dedent()
	w.Printf("%s self;\n", cppType)
	w.Println("serde::pb::wire_format_parser parser{std::move(buf)};")
	w.Printf("co_await %s(&parser, &self);\n", method)
	w.Println("parser.check_empty();")
	w.Println("co_return self;")
}

func (g *implGenerator) generateMessage(msg protoreflect.MessageDescriptor, w *codewriter) {
	parentType := g.cppTypeName(msg)
	w.Printf("%s::%s() noexcept = default;\n", parentType, parentType)
	w.Printf("%s::%s(%s&&) noexcept = default;\n", parentType, parentType, parentType)
	w.Printf("%s& %s::operator=(%s&&) noexcept = default;\n", parentType, parentType, parentType)
	w.Printf("%s::~%s() noexcept = default;\n", parentType, parentType)
	type FmtField struct {
		displayName string
		argValue    string
	}
	eqFields := []string{}
	fmtFields := []FmtField{}
	oneofs := map[protoreflect.Name]bool{}
	for i := range msg.Fields().Len() {
		field := msg.Fields().Get(i)
		fieldType, trivial := g.translateType(field)
		if oneof := field.ContainingOneof(); oneof != nil {
			if !oneofs[oneof.Name()] {
				oneofs[oneof.Name()] = true
				eqFields = append(eqFields, fmt.Sprintf("(%s_ == other.%s_)", oneof.Name(), oneof.Name()))
				fmtFields = append(fmtFields, FmtField{
					displayName: string(oneof.Name()),
					argValue:    fmt.Sprintf("%s_", oneof.Name()),
				})
				if oneof.IsSynthetic() {
					w.Printf("void %s::clear_%s() { %s_ = std::monostate{}; }\n", parentType, field.Name(), oneof.Name())
				} else {
					w.Printf("bool %s::has_%s() const { return %s_.index() != 0; }\n", parentType, oneof.Name(), oneof.Name())
					w.Printf("void %s::clear_%s() { %s_ = std::monostate{}; }\n", parentType, oneof.Name(), oneof.Name())
				}
			}
			idx := getOneofFieldVariantIndex(oneof, field)
			w.Printf("bool %s::has_%s() const { return %s_.index() == %d; }\n", parentType, field.Name(), oneof.Name(), idx)
			if trivial {
				w.Printf("%s %s::get_%s() const { return std::get<%d>(%s_); }\n", fieldType, parentType, field.Name(), idx, oneof.Name())
				w.Printf("void %s::set_%s(%s v) { %s_.emplace<%d>(v); }\n", parentType, field.Name(), fieldType, oneof.Name(), idx)
			} else {
				w.Printf("%s& %s::get_%s() { return std::get<%d>(%s_); }\n", fieldType, parentType, field.Name(), idx, oneof.Name())
				w.Printf("const %s& %s::get_%s() const { return std::get<%d>(%s_); }\n", fieldType, parentType, field.Name(), idx, oneof.Name())
				w.Printf("void %s::set_%s(%s&& v) { %s_.emplace<%d>(std::move(v)); }\n", parentType, field.Name(), fieldType, oneof.Name(), idx)
			}
		} else {
			if isPtr(field) {
				eqFields = append(eqFields, fmt.Sprintf("((!%s_ || !other.%s_ ) ? !%s_ == !other.%s_ : *%s_ == *other.%s_)", slices.Repeat([]any{field.Name()}, 6)...))
			} else {
				eqFields = append(eqFields, fmt.Sprintf("(%s_ == other.%s_)", field.Name(), field.Name()))
			}
			argValue := fmt.Sprintf("%s_", field.Name())
			if isDebugRedacted(field) {
				argValue = `"<redacted>"`
			}
			fmtFields = append(fmtFields, FmtField{
				displayName: string(field.Name()),
				argValue:    argValue,
			})
			if trivial {
				w.Printf("%s %s::get_%s() const { return %s_; }\n", fieldType, parentType, field.Name(), field.Name())
				w.Printf("void %s::set_%s(%s v) { %s_ = v; }\n", parentType, field.Name(), fieldType, field.Name())
			} else {
				w.Printf("%s& %s::get_%s() { return %s_; }\n", fieldType, parentType, field.Name(), field.Name())
				w.Printf("const %s& %s::get_%s() const { return %s_; }\n", fieldType, parentType, field.Name(), field.Name())
				w.Printf("void %s::set_%s(%s&& v) { %s_ = std::move(v); }\n", parentType, field.Name(), fieldType, field.Name())
			}
		}
	}
	w.Printf("bool %s::operator==(const %s& other) const {\n", parentType, parentType)
	w.Indent()
	if len(eqFields) == 0 {
		w.Println("std::ignore = other;")
		w.Println("return true;")
	} else {
		w.Printf("return %s;\n", strings.Join(eqFields, " && "))
	}
	w.Dedent()
	w.Println("}")
	w.Printf("fmt::iterator %s::format_to(fmt::iterator it) const {\n", parentType)
	defer w.Println("}")
	w.Indent()
	defer w.Dedent()
	var fmtSpec strings.Builder
	var fmtArgs strings.Builder
	fmtSpec.WriteString("{{")
	for i, f := range fmtFields {
		if i > 0 {
			fmtSpec.WriteString(", ")
			fmtArgs.WriteString(", ")
		}
		fmtSpec.WriteString(f.displayName)
		fmtSpec.WriteString(": {}")
		fmtArgs.WriteString(f.argValue)
	}
	fmtSpec.WriteString("}}")
	if len(fmtFields) == 0 {
		w.Printf("return fmt::format_to(it, %q);\n", fmtSpec.String())
	} else {
		w.Printf("return fmt::format_to(it, %q, %s);\n", fmtSpec.String(), fmtArgs.String())
	}

}

func (g *implGenerator) generateMessageRead(msg protoreflect.MessageDescriptor, w *codewriter) {
	retType := "seastar::future<>"
	method := "from_proto"
	w.Printf("%s %s::%s(serde::pb::wire_format_parser* parser, %s* self) {\n", retType, g.cppTypeName(msg), method, g.cppTypeName(msg))
	defer w.Println("}")
	w.Indent()
	defer w.Dedent()
	defer w.Println("co_return;")
	if msg.Fields().Len() == 0 {
		w.Println("std::ignore = self;")
	}
	w.Println("while (parser->bytes_left() > 0) {")
	defer w.Println("}")
	w.Indent()
	defer w.Dedent()
	w.Println("auto tag = parser->read_tag();")
	w.Println("switch (tag.field_number) {")
	defer w.Println("}")
	defer func() {
		w.Println("default:")
		w.Indent()
		defer w.Dedent()
		w.Println("parser->skip_unknown(tag);")
		w.Println("break;")
	}()
	for i := range msg.Fields().Len() {
		f := msg.Fields().Get(i)
		func() {
			w.Printf("case %v: { // %s\n", f.Number(), f.Name())
			defer w.Println("}")
			w.Indent()
			defer w.Dedent()
			defer w.Println("break;")
			g.generateFieldRead(f, w)
		}()
	}
}

func (g *implGenerator) generateFieldRead(f protoreflect.FieldDescriptor, w *codewriter) {
	if f.IsList() {
		g.generateRepeatedFieldRead(f, w)
	} else if f.IsMap() {
		g.generateMapFieldRead(f, w)
	} else {
		g.generateSingularFieldRead(f, w)
	}
}

func (g *implGenerator) generateMapFieldRead(f protoreflect.FieldDescriptor, w *codewriter) {
	w.Printf("serde::pb::wire_format_parser entry_parser = parser->read_message<%q>(tag);\n", f.FullName())
	w.Println("serde::pb::wire_format_parser* parser = &entry_parser;")
	w.Printf("struct map_entry {\n")
	w.Indent()
	keyType, isKeyTrivial := g.translateType(f.MapKey())
	valueType, isValueTrivial := g.translateType(f.MapValue())
	w.Printf("%s key{};\n", keyType)
	w.Printf("%s value{};\n", valueType)
	if isKeyTrivial {
		w.Printf("void set_key(%s k) { key = k; }\n", keyType)
	} else {
		w.Printf("void set_key(%s&& k) { key = std::move(k); }\n", keyType)
	}
	if isValueTrivial {
		w.Printf("void set_value(%s v) { value = v; }\n", valueType)
	} else {
		w.Printf("void set_value(%s&& v) { value = std::move(v); }\n", valueType)
	}
	w.Dedent()
	w.Println("};")
	w.Println("map_entry entry;")
	defer func() {
		key := "std::move(entry.key)"
		if isKeyTrivial {
			key = "entry.key"
		}
		value := "std::move(entry.value)"
		if isValueTrivial {
			value = "entry.value"
		}
		w.Printf("self->get_%s().insert_or_assign(%s, %s);\n", f.Name(), key, value)
	}()
	w.Println("{")
	defer w.Println("}")
	w.Indent()
	defer w.Dedent()
	w.Println("map_entry* self = &entry;")
	w.Println("while (parser->bytes_left() > 0) {")
	defer w.Println("}")
	w.Indent()
	defer w.Dedent()
	w.Println("auto tag = parser->read_tag();")
	w.Println("switch (tag.field_number) {")
	defer w.Println("}")
	defer func() {
		w.Println("default:")
		w.Indent()
		defer w.Dedent()
		w.Println("parser->skip_unknown(tag);")
		w.Println("break;")
	}()
	for _, f := range []protoreflect.FieldDescriptor{f.MapKey(), f.MapValue()} {
		func() {
			w.Printf("case %v: { // %s\n", f.Number(), f.Name())
			defer w.Println("}")
			w.Indent()
			defer w.Dedent()
			defer w.Println("break;")
			g.generateSingularFieldRead(f, w)
		}()
	}
}

func (g *implGenerator) generateRepeatedFieldRead(f protoreflect.FieldDescriptor, w *codewriter) {
	switch f.Kind() {
	case protoreflect.BoolKind:
		w.Printf("co_await parser->read_repeated_bool<%q>(tag, &self->get_%s());\n", f.FullName(), f.Name())
	case protoreflect.EnumKind:
		enumType := g.translateBaseType(f)
		w.Printf("co_await parser->read_repeated_enum<%q, %s>(tag, &self->get_%s());\n", f.FullName(), enumType, f.Name())
	case protoreflect.Int32Kind:
		w.Printf("co_await parser->read_repeated_varint<%q, int32_t, serde::pb::zigzag::no>(tag, &self->get_%s());\n", f.FullName(), f.Name())
	case protoreflect.Int64Kind:
		w.Printf("co_await parser->read_repeated_varint<%q, int64_t, serde::pb::zigzag::no>(tag, &self->get_%s());\n", f.FullName(), f.Name())
	case protoreflect.Sint32Kind:
		w.Printf("co_await parser->read_repeated_varint<%q, int32_t>(tag, &self->get_%s());\n", f.FullName(), f.Name())
	case protoreflect.Sint64Kind:
		w.Printf("co_await parser->read_repeated_varint<%q, int64_t>(tag, &self->get_%s());\n", f.FullName(), f.Name())
	case protoreflect.Uint32Kind:
		w.Printf("co_await parser->read_repeated_varint<%q, uint32_t>(tag, &self->get_%s());\n", f.FullName(), f.Name())
	case protoreflect.Uint64Kind:
		w.Printf("co_await parser->read_repeated_varint<%q, uint64_t>(tag, &self->get_%s());\n", f.FullName(), f.Name())
	case protoreflect.FloatKind, protoreflect.Fixed32Kind, protoreflect.Sfixed32Kind,
		protoreflect.DoubleKind, protoreflect.Fixed64Kind, protoreflect.Sfixed64Kind:
		scalarType := g.translateBaseType(f)
		w.Printf("co_await parser->read_repeated_fixed<%q, %s>(tag, &self->get_%s());\n", f.FullName(), scalarType, f.Name())
	case protoreflect.StringKind:
		if !isIOBuf(f) {
			w.Printf("self->get_%s().push_back(parser->read_string<%q>(tag));\n", f.Name(), f.FullName())
			break
		}
		fallthrough
	case protoreflect.BytesKind:
		w.Printf("self->get_%s().push_back(parser->read_bytes<%q>(tag));\n", f.Name(), f.FullName())
	case protoreflect.MessageKind:
		switch {
		case isWellKnownType(f.Message()):
			w.Printf(
				"self->get_%s().push_back(parser->read_wellknown_%s<%q>(tag));\n",
				f.Name(),
				pascalToSnakeCase(string(f.Message().Name())),
				f.FullName(),
			)
		default:
			w.Printf("auto msg_parser = parser->read_message<%q>(tag);\n", f.FullName())
			msgType := g.translateBaseType(f)
			w.Printf("co_await %s::from_proto(&msg_parser, &self->get_%s().emplace_back());\n", msgType, f.Name())
		}
	case protoreflect.GroupKind:
		g.emitError(fmt.Errorf("groups are not supported: %s", f.FullName()))
		w.Println(`throw std::runtime_error("groups are not supported");`)
	default:
		panic(fmt.Sprintf("unexpected protoreflect.Kind: %#v", f.Kind()))
	}
}

func (g *implGenerator) generateSingularFieldRead(f protoreflect.FieldDescriptor, w *codewriter) {
	switch f.Kind() {
	case protoreflect.BoolKind:
		w.Printf("self->set_%s(parser->read_singular_bool<%q>(tag));\n", f.Name(), f.FullName())
	case protoreflect.EnumKind:
		enumType := g.translateBaseType(f)
		w.Printf("self->set_%s(parser->read_singular_enum<%q, %s>(tag));\n", f.Name(), f.FullName(), enumType)
	case protoreflect.Int32Kind:
		w.Printf("self->set_%s(parser->read_singular_varint<%q, int32_t, serde::pb::zigzag::no>(tag));\n", f.Name(), f.FullName())
	case protoreflect.Int64Kind:
		w.Printf("self->set_%s(parser->read_singular_varint<%q, int64_t, serde::pb::zigzag::no>(tag));\n", f.Name(), f.FullName())
	case protoreflect.Sint32Kind:
		w.Printf("self->set_%s(parser->read_singular_varint<%q, int32_t>(tag));\n", f.Name(), f.FullName())
	case protoreflect.Sint64Kind:
		w.Printf("self->set_%s(parser->read_singular_varint<%q, int64_t>(tag));\n", f.Name(), f.FullName())
	case protoreflect.Uint32Kind:
		w.Printf("self->set_%s(parser->read_singular_varint<%q, uint32_t>(tag));\n", f.Name(), f.FullName())
	case protoreflect.Uint64Kind:
		w.Printf("self->set_%s(parser->read_singular_varint<%q, uint64_t>(tag));\n", f.Name(), f.FullName())
	case protoreflect.FloatKind, protoreflect.Fixed32Kind, protoreflect.Sfixed32Kind,
		protoreflect.DoubleKind, protoreflect.Fixed64Kind, protoreflect.Sfixed64Kind:
		scalarType := g.translateBaseType(f)
		w.Printf("self->set_%s(parser->read_fixed<%q, %s>(tag));\n", f.Name(), f.FullName(), scalarType)
	case protoreflect.StringKind:
		if !isIOBuf(f) {
			w.Printf("self->set_%s(parser->read_string<%q>(tag));\n", f.Name(), f.FullName())
			break
		}
		fallthrough
	case protoreflect.BytesKind:
		w.Printf("self->set_%s(parser->read_bytes<%q>(tag));\n", f.Name(), f.FullName())
	case protoreflect.MessageKind:
		switch {
		case isWellKnownType(f.Message()):
			w.Printf("self->set_%s(parser->read_wellknown_%s<%q>(tag));\n",
				f.Name(),
				pascalToSnakeCase(string(f.Message().Name())),
				f.FullName(),
			)
		default:
			w.Printf("auto msg_parser = parser->read_message<%q>(tag);\n", f.FullName())
			msgType := g.translateBaseType(f)
			if isPtr(f) {
				w.Printf("auto sub_msg = std::make_unique<%s>();\n", msgType)
				w.Printf("co_await %s::from_proto(&msg_parser, sub_msg.get());\n", msgType)
				w.Printf("self->set_%s(std::move(sub_msg));\n", f.Name())
			} else {
				w.Printf("%s sub_msg;\n", msgType)
				w.Printf("co_await %s::from_proto(&msg_parser, &sub_msg);\n", msgType)
				w.Printf("self->set_%s(std::move(sub_msg));\n", f.Name())
			}
		}
	case protoreflect.GroupKind:
		g.emitError(fmt.Errorf("groups are not supported: %s", f.FullName()))
		w.Println(`throw std::runtime_error("groups are not supported");`)
	default:
		panic(fmt.Sprintf("unexpected protoreflect.Kind: %#v", f.Kind()))
	}
}

// ----------------------------------------------------------

func collectDescriptors(parent protoreflect.Descriptor) (msgs []protoreflect.MessageDescriptor, enums []protoreflect.EnumDescriptor) {
	recurse := func(d protoreflect.Descriptor) {
		nestedMsgs, nestedEnums := collectDescriptors(d)
		msgs = append(msgs, nestedMsgs...)
		enums = append(enums, nestedEnums...)
	}
	switch d := parent.(type) {
	case protoreflect.EnumDescriptor:
		enums = append(enums, d)
	case protoreflect.MessageDescriptor:
		if d.IsMapEntry() {
			return
		}
		for i := range d.Messages().Len() {
			recurse(d.Messages().Get(i))
		}
		for i := range d.Enums().Len() {
			recurse(d.Enums().Get(i))
		}
		msgs = append(msgs, d)
	case protoreflect.FileDescriptor:
		for i := range d.Messages().Len() {
			recurse(d.Messages().Get(i))
		}
		for i := range d.Enums().Len() {
			recurse(d.Enums().Get(i))
		}
	}
	return
}

func sortMessages(msgs []protoreflect.MessageDescriptor) []protoreflect.MessageDescriptor {
	// Don't add external dependencies to the graph, only internal messages.
	internal := map[protoreflect.FullName]bool{}
	for _, msg := range msgs {
		internal[msg.FullName()] = true
	}
	return sortCyclicalGraph(msgs, func(m protoreflect.MessageDescriptor) []protoreflect.MessageDescriptor {
		var children []protoreflect.MessageDescriptor
		for i := range m.Fields().Len() {
			f := m.Fields().Get(i)
			if f.IsMap() {
				if child := f.MapKey().Message(); child != nil && internal[child.FullName()] {
					children = append(children, child)
				}
				if child := f.MapValue().Message(); child != nil && internal[child.FullName()] {
					children = append(children, child)
				}
			} else if child := f.Message(); child != nil && internal[child.FullName()] {
				children = append(children, child)
			}
		}
		return children
	})
}

// ----------------------------------------------------------

func nameToCppNamespace(file protoreflect.FileDescriptor) string {
	if ns, ok := customNamespace(file); ok {
		return ns
	}
	return strings.ReplaceAll(string(file.Package()), ".", "::")
}

func mapImport(path string) string {
	if strings.HasPrefix(path, "proto/redpanda/core/pbgen") {
		return ""
	}
	if strings.HasPrefix(path, "google/api") {
		return ""
	}
	switch path {
	case "google/protobuf/duration.proto",
		"google/protobuf/timestamp.proto":
		return "absl/time/time.h"
	case "google/protobuf/field_mask.proto":
		return "serde/protobuf/field_mask.h"
	default:
		if strings.HasPrefix(path, "google/protobuf") {
			log.Fatalf("well known import %q must be handled specially in code generation\n", path)
		}
		return path + ".h"
	}
}

func isWellKnownType(d protoreflect.Descriptor) bool {
	switch d.FullName() {
	case "google.protobuf.Duration",
		"google.protobuf.FieldMask",
		"google.protobuf.Timestamp":
		return true
	}
	return false
}

// getOneofFieldVariantIndex returns the std::variant index of a field in a oneof
func getOneofFieldVariantIndex(oneof protoreflect.OneofDescriptor, field protoreflect.FieldDescriptor) int {
	for i := range oneof.Fields().Len() {
		if oneof.Fields().Get(i).Number() == field.Number() {
			return i + 1 // +1 because of std::monostate
		}
	}
	return -1
}
