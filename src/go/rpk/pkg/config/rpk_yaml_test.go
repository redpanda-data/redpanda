// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package config

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestRpkYamlVersion(t *testing.T) {
	types := make(map[reflect.Type]struct{})

	sb := new(strings.Builder)
	var walk func(reflect.Type) bool
	var nested int
	spaces := func() string { return strings.Repeat("  ", nested) }
	walk = func(typ reflect.Type) bool {
		fmt.Fprintf(sb, "%s", typ.Name())

		switch typ.Kind() {
		default:
			t.Errorf("unsupported type %s at %s", typ.Kind(), sb.String())
			return false

		case reflect.Bool, reflect.String,
			reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return true

		case reflect.Pointer:
			fmt.Fprintf(sb, "*")
			return walk(typ.Elem())

		case reflect.Slice:
			fmt.Fprintf(sb, "[]")
			return walk(typ.Elem())

		case reflect.Struct:
			// rest of this function
		}

		fmt.Fprintf(sb, "{\n")
		defer fmt.Fprintf(sb, spaces()+"}")

		_, seen := types[typ]
		if seen {
			fmt.Fprintf(sb, spaces()+"CYCLE")
			return true // a cycle is fine, all types known in this cycle so far are valid
		}
		types[typ] = struct{}{}
		defer delete(types, typ)

		nested++
		defer func() { nested-- }()
		for i := 0; i < typ.NumField(); i++ {
			sf := typ.Field(i)
			if !sf.IsExported() {
				continue
			}
			tag := sf.Tag.Get("yaml")
			if tag == "-" {
				continue
			}
			if tag == "" {
				t.Errorf("field %s.%s at %s is missing a yaml tag", typ.Name(), sf.Name, sb.String())
				return false
			}

			fmt.Fprintf(sb, "%s%s: ", spaces(), sf.Name)
			addr := sf.Type
			typ := sf.Type
			if addr.Kind() != reflect.Ptr {
				addr = reflect.PointerTo(addr)
			}
			for typ.Kind() == reflect.Ptr {
				typ = typ.Elem()
			}

			_, hasUnmarshalText := addr.MethodByName("UnmarshalText")
			if hasUnmarshalText {
				fnt, ok := addr.MethodByName("YamlTypeNameForTest")
				if !ok {
					t.Errorf("field %s.%s at %s has UnmarshalText no YamlTypeNameForTest", typ.Name(), sf.Name, sb.String())
					return false
				}
				if fnt.Type.NumIn() != 1 || fnt.Type.NumOut() != 1 || fnt.Type.Out(0).Kind() != reflect.String {
					t.Errorf("field %s.%s at %s YamlTypeNameForTest: wrong signature", typ.Name(), sf.Name, sb.String())
					return false
				}
				name := reflect.New(typ).MethodByName("YamlTypeNameForTest").Call(nil)[0].String()
				fmt.Fprintf(sb, "%s", name)
			} else {
				if !walk(sf.Type) {
					return false
				}
			}
			fmt.Fprintf(sb, " `yaml:\"%s\"`\n", tag)
		}
		return true
	}

	ok := walk(reflect.TypeOf(RpkYaml{}))
	if !ok {
		return
	}

	sha := sha256.Sum256([]byte(sb.String()))
	shastr := hex.EncodeToString(sha[:])

	const (
		v1sha = "0f2ca9327b6ea07b93e37909c7c2c4eb1497d953038386443d6bac553f36b4fa" // 23-07-13
	)

	if shastr != v1sha {
		t.Errorf("rpk.yaml type shape has changed (got sha %s != exp %s, if fields were reordered, update the valid v1 sha, otherwise bump the rpk.yaml version number", shastr, v1sha)
		t.Errorf("current shape:\n%s\n", sb.String())
	}
}
