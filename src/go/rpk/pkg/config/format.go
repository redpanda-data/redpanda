package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"gopkg.in/yaml.v3"
)

// OutFormatter is the output formatter for rpk.
type OutFormatter struct {
	Kind string
	t    reflect.Type
}

var marshallerMap = map[string]func(any) ([]byte, error){
	"json": json.Marshal,
	"yaml": yaml.Marshal,
}

// Format formats the input data based on the OutFormatter.Kind and returns the
// formatted string, along with a boolean flag indicating if the 'kind' is plain
// text (short) or wide text.
func (f *OutFormatter) Format(data any) (isShort, isLong bool, s string, err error) {
	if f.t != nil && reflect.TypeOf(data) != f.t {
		return false, false, "", fmt.Errorf("--help format type %s does not match serialize type %s, please open a bug report against rpk with the command you are running", f.t, reflect.TypeOf(data))
	}
	switch strings.ToLower(f.Kind) {
	case "help":
		return false, false, "", errors.New("--help should have been handled already, please open a bug report against rpk with the command you are running")
	case "json", "yaml":
		marshaller := marshallerMap[f.Kind]
		b, err := marshaller(data)
		if err != nil {
			return false, false, "", err
		}
		return false, false, string(b), nil
	case "text", "short":
		return true, false, "", nil
	case "wide", "long":
		return true, true, "", nil
	default:
		return false, false, "", fmt.Errorf("--format %q not supported", f.Kind)
	}
}

func (*OutFormatter) SupportedFormats() []string {
	return []string{"json", "yaml", "text", "wide", "help"}
}

func (f *OutFormatter) Help(t any) (string, bool) {
	if f.Kind != "help" {
		return "", false
	}
	f.t = reflect.TypeOf(t)
	s, err := formatType(t, false)
	if err != nil {
		panic(err)
	}
	return s, true
}

func formatType(t any, includeTypeName bool) (string, error) {
	types := make(map[reflect.Type]struct{})

	sb := new(strings.Builder)
	var walk func(reflect.Type) error
	var nested int
	spaces := func() string { return strings.Repeat("  ", nested) }
	walk = func(typ reflect.Type) error {
		switch typ.Kind() {
		default:
			// complex64, complex128, chan, func, interface, map, unsafepointer
			return fmt.Errorf("unsupported type %s at %s", typ.Kind(), sb.String())

		case reflect.Bool, reflect.String,
			reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
			reflect.Float32, reflect.Float64:
			fmt.Fprintf(sb, "%s", typ.Name()) // primitive types always need the type in the output
			return nil

		case reflect.Pointer:
			fmt.Fprint(sb, "*")
			return walk(typ.Elem())

		case reflect.Slice, reflect.Array:
			fmt.Fprint(sb, "[]")
			return walk(typ.Elem())

		case reflect.Map:
			fmt.Fprintf(sb, "map[%v]%v", typ.Key(), typ.Elem())
			return nil

		case reflect.Struct:
			// rest of this function
		}

		if includeTypeName {
			fmt.Fprintf(sb, "%s", typ.Name())
		}

		fmt.Fprint(sb, "{\n")
		defer fmt.Fprint(sb, spaces()+"}")

		_, seen := types[typ]
		if seen {
			fmt.Fprint(sb, spaces()+"CYCLE")
			return nil // a cycle is fine, all types known in this cycle so far are valid
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
			ytag := sf.Tag.Get("yaml")
			jtag := sf.Tag.Get("json")
			if ytag == "-" {
				if jtag != "-" {
					return fmt.Errorf("field %s.%s at %s has yaml:- but json:%q", typ.Name(), sf.Name, sb.String(), jtag)
				}
				continue
			}
			if ytag == "" {
				return fmt.Errorf("field %s.%s at %s is missing a yaml tag", typ.Name(), sf.Name, sb.String())
			}
			if jtag == "" {
				return fmt.Errorf("field %s.%s at %s is missing a json tag", typ.Name(), sf.Name, sb.String())
			}
			if jtag != ytag {
				return fmt.Errorf("field %s.%s at %s has different json:%q and yaml:%q tags", typ.Name(), sf.Name, sb.String(), jtag, ytag)
			}

			split := strings.Split(jtag, ",")
			name := split[0]
			var extra string
			if len(split) > 1 {
				extra = "," + split[1]
			}
			fmt.Fprintf(sb, "%s%s: ", spaces(), name)
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
					return fmt.Errorf("field %s.%s at %s has UnmarshalText no YamlTypeNameForTest", typ.Name(), sf.Name, sb.String())
				}
				if fnt.Type.NumIn() != 1 || fnt.Type.NumOut() != 1 || fnt.Type.Out(0).Kind() != reflect.String {
					return fmt.Errorf("field %s.%s at %s YamlTypeNameForTest: wrong signature", typ.Name(), sf.Name, sb.String())
				}
				name := reflect.New(typ).MethodByName("YamlTypeNameForTest").Call(nil)[0].String()
				fmt.Fprintf(sb, "%s", name)
			} else {
				if err := walk(sf.Type); err != nil {
					return err
				}
			}
			fmt.Fprintf(sb, "%s\n", extra)
		}
		return nil
	}

	err := walk(reflect.TypeOf(t))
	return sb.String(), err
}
