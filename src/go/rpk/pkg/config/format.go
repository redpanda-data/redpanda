package config

import (
	"encoding/json"
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

// OutFormatter is the output formatter for rpk.
type OutFormatter struct {
	Kind string
}

var marshallerMap = map[string]func(any) ([]byte, error){
	"json": json.Marshal,
	"yaml": yaml.Marshal,
}

// Format formats the input data based on the OutFormatter.Kind and returns the
// formatted string, along with a boolean flag indicating if the 'kind' is plain
// text (short) or wide text.
func (f *OutFormatter) Format(data any) (isShort, isLong bool, s string, err error) { // TODO add isWide
	switch strings.ToLower(f.Kind) {
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
		return false, true, "", nil
	default:
		return false, false, "", fmt.Errorf("--format %q not supported", f.Kind)
	}
}

func (*OutFormatter) SupportedFormats() []string {
	return []string{"json", "yaml", "text", "wide"}
}
