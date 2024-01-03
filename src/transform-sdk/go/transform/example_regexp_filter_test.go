// Copyright 2023 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package transform_test

import (
	"os"
	"regexp"

	"github.com/redpanda-data/redpanda/src/transform-sdk/go/transform"
)

var (
	re         *regexp.Regexp = nil
	checkValue bool           = false
)

// This example shows a filter that uses a regexp to filter records from
// one topic into another. The filter can be determined when the transform
// is deployed by using environment variables to specify the pattern.
func Example_regularExpressionFilter() {
	// setup the regexp
	pattern, ok := os.LookupEnv("PATTERN")
	if !ok {
		panic("Missing PATTERN variable")
	}
	re = regexp.MustCompile(pattern)
	mk, ok := os.LookupEnv("MATCH_VALUE")
	checkValue = ok && mk == "1"

	transform.OnRecordWritten(doRegexFilter)
}

func doRegexFilter(e transform.WriteEvent, w transform.RecordWriter) error {
	var b []byte
	if checkValue {
		b = e.Record().Value
	} else {
		b = e.Record().Key
	}
	if b == nil {
		return nil
	}
	pass := re.Match(b)
	if pass {
		return w.Write(e.Record())
	} else {
		return nil
	}
}
