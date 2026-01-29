// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package profile

import (
	"reflect"
	"testing"

	"github.com/fatih/color"
)

func TestSplitPromptParens(t *testing.T) {
	for _, test := range []struct {
		in     string
		exp    []string
		expErr bool
	}{
		{"", nil, false},
		{"()", nil, false},
		{" ( ) ", nil, false},
		{" ", nil, false},
		{` blue red ,gr\een,\"text"`, []string{`blue red ,gr\een,\"text"`}, false}, // we just parse parens, not contents
		{` unexpected end paren) `, nil, true},
		{` (unclosed open paren`, nil, true},
		{` ("\)")`, []string{`"\)"`}, false},
		{` ( ) asdf `, nil, true},
		{` ( ) (red blue  green ) (bog)`, []string{"red blue  green", "bog"}, false},
	} {
		t.Run(test.in, func(t *testing.T) {
			got, err := splitPromptParens(test.in)
			gotErr := err != nil
			if gotErr != test.expErr {
				t.Errorf("got err? %v (%v), exp %v", gotErr, err, test.expErr)
				return
			}
			if !reflect.DeepEqual(got, test.exp) {
				t.Errorf("got %v != exp %v", got, test.exp)
			}
		})
	}
}

func TestParsePrompt(t *testing.T) {
	const name = "foo"
	for _, test := range []struct {
		in      string
		expText string
		expAttr []color.Attribute
		expRaw  bool
		expErr  bool
	}{
		{"", "", nil, false, false}, // empty is ok
		{`blue , green , bg-hi-blue, "%n"`, "foo", []color.Attribute{color.FgBlue, color.FgGreen, color.BgHiBlue}, false, false}, // somewhat complete
		{`\"blue\"`, "", nil, false, true},          // backslash only allowed in quoted str
		{` "prompt" `, "prompt", nil, false, false}, // simple
		{`unknown-thing `, "", nil, false, true},    // unknown keyword stripped
		{`blue	green red, bg-hi-blue`, "foo", []color.Attribute{color.FgBlue, color.FgGreen, color.FgRed, color.BgHiBlue}, false, false}, // attr at end is kept, name is added by default
		{` " %n " `, " foo ", nil, false, false},   // name swapped in
		{`"\\\%%%%n"`, "\\%%n", nil, false, false}, // escaping works, and %% works
		{`"text1" "text2"`, "", nil, false, true},  // one quoted string
		{`b"text"`, "", nil, false, true},          // unexpected text before quote
		{`blue unknown`, "", nil, false, true},     // unknown attr at end
		{`"%u"`, "", nil, false, true},             // unknown % escape

		// raw modifier tests
		{`raw, "%n"`, "foo", nil, true, false},                                   // raw with format string
		{`raw`, "foo", nil, true, false},                                         // raw alone defaults to name
		{`raw, blue, "%n"`, "foo", []color.Attribute{color.FgBlue}, true, false}, // raw with colors (raw wins, colors parsed but not applied)
		{`RAW, "%n"`, "foo", nil, true, false},                                   // case-insensitive
		{`blue, raw, "%n"`, "foo", []color.Attribute{color.FgBlue}, true, false}, // raw can appear anywhere
	} {
		t.Run(test.in, func(t *testing.T) {
			gotText, gotAttr, gotRaw, err := parsePrompt(test.in, name)
			gotErr := err != nil
			if gotErr != test.expErr {
				t.Errorf("got err? %v (%v), exp %v", gotErr, err, test.expErr)
				return
			}
			if gotText != test.expText {
				t.Errorf("got text %v != exp %v", gotText, test.expText)
			}
			if !reflect.DeepEqual(gotAttr, test.expAttr) {
				t.Errorf("got attr %v != exp %v", gotAttr, test.expAttr)
			}
			if gotRaw != test.expRaw {
				t.Errorf("got raw %v != exp %v", gotRaw, test.expRaw)
			}
		})
	}
}
