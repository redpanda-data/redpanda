// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package out

import (
	"fmt"
	"io"
	"reflect"
	"strings"
)

// Formatter is the interface satisfied by *config.OutFormatter. Accepting an
// interface here avoids the import cycle that would arise from importing
// pkg/config directly (pkg/config already imports pkg/out).
//
// Because config.OutFormatter.Format has a pointer receiver, callers pass
// the address: out.Render(&p.Formatter, os.Stdout, data).
type Formatter interface {
	// Format serialises v according to the chosen output kind and returns:
	//   isText  – true when the caller should render as a text table
	//   isWide  – true when the text table should include wide-only columns
	//   s       – the serialised string for non-text formats (JSON / YAML)
	//   err     – non-nil when the format kind is unsupported or marshalling fails
	Format(v any) (isText, isWide bool, s string, err error)
}

// Render emits v in the format selected by f, writing to w.
// The output shape is determined by reflecting on v:
//
//   - slice / array → list (header row + one row per element)
//   - struct with at least one field tagged `header:"..."` → composite sections
//   - any other struct → object (LABEL / VALUE two-column layout)
//
// JSON / YAML formats are handed to f.Format and written as-is.
// If v is a nil pointer, text output is empty; JSON / YAML serialise nil
// according to the chosen encoder (typically `null`).
func Render(f Formatter, w io.Writer, v any) error {
	isText, isWide, s, err := f.Format(v)
	if !isText {
		if err != nil {
			return err
		}
		fmt.Fprintln(w, s)
		return nil
	}
	return renderText(w, v, isWide)
}

func renderText(w io.Writer, v any, isWide bool) error {
	rv := reflect.ValueOf(v)
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return nil
		}
		rv = rv.Elem()
	}
	switch rv.Kind() {
	case reflect.Slice, reflect.Array:
		return renderList(w, rv, isWide)
	case reflect.Struct:
		if hasHeaderTag(rv.Type()) {
			return renderComposite(w, rv, isWide)
		}
		return renderObject(w, rv, isWide)
	default:
		return fmt.Errorf("out.Render: unsupported kind %s", rv.Kind())
	}
}

// listColumn is a resolved text column derived from a struct field's table tag.
type listColumn struct {
	header     string
	fieldIndex int
	wide       bool
	omitempty  bool
}

// listColumns extracts the ordered set of table columns from a struct type.
func listColumns(t reflect.Type) []listColumn {
	var cols []listColumn
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		tag := parseTableTag(f.Tag.Get("table"))
		if !tag.present || tag.skip {
			continue
		}
		cols = append(cols, listColumn{
			header:     tag.header,
			fieldIndex: i,
			wide:       tag.wide,
			omitempty:  tag.omitempty,
		})
	}
	return cols
}

// renderList renders a slice or array of structs as a text table.
func renderList(w io.Writer, slice reflect.Value, isWide bool) error {
	elemT := slice.Type().Elem()
	for elemT.Kind() == reflect.Pointer {
		elemT = elemT.Elem()
	}
	if elemT.Kind() != reflect.Struct {
		return fmt.Errorf("out.Render: list element must be a struct, got %s", elemT.Kind())
	}

	// If elements have header: tags, render each as a titled composite block.
	if hasHeaderTag(elemT) {
		for i := 0; i < slice.Len(); i++ {
			if i > 0 {
				fmt.Fprintln(w)
			}
			elem := slice.Index(i)
			for elem.Kind() == reflect.Pointer {
				if elem.IsNil() {
					break
				}
				elem = elem.Elem()
			}
			if elem.Kind() != reflect.Struct {
				continue
			}
			if err := renderComposite(w, elem, isWide); err != nil {
				return err
			}
		}
		return nil
	}

	all := listColumns(elemT)
	visible := visibleListColumns(all, isWide)
	headers := make([]string, len(visible))
	for i, c := range visible {
		headers[i] = c.header
	}
	tw := NewTableTo(w, headers...)
	defer tw.Flush()
	for i := 0; i < slice.Len(); i++ {
		elem := slice.Index(i)
		for elem.Kind() == reflect.Pointer {
			if elem.IsNil() {
				break
			}
			elem = elem.Elem()
		}
		if elem.Kind() != reflect.Struct {
			// Nil pointer element — render a blank row so the row count still matches.
			tw.Print(make([]any, len(visible))...)
			continue
		}
		cells := make([]any, len(visible))
		for j, c := range visible {
			cells[j] = cellValue(elem.Field(c.fieldIndex))
		}
		tw.Print(cells...)
	}
	return nil
}

// renderObject renders a single struct as a two-column LABEL / VALUE layout,
// using the same table: tags that drive list columns.
func renderObject(w io.Writer, v reflect.Value, isWide bool) error {
	t := v.Type()
	tw := NewTabWriterTo(w)
	defer tw.Flush()
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		tag := parseTableTag(f.Tag.Get("table"))
		if !tag.present || tag.skip {
			continue
		}
		if tag.wide && !isWide {
			continue
		}
		fv := v.Field(i)
		if tag.omitempty && fv.IsZero() {
			continue
		}
		tw.PrintColumn(tag.header, cellValue(fv))
	}
	return nil
}

// cellValue extracts a field value appropriate for table rendering. Non-nil
// pointers are dereferenced so fmt.Sprint formats the pointee, not the address;
// nil pointers render as empty string.
func cellValue(v reflect.Value) any {
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return ""
		}
		v = v.Elem()
	}
	return v.Interface()
}

func visibleListColumns(all []listColumn, isWide bool) []listColumn {
	if isWide {
		return all
	}
	out := make([]listColumn, 0, len(all))
	for _, c := range all {
		if !c.wide {
			out = append(out, c)
		}
	}
	return out
}

// tableTag is a parsed `table:"..."` struct tag.
type tableTag struct {
	header    string
	wide      bool
	omitempty bool
	skip      bool // tag was "-"
	present   bool // tag was set (not absent)
}

func parseTableTag(s string) tableTag {
	if s == "" {
		return tableTag{}
	}
	if s == "-" {
		return tableTag{present: true, skip: true}
	}
	parts := strings.Split(s, ",")
	t := tableTag{present: true, header: parts[0]}
	for _, p := range parts[1:] {
		switch p {
		case "wide":
			t.wide = true
		case "omitempty":
			t.omitempty = true
		}
	}
	return t
}

// headerTag is a parsed `header:"..."` struct tag used to mark composite sections.
type headerTag struct {
	title     string
	omitempty bool
	present   bool
}

func parseHeaderTag(s string) headerTag {
	if s == "" {
		return headerTag{}
	}
	parts := strings.Split(s, ",")
	h := headerTag{present: true, title: parts[0]}
	for _, p := range parts[1:] {
		if p == "omitempty" {
			h.omitempty = true
		}
	}
	return h
}

func hasHeaderTag(t reflect.Type) bool {
	for i := 0; i < t.NumField(); i++ {
		if parseHeaderTag(t.Field(i).Tag.Get("header")).present {
			return true
		}
	}
	return false
}

func renderComposite(w io.Writer, v reflect.Value, isWide bool) error {
	t := v.Type()
	first := true
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		h := parseHeaderTag(f.Tag.Get("header"))
		if !h.present {
			continue
		}
		fv := v.Field(i)
		if h.omitempty && isSectionEmpty(fv) {
			continue
		}
		if !first {
			fmt.Fprintln(w)
		}
		first = false
		fmt.Fprintln(w, h.title)
		fmt.Fprintln(w, strings.Repeat("=", len(h.title)))
		if err := renderCompositeField(w, fv, isWide); err != nil {
			return err
		}
	}
	return nil
}

// isSectionEmpty reports whether v should be treated as "empty" for the
// composite section omitempty contract: nil pointer, zero struct, or
// empty slice / array / map / string.
//
// For struct sections, emptiness is determined by reflect.Value.IsZero(),
// which returns true only when every field is the zero value. Use
// header:",omitempty" on a struct section only when a fully-zero struct
// is meaningless — a struct with a single bool or int field where the
// zero value is a valid rendered state would be silently suppressed.
func isSectionEmpty(v reflect.Value) bool {
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return true
		}
		v = v.Elem()
	}
	switch v.Kind() {
	case reflect.Slice, reflect.Array, reflect.Map, reflect.Chan, reflect.String:
		return v.Len() == 0
	}
	return v.IsZero()
}

// renderCompositeField dispatches on the section value's kind, mirroring
// renderText but without the JSON/YAML branch (the composite is already in
// text mode) and without wrapping the whole thing in another composite.
func renderCompositeField(w io.Writer, v reflect.Value, isWide bool) error {
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return nil
		}
		v = v.Elem()
	}
	switch v.Kind() {
	case reflect.Slice, reflect.Array:
		return renderList(w, v, isWide)
	case reflect.Struct:
		if hasHeaderTag(v.Type()) {
			return renderComposite(w, v, isWide)
		}
		return renderObject(w, v, isWide)
	default:
		return fmt.Errorf("out.Render: unsupported section kind %s", v.Kind())
	}
}
