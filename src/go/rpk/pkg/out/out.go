// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Package out contains helpers to write to stdout / stderr and to exit the
// process.
package out

import (
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"text/tabwriter"

	"github.com/AlecAivazis/survey/v2"
	"github.com/twmb/franz-go/pkg/kadm"
)

// Confirm prompts the user to confirm the formatted message and returns the
// confirmation result or an error.
func Confirm(msg string, args ...interface{}) (bool, error) {
	var confirmation bool
	return confirmation, survey.AskOne(&survey.Confirm{
		Message: fmt.Sprintf(msg, args...),
		Default: true,
	}, &confirmation)
}

// Pick prompts the user to pick one of many options, returning the selected
// option or an error.
func Pick(options []string, msg string, args ...interface{}) (string, error) {
	var selected int
	err := survey.AskOne(&survey.Select{
		Message: fmt.Sprintf(msg, args...),
		Options: options,
	}, &selected)
	if err != nil {
		return "", err
	}
	return options[selected], nil
}

// Die formats the message with a suffixed newline to stderr and exits the
// process with 1.
func Die(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}

// MaybeDie calls Die if err is non-nil.
func MaybeDie(err error, msg string, args ...interface{}) {
	if err != nil {
		Die(msg, args...)
	}
}

// MaybeDieErr calls Die if err is non-nil, with just the err as the message.
func MaybeDieErr(err error) {
	if err != nil {
		Die("%v", err)
	}
}

// Exit formats the message with a suffixed newline to stdout and exist
// successfully with 0.
func Exit(msg string, args ...interface{}) {
	fmt.Printf(msg+"\n", args...)
	os.Exit(0)
}

// HandleShardError prints a message and potentially exits depending on the
// inner error. If the error is a shard error and not everything failed, this
// allows the cli to continue.
func HandleShardError(name string, err error) {
	var se *kadm.ShardErrors
	var ae *kadm.AuthError
	switch {
	case err == nil:

	case errors.As(err, &se):
		if se.AllFailed {
			fmt.Printf("all %d %s request failures, first error: %s\n", len(se.Errs), se.Name, se.Errs[0].Err)
			os.Exit(1)
		}
		fmt.Printf("%d %s request failures, first error: %s\n", len(se.Errs), se.Name, se.Errs[0].Err)

	case errors.As(err, &ae):
		fmt.Printf("%s authorization problem: %s\n", name, err)
		os.Exit(1)

	default:
		fmt.Printf("unable to issue %s request: %s\n", name, err)
		os.Exit(1)
	}
}

func args2strings(args []interface{}) []string {
	sargs := make([]string, len(args))
	for i, arg := range args {
		sargs[i] = fmt.Sprint(arg)
	}
	return sargs
}

// Section prints header in uppercase, followed by a line of =.
func Section(header string) {
	fmt.Println(strings.ToUpper(header))
	fmt.Println(strings.Repeat("=", len(header)))
}

// SectionFn prints header in uppercase, followed by a line of = as long as the
// header, and then calls fn.
func SectionFn(header string, fn func()) {
	Section(header)
	fn()
}

// TabWriter writes tab delimited output.
type TabWriter struct {
	*tabwriter.Writer
}

// NewTable returns a TabWriter that is meant to output a "table". The headers
// are uppercased and immediately printed; Print can be used to append
// additional rows.
func NewTable(headers ...string) *TabWriter {
	return NewTableTo(os.Stdout, headers...)
}

// NewTableTo is NewTable writing to w.
func NewTableTo(w io.Writer, headers ...string) *TabWriter {
	var iheaders []interface{}
	for _, header := range headers {
		iheaders = append(iheaders, strings.ToUpper(header))
	}
	t := NewTabWriterTo(w)
	t.Print(iheaders...)
	return t
}

// NewTabWriter returns a TabWriter. For table formatted output, prefer
// NewTable. This function is meant to be used when you may want some column
// style output (i.e., headers on the left).
func NewTabWriter() *TabWriter {
	return NewTabWriterTo(os.Stdout)
}

// NewTabWriterTo returns a TabWriter that writes to w.
func NewTabWriterTo(w io.Writer) *TabWriter {
	return &TabWriter{tabwriter.NewWriter(w, 6, 4, 2, ' ', 0)}
}

// Print stringifies the arguments and prints them tab-delimited and
// newline-suffixed to the tab writer.
func (t *TabWriter) Print(args ...interface{}) {
	t.PrintStrings(args2strings(args)...)
}

// PrintStrings prints args tab-delimited and newline-suffixed to the tab
// writer.
func (t *TabWriter) PrintStrings(args ...string) {
	fmt.Fprint(t.Writer, strings.Join(args, "\t")+"\n")
}

// PrintStructFields prints the values stored in fields in a struct.
//
// This is a function meant to allow users to define helper structs that have
// defined field types. Rather than passing arguments blindly to Print, you can
// put those arguments in your helper struct to *ensure* there are no breaking
// output changes if any field changes types.
func (t *TabWriter) PrintStructFields(s interface{}) {
	v := reflect.ValueOf(s)
	if v.Kind() == reflect.Ptr {
		v = reflect.Indirect(v)
	}
	if v.Kind() != reflect.Struct {
		panic("PrintStructFields not called on a *reflect.Struct nor a reflect.Struct!")
	}
	var fields []interface{}
	typ := v.Type()
	for i := 0; i < v.NumField(); i++ {
		if typ.Field(i).PkgPath != "" {
			panic("PrintStructFields used on type with private fields!")
		}
		fields = append(fields, v.Field(i).Interface())
	}
	t.Print(fields...)
}

// PrintColumn is the same as Print, but prints header uppercased as the first
// argument.
func (t *TabWriter) PrintColumn(header string, args ...interface{}) {
	header = strings.ToUpper(header)
	t.Print(append([]interface{}{header}, args...)...)
}

// Line prints a newline in our tab writer. This will reset tab spacing.
func (t *TabWriter) Line(sprint ...interface{}) {
	fmt.Fprint(t.Writer, append(sprint, "\n")...)
}
