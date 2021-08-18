// Package out contains helpers to write to stdout / stderr and to exit the
// process.
package out

import (
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
)

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

func args2strings(args []interface{}) []string {
	sargs := make([]string, len(args))
	for i, arg := range args {
		sargs[i] = fmt.Sprint(arg)
	}
	return sargs
}

// TabWriter writes tab delimited output.
type TabWriter struct {
	*tabwriter.Writer
}

// NewTable returns a TabWriter that is meant to output a "table". The headers
// are uppercased and immediately printed; Print can be used to append
// additional rows.
func NewTable(headers ...string) *TabWriter {
	for i, header := range headers {
		headers[i] = strings.ToUpper(header)
	}
	t := NewTabWriter()
	t.PrintStrings(headers...)
	return t
}

// NewTabWriter returns a TabWriter. For table formatted output, prefer
// NewTable. This function is meant to be used when you may want some column
// style output (i.e., headers on the left).
func NewTabWriter() *TabWriter {
	return &TabWriter{tabwriter.NewWriter(os.Stdout, 6, 4, 2, ' ', 0)}
}

// Print stringifies the arguments and calls PrintStrings.
func (t *TabWriter) Print(args ...interface{}) {
	t.PrintStrings(args2strings(args)...)
}

// PrintStrings prints the arguments tab-delimited and newline-suffixed to the
// tab writer.
func (t *TabWriter) PrintStrings(args ...string) {
	fmt.Fprint(t.Writer, strings.Join(args, "\t")+"\n")
}

// Line prints a newline in our tab writer. This will reset tab spacing.
func (t *TabWriter) Line(sprint ...interface{}) {
	fmt.Fprint(t.Writer, append(sprint, "\n")...)
}
