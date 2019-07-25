package ui

import (
	"io"
	"os"

	"github.com/olekukonko/tablewriter"
)

func NewRpkTable(writer io.Writer) *tablewriter.Table {
	table := tablewriter.NewWriter(os.Stdout)
	table.SetBorder(false)
	table.SetColumnSeparator("")
	table.SetHeaderLine(false)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	return table
}
