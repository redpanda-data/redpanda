package commands

import (
	"bufio"
)

type Command interface {
	Execute() error
	RenderScript(*bufio.Writer) error
}
