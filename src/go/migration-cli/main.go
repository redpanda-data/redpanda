package main

import (
	"fmt"
	"os"

	"github.com/redpanda-data/redpanda/src/go/migration-cli/cmd"
)

func main() {
	if err := cmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
}
