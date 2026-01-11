package main

import (
	"os"

	"github.com/redpanda-data/redpanda/tests/go/client-swarm/cmd"
)

func main() {
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
