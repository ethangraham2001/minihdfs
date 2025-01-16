// Package main is the main package.
package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/ethangraham2001/minihdfs/datanode"
)

const USAGE string = `
usage:
	./minihdfs [opts]

options:
	--DataNode run the DataNode protocol
	--NameNode run the NameNode protocol
	--port=[int] define port number
`

func main() {
	if len(os.Args) < 3 {
		panic("invalid arguments")
	}

	port, err := strconv.Atoi(os.Args[2])
	if err != nil {
		panic("invalid port num")
	}

	fmt.Print(os.Args)

	if os.Args[1] == "DataNode" {
		datanode.RunDataNodeProtocol(port)
	} else if os.Args[1] == "NameNode" {
		// run NameNode protocol
	} else {
		panic("invalid protocol")
	}
}
