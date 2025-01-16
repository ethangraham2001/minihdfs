// Package main is the main package.
package main

import (
	"os"
	"strconv"

	"github.com/ethangraham2001/minihdfs/client"
	"github.com/ethangraham2001/minihdfs/datanode"
	"github.com/ethangraham2001/minihdfs/namenode"
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
	if os.Args[1] == "DataNode" {
		if len(os.Args) < 3 {
			panic("need to provide port number for datanode")
		}
		port, err := strconv.Atoi(os.Args[2])
		if err != nil {
			panic("invalid port num")
		}
		datanode.RunDataNodeProtocol(port)
	} else if os.Args[1] == "NameNode" {
		namenode.RunNameNodeProtocol()
	} else if os.Args[1] == "Client" {
		client.RunClientCommand(os.Args)
	} else {
		panic("invalid protocol")
	}
}
