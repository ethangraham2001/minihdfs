// Package client contains the client program
package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/ethangraham2001/minihdfs/common"
)

func RunClientCommand(opts []string) {
	fmt.Print(opts)
	if len(opts) < 4 {
		panic("invalid command")
	}

	cmd := opts[2]
	filepath := opts[3]

	switch cmd {
	case "read":
		handleRead(filepath)
	case "create":
		handleCreate(filepath)
	case "new_block":
		handleNewBlock(filepath)
	}
}

func handleRead(filepath string) {
	request := common.NameNodeReq{Filepath: filepath}
	buff, err := json.Marshal(request)
	resp, err := http.NewRequest(http.MethodGet, common.NAMENODE_ADDR+"/read", bytes.NewBuffer(buff))
	if err != nil {
		panic("failed to get server resp")
	}
	fmt.Print(resp)
}

func handleCreate(filepath string) {
	request := common.NameNodeReq{Filepath: filepath}
	buff, err := json.Marshal(request)
	resp, err := http.NewRequest(http.MethodPost, common.NAMENODE_ADDR+"/read", bytes.NewBuffer(buff))
	if err != nil {
		panic("failed to get server resp")
	}
	fmt.Print(resp)
}

func handleNewBlock(filepath string) {
	request := common.NameNodeReq{Filepath: filepath}
	buff, err := json.Marshal(request)
	resp, err := http.NewRequest(http.MethodPost, common.NAMENODE_ADDR+"/read", bytes.NewBuffer(buff))
	if err != nil {
		panic("failed to get server resp")
	}
	fmt.Print(resp)
}
