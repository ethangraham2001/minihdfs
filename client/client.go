// Package client contains the client program
package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/ethangraham2001/minihdfs/common"
)

func RunClientCommand(opts []string) {
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
	case "write":
		handleWrite(opts[4], filepath)
	}
}

func handleRead(filepath string) {
	request := common.NameNodeReq{Filepath: filepath}
	buff, err := json.Marshal(request)
	req, err := http.NewRequest(http.MethodGet, common.NAMENODE_ADDR+"/read", bytes.NewBuffer(buff))
	if err != nil {
		log.Fatal(err)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("RESP = %s\n", resp.Status)

	var readFileResp common.ReadFileResp
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Fatal(err)
	}

	err = json.Unmarshal(body, &readFileResp)
	if err != nil {
		log.Fatal(err)
	}

	bytesBuff := []byte{}
	for _, mapping := range readFileResp.Blocks {
		for _, addr := range mapping.DataNodeAddrs {
			url := fmt.Sprintf("%s/%d", addr, mapping.BlockID)
			resp, err := http.Get(url)
			if err != nil {
				log.Printf("no response from %s", url)
				continue
			}
			data, err := io.ReadAll(resp.Body)
			if err != nil {
				log.Printf("unable to read data %s", url)
				continue
			}
			bytesBuff = append(bytesBuff, data...)
			break
		}
	}

	log.Printf("read data is of size %d B", len(bytesBuff))

	err = os.WriteFile(time.Now().String(), bytesBuff, 0644)
	if err != nil {
		log.Fatalf("failed to write output")
		return
	}
	log.Printf("Successfully wrote output")
}

func handleCreate(filepath string) {
	request := common.NameNodeReq{Filepath: filepath}
	buff, err := json.Marshal(request)
	req, err := http.NewRequest(http.MethodPost, common.NAMENODE_ADDR+"/create", bytes.NewBuffer(buff))
	if err != nil {
		log.Fatal(err)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("RESP = %s\n", resp.Status)
}

func handleNewBlock(filepath string) (common.BlockID, []string) {
	request := common.NameNodeReq{Filepath: filepath}
	buff, err := json.Marshal(request)
	req, err := http.NewRequest(http.MethodPost, common.NAMENODE_ADDR+"/new_block", bytes.NewBuffer(buff))
	if err != nil {
		log.Fatal(err)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Fatal(err)
	}

	var newBlockInfo common.RequestBlockResp
	err = json.Unmarshal(body, &newBlockInfo)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("blockID: %d\n", newBlockInfo.NewBlockID)
	for _, addr := range newBlockInfo.DataNodes {
		fmt.Printf("\t%s\n", addr)
	}
	return common.BlockID(newBlockInfo.NewBlockID), newBlockInfo.DataNodes
}

func handleWrite(localFilepath, remoteFilepath string) {
	file, err := os.Open(localFilepath)
	if err != nil {
		log.Fatal(err)
	}
	data, err := io.ReadAll(file)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("write data is of size %d B", len(data))

	numBlocks := len(data) / int(common.BLOCK_SIZE)
	if len(data)%int(common.BLOCK_SIZE) != 0 {
		numBlocks += 1
	}

	for len(data) > 0 {
		interval := min(len(data), int(common.BLOCK_SIZE))
		slice := data[:interval]
		id, addrs := handleNewBlock(remoteFilepath)
		for _, addr := range addrs {
			url := fmt.Sprintf("%s/%d", addr, id)
			_, err := http.Post(url, "", bytes.NewBuffer(slice))
			if err != nil {
				log.Printf("unable to write %d to %s", id, addr)
			}
		}
		data = data[interval:]
	}
}
