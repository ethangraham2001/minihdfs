package namenode

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/ethangraham2001/minihdfs/common"
)

func RunNameNodeProtocol() {
	nameNode := NewNameNode()

	http.HandleFunc("/create", createFileHandler(&nameNode))
	http.HandleFunc("/new_block", allocateBlockHandler(&nameNode))
	http.HandleFunc("read", readFileHandler(&nameNode))
	log.Printf("NameNode listening on port %d", common.NAMENODE_PORT)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", common.NAMENODE_PORT), nil))
}

type NameNodeReq struct {
	Filepath string `json:"filepath"`
}

func createFileHandler(nameNode *NameNode) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		body, err := io.ReadAll(r.Body)
		defer r.Body.Close()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		var req NameNodeReq
		err = json.Unmarshal(body, &req)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		err = nameNode.createFile(req.Filepath)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusCreated)
	}
}

type RequestBlockResp struct {
	NewBlockID uint64   `json:"new_block_id"`
	DataNodes  []string `json:"datanodes"`
}

func allocateBlockHandler(nameNode *NameNode) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		body, err := io.ReadAll(r.Body)
		defer r.Body.Close()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		var req NameNodeReq
		err = json.Unmarshal(body, &req)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		newBlockID, err := nameNode.allocateNewBlockToFile(req.Filepath)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		dataNodes := nameNode.allocNDataNodes(1, newBlockID)

		resp := RequestBlockResp{NewBlockID: uint64(newBlockID), DataNodes: dataNodes}
		marshalled, err := json.Marshal(resp)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write(marshalled)
	}
}

type ReadFileResp struct {
	Blocks []BlockMapping `json:"blocks"`
}

type BlockMapping struct {
	BlockID       common.BlockID `json:"block_id"`
	DataNodeAddrs []string       `json:"datanodes"`
}

func readFileHandler(nameNode *NameNode) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		body, err := io.ReadAll(r.Body)
		defer r.Body.Close()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		var req NameNodeReq
		err = json.Unmarshal(body, &req)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		blockIDs, err := nameNode.getFileBlockIDs(req.Filepath)
		mappings := make([]BlockMapping, len(blockIDs))
		for i, id := range blockIDs {
			mappings[i] = BlockMapping{
				BlockID:       id,
				DataNodeAddrs: nameNode.blockIDMap[id],
			}
		}

		resp, err := json.Marshal(ReadFileResp{Blocks: mappings})
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write(resp)
	}
}
