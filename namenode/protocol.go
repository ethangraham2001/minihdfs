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
	http.HandleFunc("/new_block", newBlockHandler(&nameNode))
	http.HandleFunc("/read", readFileHandler(&nameNode))
	http.HandleFunc("/register", registerHandler(&nameNode))
	log.Printf("NameNode listening on port %d", common.NAMENODE_PORT)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", common.NAMENODE_PORT), nil))
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

		var req common.NameNodeReq
		err = json.Unmarshal(body, &req)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		err = nameNode.createFile(req.Filepath)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		log.Printf("created file %s", req.Filepath)
		w.WriteHeader(http.StatusCreated)
	}
}

func newBlockHandler(nameNode *NameNode) http.HandlerFunc {
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

		var req common.NameNodeReq
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
		dataNodes := nameNode.allocNDataNodes(common.REPLICATION_FACTOR, newBlockID)

		resp := common.RequestBlockResp{NewBlockID: uint64(newBlockID), DataNodes: dataNodes}
		marshalled, err := json.Marshal(resp)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		log.Printf("new block %d for file %s", newBlockID, req.Filepath)
		w.WriteHeader(http.StatusOK)
		w.Write(marshalled)
	}
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

		var req common.NameNodeReq
		err = json.Unmarshal(body, &req)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		blockIDs, err := nameNode.getFileBlockIDs(req.Filepath)
		if err != nil {
			log.Printf("no blocks found for %s", req.Filepath)
			w.WriteHeader(http.StatusNotFound)
			return
		}

		log.Printf("datanodes: %#v", nameNode.dataNodeAddrs)
		mappings := make([]common.BlockMapping, len(blockIDs))
		for i, id := range blockIDs {
			mappings[i] = common.BlockMapping{
				BlockID:       id,
				DataNodeAddrs: nameNode.blockIDMap[id],
			}
		}

		resp, err := json.Marshal(common.ReadFileResp{Blocks: mappings})
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write(resp)
	}
}

func registerHandler(nameNode *NameNode) http.HandlerFunc {
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

		var req common.RegisterReq
		err = json.Unmarshal(body, &req)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		address := fmt.Sprintf("http://localhost:%d", req.Port)
		nameNode.dataNodeAddrs = append(nameNode.dataNodeAddrs, address)

		log.Printf("%s registered", address)
		w.WriteHeader(http.StatusOK)
	}
}
