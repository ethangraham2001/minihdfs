package datanode

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"

	"github.com/ethangraham2001/minihdfs/common"
)

// RunDataNodeProtocol listens for connections at the provided port number
// and interacts with a DataNode
func RunDataNodeProtocol(port int) {
	dataNode := NewVolatileDataNode()
	handlerFunc := newRequestHandler(dataNode)
	log.Printf("DataNode listening at port %d", port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", port), handlerFunc))
}

func newRequestHandler(dataNode DataNode) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path[1:]
		parsed, err := strconv.ParseUint(path, 10, 64)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		blockID := common.BlockID(parsed)
		if r.Method == http.MethodGet {
			if !dataNode.store.ContainsBlock(blockID) {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			block, err := dataNode.ReadBlock(blockID)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
			w.Write(block)
		} else if r.Method == http.MethodPost {
			block, err := io.ReadAll(r.Body)
			defer r.Body.Close()
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			err = dataNode.WriteBlock(blockID, block)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
			return
		} else {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
	}
}
