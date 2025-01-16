# HDFS Clone Speedrun

I got bored studying HDFS design decisions so made a stripped clone. Works
some of the time.

![MiniHDFS in action](readme-assets/minihdfs.gif)

Here we

- spin up 4 DataNodes registered with one NameNode
- Client sends a `create` request creating file `pictures/mystery` to NameNode
- Client sends a `write` request for `pictures/mystery` to NameNode for each
    1KB block composing the image _(last block is smaller)_. The NameNode 
    answers with the `BlockID` and DataNodes holding the image
- Client writes each chunk to the DataNodes _(replication handled by client,
    pipelining is future work)_
- We shut down DataNodes 1 and 4 _(which hold different data in this case, 
    DataNode selection is based on round-robin)_.
- Client sends a `read` request to NameNode, who tells it which `BlockIDs` to
    read from which DataNodes.
- Client reads the blocks.
- We open the resulting image retrieved from the individual chunks in the
    DataNodes.

EZPZ.
