# RAFT Leader – Follower (Go)
## Overview
This repo implements a simple **leader–follower replicated log** using the RAFT protocol (via `hashicorp/raft`).  
Cluster runs with **4 nodes** (static, no reconfig).  
Each node stores its log in **SQLite (WAL mode)** and exposes a HTTP Key–Value API.  

Main goals:  
- automatic leader election  
- client can write to *any* node (follower forwards to leader)  
- crash safety with persisted entries  
- avoid duplicate apply using **idempotency_key**  

---

## Features
- Leader election (random timeout ~250–500 ms, heartbeat 50ms)  
- Followers forward writes to current leader  
- Log replication with batch ,pipeline  
- Backpressure - bounded queue, no infinite memory

---

## Deduplication
idempotency_key
# Used LRU for Cache Mechanisum
When applying:  
1. Check if key already exists in metadata table  
2. If yes, skip apply (return success anyway)  
3. Else, insert + apply in same SQLite transaction  
This ensures same command isn’t applied twice even if client retries or leader fails.  
We also keep a small in-mem LRU cache to avoid hitting DB all the time.  
---

##  Run Instructions
Redirect to project
rm -rf data
# Start the first node (bootstrap leader)
go run main.go -id node1 -raft-addr 127.0.0.1:12000 -http-addr 127.0.0.1:9000 -data-dir data/node1
# Start other nodes and join to cluster
go run main.go -id node2 -raft-addr 127.0.0.1:12001 -http-addr 127.0.0.1:9001 -data-dir data/node2 -join 127.0.0.1:9000
go run main.go -id node3 -raft-addr 127.0.0.1:12002 -http-addr 127.0.0.1:9002 -data-dir data/node3 -join 127.0.0.1:9000
go run main.go -id node4 -raft-addr 127.0.0.1:12003 -http-addr 127.0.0.1:9003 -data-dir data/node4 -join 127.0.0.1:9000


# Test Set and Get
curl -X POST http://127.0.0.1:9001/set \
    -d '{"key":"Name","value":"PremKUmar"}' \
    -H "Content-Type: application/json"
curl http://127.0.0.1:9001/get?key=Name


