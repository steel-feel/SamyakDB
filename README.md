# G-Dist

G-Dist is a distributed, strongly consistent Key-Value database designed for high availability and horizontal scalability. It employs a multi-layered architecture using Gossip for membership, Consistent Hashing for data distribution, and Raft for consensus-backed replication.

## Architecture
*   **Strong Consistency (CP):** Guarantee that all reads return the most recent write within a shard.
*   **Horizontal Scalability:** Ability to add nodes with minimal data reshuffling.
*   **Decentralized Discovery:** No "master" node for cluster membership.
*   **Fault Tolerance:** Automatic leader re-election and data recovery after node failure.

## Getting Started

### Prerequisites
- Go 1.25 or later

### Building the Project
Build the server and the CLI:
```bash
go build -o g-dist-server ./cmd/server
go build -o g-dist-cli ./cmd/cli
```

### Running the Server
Start the G-Dist server. Each node requires a unique name.

**Start the first node:**
```bash
./g-dist-server -name node1
```

**Start additional nodes and join the cluster:**
```bash
./g-dist-server -name node2 -bind-addr :50052 -gossip-addr :7947 -join-addrs 127.0.0.1:7946
```

#### Server Flags
- `-name`: Unique name for this node (required).
- `-bind-addr`: Address to bind gRPC server (default "127.0.0.1:50051").
- `-gossip-addr`: Address to bind gossip (default "127.0.0.1:7946").
- `-join-addrs`: Comma-separated list of gossip addresses to join.

### CLI Usage
The G-Dist CLI allows you to interact with the database from the terminal.

#### Global Flags
- `--addr`: Server address (default "localhost:50051")

#### Commands

**Put a key-value pair:**
```bash
./g-dist-cli put <key> <value>
```
Example: `./g-dist-cli put mykey myvalue`

**Get a value by key:**
```bash
./g-dist-cli get <key>
```
Example: `./g-dist-cli get mykey`

**Delete a key-value pair:**
```bash
./g-dist-cli delete <key>
```
Example: `./g-dist-cli delete mykey`

**Check cluster status:**
```bash
./g-dist-cli status
```


