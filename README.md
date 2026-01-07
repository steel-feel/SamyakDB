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
Start the G-Dist server:
```bash
./g-dist-server
```
The server will start listening on `:50051` by default.

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


