# G-Dist

G-Dist is a distributed, strongly consistent Key-Value database designed for high availability and horizontal scalability. It employs a multi-layered architecture using Gossip for membership, Consistent Hashing for data distribution, and Raft for consensus-backed replication.

## Architecture
*   **Strong Consistency (CP):** Guarantee that all reads return the most recent write within a shard.
*   **Horizontal Scalability:** Ability to add nodes with minimal data reshuffling.
*   **Decentralized Discovery:** No "master" node for cluster membership.
*   **Fault Tolerance:** Automatic leader re-election and data recovery after node failure.


