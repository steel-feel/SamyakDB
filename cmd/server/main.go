package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	pb "github.com/steel-feel/SamyakDB/api/v1"
	"github.com/steel-feel/SamyakDB/pkg/consensus"
	"github.com/steel-feel/SamyakDB/pkg/discovery"
	"github.com/steel-feel/SamyakDB/pkg/sharding"
	"github.com/steel-feel/SamyakDB/pkg/storage"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
)

var (
	nodeName      = flag.String("name", "", "Unique name for this node")
	bindAddr      = flag.String("bind-addr", "127.0.0.1:50051", "Address to bind gRPC server")
	gossipAddr    = flag.String("gossip-addr", "127.0.0.1:7946", "Address to bind gossip")
	joinAddrs     = flag.String("join-addrs", "", "Comma-separated list of addresses to join")
	replicas      = flag.Int("replicas", 256, "Number of virtual nodes per physical node")
	raftAddr      = flag.String("raft-addr", "127.0.0.1:8000", "Address to bind Raft")
	raftDir       = flag.String("raft-dir", "raft_data", "Directory for Raft data")
	raftBootstrap = flag.Bool("raft-bootstrap", false, "Bootstrap the Raft cluster")
)

type server struct {
	pb.UnimplementedGDistServer
	name       string
	storage    *storage.Storage
	membership *discovery.Membership
	hashRing   *sharding.HashRing
	raftNode   *consensus.RaftNode

	mu            sync.RWMutex
	nodeAddresses map[string]string // name -> rpc_addr
	raftAddresses map[string]string // raft_addr -> rpc_addr (for leader forwarding)
	clients       map[string]pb.GDistClient
}

func newServer(name string) *server {
	return &server{
		name:          name,
		storage:       storage.NewStorage(),
		hashRing:      sharding.NewHashRing(*replicas),
		nodeAddresses: make(map[string]string),
		raftAddresses: make(map[string]string),
		clients:       make(map[string]pb.GDistClient),
	}
}

func (s *server) getClient(node string) (pb.GDistClient, error) {
	s.mu.RLock()
	client, ok := s.clients[node]
	addr := s.nodeAddresses[node]
	s.mu.RUnlock()

	if ok {
		return client, nil
	}

	if addr == "" {
		return nil, fmt.Errorf("no address for node %s", node)
	}

	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	client = pb.NewGDistClient(conn)
	s.mu.Lock()
	s.clients[node] = client
	s.mu.Unlock()

	return client, nil
}

// getClientByAddr finds a client based on the RPC address.
// This is useful when we know the leader's RPC address (mapped from Raft address)
func (s *server) getClientByAddr(rpcAddr string) (pb.GDistClient, error) {
	s.mu.RLock()
	// Reverse lookup name from address? Or just dial directly.
	// Dialing directly is easier.
	// Check if we already have a client for this address?
	// We store clients by Node Name.
	// Let's just dial.
	s.mu.RUnlock()

	conn, err := grpc.Dial(rpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return pb.NewGDistClient(conn), nil
}

func (s *server) forwardToLeader(ctx context.Context, method string, req interface{}) (interface{}, error) {
	leaderAddr := s.raftNode.Raft.Leader()
	if leaderAddr == "" {
		return nil, fmt.Errorf("no leader known")
	}

	s.mu.RLock()
	rpcAddr, ok := s.raftAddresses[string(leaderAddr)]
	s.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("leader raft address %s not mapped to rpc address", leaderAddr)
	}

	log.Printf("Forwarding %s to leader at %s (raft: %s)", method, rpcAddr, leaderAddr)
	client, err := s.getClientByAddr(rpcAddr)
	if err != nil {
		return nil, err
	}

	switch method {
	case "Put":
		return client.Put(ctx, req.(*pb.PutRequest))
	case "Get":
		return client.Get(ctx, req.(*pb.GetRequest))
	case "Delete":
		return client.Delete(ctx, req.(*pb.DeleteRequest))
	default:
		return nil, fmt.Errorf("unknown method %s", method)
	}
}

func (s *server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	// First, check HashRing owner.
	owner := s.hashRing.GetNode(req.Key)
	if owner != s.name {
		// Even if not owner, if we are in the same Raft group, we should forward to Leader.
		// BUT, if the HashRing logic dictates "Sharding", then we must forward to the correct Shard Owner.
		// For M4 (Single Shard), 'owner' is just one of us.
		// If we assume "Single Shard", we can ignore HashRing forwarding?
		// Let's keep it to respect M3, but if it loops back to us (via Raft forwarding), it's fine?
		// Wait, if A -> B (Owner) -> B is Follower -> Forward to C (Leader) -> C applies.
		// This works.
		log.Printf("Forwarding Put(%s) to %s", req.Key, owner)
		client, err := s.getClient(owner)
		if err != nil {
			return nil, err
		}
		return client.Put(ctx, req)
	}

	// I am the owner (according to HashRing).
	// Now I must apply via Raft.
	if s.raftNode.Raft.State() != raft.Leader {
		resp, err := s.forwardToLeader(ctx, "Put", req)
		if err != nil {
			return nil, err
		}
		return resp.(*pb.PutResponse), nil
	}

	err := s.raftNode.Apply("put", req.Key, req.Value)
	if err != nil {
		return nil, err
	}
	return &pb.PutResponse{Success: true}, nil
}

func (s *server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	owner := s.hashRing.GetNode(req.Key)
	if owner != s.name {
		log.Printf("Forwarding Get(%s) to %s", req.Key, owner)
		client, err := s.getClient(owner)
		if err != nil {
			return nil, err
		}
		return client.Get(ctx, req)
	}

	// Strong consistency: Read from Leader or VerifyLeader
	if s.raftNode.Raft.State() != raft.Leader {
		// Forward to leader for strong consistency
		resp, err := s.forwardToLeader(ctx, "Get", req)
		if err != nil {
			// Fallback to local read or error?
			// Spec says "Strong Consistency".
			return nil, err
		}
		return resp.(*pb.GetResponse), nil
	}

	// Leader Read
	val, found := s.storage.Get(req.Key)
	return &pb.GetResponse{Value: val, Found: found}, nil
}

func (s *server) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	owner := s.hashRing.GetNode(req.Key)
	if owner != s.name {
		log.Printf("Forwarding Delete(%s) to %s", req.Key, owner)
		client, err := s.getClient(owner)
		if err != nil {
			return nil, err
		}
		return client.Delete(ctx, req)
	}

	if s.raftNode.Raft.State() != raft.Leader {
		resp, err := s.forwardToLeader(ctx, "Delete", req)
		if err != nil {
			return nil, err
		}
		return resp.(*pb.DeleteResponse), nil
	}

	err := s.raftNode.Apply("delete", req.Key, nil)
	if err != nil {
		return nil, err
	}
	return &pb.DeleteResponse{Success: true}, nil
}

func (s *server) Status(ctx context.Context, req *pb.StatusRequest) (*pb.ClusterStatus, error) {
	nodes := s.membership.Members()
	var members []*pb.Member
	for _, node := range nodes {
		members = append(members, &pb.Member{
			Name:   node.Name,
			Addr:   node.Addr.String(),
			Status: fmt.Sprintf("%d", node.State),
		})
	}
	return &pb.ClusterStatus{Members: members}, nil
}

type handler struct {
	srv *server
}

func (h *handler) Join(name, addr string, tags map[string]string) error {
	log.Printf("Node joined: %s at %s with tags %v", name, addr, tags)
	rpcAddr := tags["rpc_addr"]
	raftAddr := tags["raft_addr"]

	h.srv.mu.Lock()
	if rpcAddr != "" {
		h.srv.nodeAddresses[name] = rpcAddr
	}
	if raftAddr != "" {
		h.srv.raftAddresses[raftAddr] = rpcAddr
	}
	h.srv.mu.Unlock()
	
	h.srv.hashRing.AddNode(name)

	// Try to join Raft if we are Leader
	if h.srv.raftNode != nil && h.srv.raftNode.Raft.State() == raft.Leader && raftAddr != "" {
		go func() {
			// Small delay to ensure node is ready
			time.Sleep(2 * time.Second)
			if err := h.srv.raftNode.Join(name, raftAddr); err != nil {
				log.Printf("Failed to join node %s to raft: %v", name, err)
			}
		}()
	}

	return nil
}

func (h *handler) Leave(name string) error {
	log.Printf("Node left: %s", name)
	h.srv.mu.Lock()
	delete(h.srv.nodeAddresses, name)
	delete(h.srv.clients, name)
	// We can't easily delete from raftAddresses as we don't know the raft addr for the name easily here
	// simpler to leave it or improve data structure
	h.srv.mu.Unlock()
	
	h.srv.hashRing.RemoveNode(name)
	
	// Remove from Raft?
	// If leader, we could remove server.
	// But 'name' here is gossip name. Raft ID should be same as name if we configured it so.
	if h.srv.raftNode != nil && h.srv.raftNode.Raft.State() == raft.Leader {
		future := h.srv.raftNode.Raft.RemoveServer(raft.ServerID(name), 0, 0)
		if err := future.Error(); err != nil {
			log.Printf("Failed to remove node %s from raft: %v", name, err)
		}
	}

	return nil
}

func main() {
	flag.Parse()

	if *nodeName == "" {
		log.Fatal("node name is required")
	}

	lis, err := net.Listen("tcp", *bindAddr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	var startJoinAddrs []string
	if *joinAddrs != "" {
		startJoinAddrs = strings.Split(*joinAddrs, ",")
	}

	// Ensure raft dir exists
	if err := os.MkdirAll(*raftDir, 0755); err != nil {
		log.Fatalf("failed to create raft dir: %v", err)
	}

	s := newServer(*nodeName)

	// Setup Raft
	rNode, err := consensus.NewRaftNode(*nodeName, *raftDir, *raftAddr, s.storage, *raftBootstrap)
	if err != nil {
		log.Fatalf("failed to create raft node: %v", err)
	}
	s.raftNode = rNode
	s.raftAddresses[*raftAddr] = *bindAddr // Register self

	m, err := discovery.New(&handler{srv: s}, discovery.Config{
		NodeName:       *nodeName,
		BindAddr:       *gossipAddr,
		StartJoinAddrs: startJoinAddrs,
		Tags: map[string]string{
			"rpc_addr":  *bindAddr,
			"raft_addr": *raftAddr,
		},
	})
	if err != nil {
		log.Fatalf("failed to setup membership: %v", err)
	}
	s.membership = m

	grpcServer := grpc.NewServer()
	pb.RegisterGDistServer(grpcServer, s)
	reflection.Register(grpcServer)

	log.Printf("server %s listening at %v (Raft: %s)", *nodeName, lis.Addr(), *raftAddr)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}