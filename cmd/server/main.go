package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"

	pb "github.com/steel-feel/SamyakDB/api/v1"
	"github.com/steel-feel/SamyakDB/pkg/discovery"
	"github.com/steel-feel/SamyakDB/pkg/sharding"
	"github.com/steel-feel/SamyakDB/pkg/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
)

var (
	nodeName   = flag.String("name", "", "Unique name for this node")
	bindAddr   = flag.String("bind-addr", "127.0.0.1:50051", "Address to bind gRPC server")
	gossipAddr = flag.String("gossip-addr", "127.0.0.1:7946", "Address to bind gossip")
	joinAddrs  = flag.String("join-addrs", "", "Comma-separated list of addresses to join")
	replicas   = flag.Int("replicas", 256, "Number of virtual nodes per physical node")
)

type server struct {
	pb.UnimplementedGDistServer
	name       string
	storage    *storage.Storage
	membership *discovery.Membership
	hashRing   *sharding.HashRing
	
	mu            sync.RWMutex
	nodeAddresses map[string]string
	clients       map[string]pb.GDistClient
}

func newServer(name string) *server {
	return &server{
		name:          name,
		storage:       storage.NewStorage(),
		hashRing:      sharding.NewHashRing(*replicas),
		nodeAddresses: make(map[string]string),
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

func (s *server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	owner := s.hashRing.GetNode(req.Key)
	if owner != s.name {
		log.Printf("Forwarding Put(%s) to %s", req.Key, owner)
		client, err := s.getClient(owner)
		if err != nil {
			return nil, err
		}
		return client.Put(ctx, req)
	}

	s.storage.Put(req.Key, req.Value)
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

	success := s.storage.Delete(req.Key)
	return &pb.DeleteResponse{Success: success}, nil
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
	if rpcAddr == "" {
		log.Printf("Warning: Node %s joined without rpc_addr", name)
		return nil
	}

	h.srv.mu.Lock()
	h.srv.nodeAddresses[name] = rpcAddr
	h.srv.mu.Unlock()
	
	h.srv.hashRing.AddNode(name)
	return nil
}

func (h *handler) Leave(name string) error {
	log.Printf("Node left: %s", name)
	h.srv.mu.Lock()
	delete(h.srv.nodeAddresses, name)
	delete(h.srv.clients, name)
	h.srv.mu.Unlock()
	
	h.srv.hashRing.RemoveNode(name)
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

	s := newServer(*nodeName)

	m, err := discovery.New(&handler{srv: s}, discovery.Config{
		NodeName:       *nodeName,
		BindAddr:       *gossipAddr,
		StartJoinAddrs: startJoinAddrs,
		Tags: map[string]string{
			"rpc_addr": *bindAddr,
		},
	})
	if err != nil {
		log.Fatalf("failed to setup membership: %v", err)
	}
	s.membership = m

	grpcServer := grpc.NewServer()
	pb.RegisterGDistServer(grpcServer, s)
	reflection.Register(grpcServer)

	log.Printf("server %s listening at %v", *nodeName, lis.Addr())
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}