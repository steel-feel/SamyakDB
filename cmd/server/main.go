package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"strings"

	pb "github.com/himank/g-dist/api/v1"
	"github.com/himank/g-dist/pkg/discovery"
	"github.com/himank/g-dist/pkg/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var (
	nodeName   = flag.String("name", "", "Unique name for this node")
	bindAddr   = flag.String("bind-addr", "127.0.0.1:50051", "Address to bind gRPC server")
	gossipAddr = flag.String("gossip-addr", "127.0.0.1:7946", "Address to bind gossip")
	joinAddrs  = flag.String("join-addrs", "", "Comma-separated list of addresses to join")
)

type server struct {
	pb.UnimplementedGDistServer
	storage    *storage.Storage
	membership *discovery.Membership
}

func (s *server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	s.storage.Put(req.Key, req.Value)
	return &pb.PutResponse{Success: true}, nil
}

func (s *server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	val, found := s.storage.Get(req.Key)
	return &pb.GetResponse{Value: val, Found: found}, nil
}

func (s *server) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
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

type handler struct{}

func (h *handler) Join(name, addr string) error {
	log.Printf("Node joined: %s at %s", name, addr)
	return nil
}

func (h *handler) Leave(name string) error {
	log.Printf("Node left: %s", name)
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

	s := &server{
		storage: storage.NewStorage(),
	}

	m, err := discovery.New(&handler{}, discovery.Config{
		NodeName:       *nodeName,
		BindAddr:       *gossipAddr,
		StartJoinAddrs: startJoinAddrs,
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