package main

import (
	"context"
	"log"
	"net"

	pb "github.com/himank/g-dist/api/v1"
	"github.com/himank/g-dist/pkg/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type server struct {
	pb.UnimplementedGDistServer
	storage *storage.Storage
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

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterGDistServer(s, &server{
		storage: storage.NewStorage(),
	})
	reflection.Register(s)
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
