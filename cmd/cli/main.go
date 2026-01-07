package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	pb "github.com/himank/g-dist/api/v1"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	addr string
)

func main() {
	rootCmd := &cobra.Command{Use: "g-dist-cli"}
	rootCmd.PersistentFlags().StringVar(&addr, "addr", "localhost:50051", "server address")

	putCmd := &cobra.Command{
		Use:   "put [key] [value]",
		Short: "Put a key-value pair",
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			conn, client := getClient()
			defer conn.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			resp, err := client.Put(ctx, &pb.PutRequest{Key: args[0], Value: []byte(args[1])})
			if err != nil {
				log.Fatalf("could not put: %v", err)
			}
			fmt.Printf("Put success: %v\n", resp.Success)
		},
	}

	getCmd := &cobra.Command{
		Use:   "get [key]",
		Short: "Get a value by key",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			conn, client := getClient()
			defer conn.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			resp, err := client.Get(ctx, &pb.GetRequest{Key: args[0]})
			if err != nil {
				log.Fatalf("could not get: %v", err)
			}
			if resp.Found {
				fmt.Printf("Value: %s\n", string(resp.Value))
			} else {
				fmt.Println("Key not found")
			}
		},
	}

	deleteCmd := &cobra.Command{
		Use:   "delete [key]",
		Short: "Delete a key-value pair",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			conn, client := getClient()
			defer conn.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			resp, err := client.Delete(ctx, &pb.DeleteRequest{Key: args[0]})
			if err != nil {
				log.Fatalf("could not delete: %v", err)
			}
			fmt.Printf("Delete success: %v\n", resp.Success)
		},
	}

	rootCmd.AddCommand(putCmd, getCmd, deleteCmd)
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func getClient() (*grpc.ClientConn, pb.GDistClient) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	return conn, pb.NewGDistClient(conn)
}
