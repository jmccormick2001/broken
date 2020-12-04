package main

import (
	"context"
	"net"
	"os"
	"testing"
	"time"

	"gitlab.com/churro-group/churro/internal/watch"
	pb "gitlab.com/churro-group/churro/rpc/watch"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func Server() {
	l, _ := zap.NewDevelopment()
	defer l.Sync()
	logger = l.Sugar()

	lis, err := net.Listen("tcp", watch.DEFAULT_PORT)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
	s := grpc.NewServer()
	server := watch.Server{}
	pb.RegisterWatchServer(s, &server)
	if err := s.Serve(lis); err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
}

func TestMain(m *testing.M) {
	go Server()
	time.Sleep(2 * time.Second)
	os.Exit(m.Run())
}

func TestClient(t *testing.T) {
	logger.Info("churro-watch-test TestClient")

	conn, err := grpc.Dial(watch.DEFAULT_PORT, grpc.WithInsecure())
	if err != nil {
		t.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewWatchClient(conn)

	t.Run("Ping", func(t *testing.T) {
		_, err := c.Ping(context.Background(), &pb.PingRequest{})
		if err != nil {
			t.Fatalf("could not ping: %v", err)
		}

	})

}
