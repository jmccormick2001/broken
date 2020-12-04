package main

import (
	"context"

	"go.uber.org/zap"

	"net"
	"os"
	"testing"
	"time"

	"gitlab.com/churro-group/churro/internal/loader"
	pb "gitlab.com/churro-group/churro/rpc/loader"
	"google.golang.org/grpc"
)

func Server() {
	l, _ := zap.NewDevelopment()
	defer l.Sync()
	logger := l.Sugar()

	lis, err := net.Listen("tcp", loader.DEFAULT_PORT)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
	s := grpc.NewServer()
	server := loader.Server{}
	pb.RegisterLoaderServer(s, &server)
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

	conn, err := grpc.Dial(loader.DEFAULT_PORT, grpc.WithInsecure())
	if err != nil {
		t.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewLoaderClient(conn)

	t.Run("Ping", func(t *testing.T) {
		r, err := c.Ping(context.Background(), &pb.PingRequest{})
		if err != nil {
			t.Fatalf("could not ping: %v", err)
		}
		t.Logf("status code response: %d", r.StatusCode)

	})

}
