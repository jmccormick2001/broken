package main

import (
	"context"

	"gitlab.com/churro-group/churro/internal/extract"
	pb "gitlab.com/churro-group/churro/rpc/extract"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"net"
	"os"
	"testing"
	"time"
)

func Server() {
	l, _ := zap.NewDevelopment()
	defer l.Sync()
	logger := l.Sugar()

	lis, err := net.Listen("tcp", extract.DEFAULT_PORT)
	if err != nil {
		logger.Errorf("error in Listen %s\n", err.Error())
		os.Exit(1)
	}
	s := grpc.NewServer()
	eserver := extract.Server{}
	pb.RegisterExtractServer(s, &eserver)
	if err := s.Serve(lis); err != nil {
		logger.Errorf("error in Serve %s\n", err.Error())
		os.Exit(1)
	}
}

func TestMain(m *testing.M) {
	go Server()
	time.Sleep(2 * time.Second)
	os.Exit(m.Run())
}

func TestClient(t *testing.T) {

	// Set up a connection to the Server.
	conn, err := grpc.Dial(extract.DEFAULT_PORT, grpc.WithInsecure())
	if err != nil {
		t.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewExtractClient(conn)

	// Test SayHello
	t.Run("SayHello", func(t *testing.T) {
		r, err := c.Ping(context.Background(), &pb.PingRequest{Backpressure: 1})
		if err != nil {
			t.Fatalf("could not greet: %v", err)
		}
		t.Logf("backpressure response: %d", r.Backpressure)

	})
}
