package main

import (
	"context"

	"gitlab.com/churro-group/churro/internal/ctl"
	pb "gitlab.com/churro-group/churro/rpc/ctl"
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

	lis, err := net.Listen("tcp", ctl.DEFAULT_PORT)
	if err != nil {
		logger.Errorf("failed to listen %s\n", err.Error())
		os.Exit(1)
	}
	s := grpc.NewServer()
	server := ctl.Server{}
	pb.RegisterCtlServer(s, &server)
	if err := s.Serve(lis); err != nil {
		logger.Errorf("failed to serve %s\n", err.Error())
		os.Exit(1)
	}
}

func TestMain(m *testing.M) {
	go Server()
	time.Sleep(2 * time.Second)
	os.Exit(m.Run())
}

func TestClient(t *testing.T) {
	conn, err := grpc.Dial(ctl.DEFAULT_PORT, grpc.WithInsecure())
	if err != nil {
		t.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewCtlClient(conn)

	t.Run("Ping", func(t *testing.T) {
		_, err := c.Ping(context.Background(), &pb.PingRequest{})
		if err != nil {
			t.Fatalf("could not ping: %v", err)
		}

	})

}
