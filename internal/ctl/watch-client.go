package ctl

import (
	"context"
	"fmt"
	"gitlab.com/churro-group/churro/internal/watch"
	pb "gitlab.com/churro-group/churro/rpc/watch"
	"go.uber.org/zap"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// callers of this need to close the connection after using it
func (s *Server) getWatchConn() (conn *grpc.ClientConn, err error) {
	serviceCrt := s.ServiceCreds.ServiceCrt

	url := fmt.Sprintf("%s:%d", s.Pi.Spec.WatchConfig.Location.Host, s.Pi.Spec.WatchConfig.Location.Port)
	s.logger.Debug("watch target url", zap.String("url", url))

	creds, err := credentials.NewClientTLSFromFile(serviceCrt, "")
	if err != nil {
		return conn, fmt.Errorf("could not process the credentials: %v", err)
	}

	conn, err = grpc.Dial(url, grpc.WithTransportCredentials(creds))
	if err != nil {
		return conn, fmt.Errorf("dial failed to connect to watch service: %v", err)
	}

	return conn, nil

}

// send a create watchdir event to the churro-watch service
func (s *Server) createWatchDirectory(ctx context.Context, watchDir watch.WatchDirectory) (err error) {
	conn, err := s.getWatchConn()
	if err != nil {
		return fmt.Errorf("error connect to watch service: %v", err)
	}
	defer conn.Close()

	watchClient := pb.NewWatchClient(conn)
	req := pb.CreateWatchDirectoryRequest{}

	req.ConfigString = "foo"
	_, err = watchClient.CreateWatchDirectory(ctx, &req)
	if err != nil {
		return err
	}

	return nil

}

// send a delete watchdir event to the churro-watch service
func (s *Server) deleteWatchDirectory(ctx context.Context, watchName string) (err error) {
	conn, err := s.getWatchConn()
	if err != nil {
		return fmt.Errorf("error connect to watch service: %v", err)
	}
	defer conn.Close()

	watchClient := pb.NewWatchClient(conn)
	req := pb.DeleteWatchDirectoryRequest{}
	req.WatchName = watchName
	_, err = watchClient.DeleteWatchDirectory(ctx, &req)
	if err != nil {
		return err
	}

	return nil

}
