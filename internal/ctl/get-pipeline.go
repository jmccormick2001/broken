package ctl

import (
	"context"

	"gitlab.com/churro-group/churro/rpc/ctl"
	pb "gitlab.com/churro-group/churro/rpc/ctl"
)

// getPipeline gets information on a pipeline
func (s *Server) GetPipeline(ctx context.Context, req *ctl.GetPipelineRequest) (*pb.GetPipelineResponse, error) {
	//serviceCrt := s.ServiceCreds.ServiceCrt

	resp := &pb.GetPipelineResponse{
		LoaderStatus: "ok",
		WatchStatus:  "ok",
	}

	// ping the loader service

	/**
	url := fmt.Sprintf("%s:%d", pipeline.Spec.LoaderConfig.Location.Host, pipeline.Spec.LoaderConfig.Location.Port)
	s.logger.Debug("loader target url", zap.String("url", url))

	creds, err := credentials.NewClientTLSFromFile(serviceCrt, "")
	if err != nil {
		return nil, fmt.Errorf("could not process the credentials: %v", err)
	}

	conn, err := grpc.Dial(url, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, fmt.Errorf("dial failed to connect: %v", err)
	}
	defer conn.Close()
	loaderclient := pbloader.NewLoaderClient(conn)

	_, err = loaderclient.Ping(ctx, &pbloader.PingRequest{})
	if err != nil {
		return nil, fmt.Errorf("could not ping loader %v", err)
	}

	// ping the watch service

	url = fmt.Sprintf("%s:%d", pipeline.Spec.WatchConfig.Location.Host, pipeline.Spec.WatchConfig.Location.Port)
	s.logger.Debug("watch target url ", zap.String("url", url))

	watchconn, watcherr := grpc.Dial(url, grpc.WithTransportCredentials(creds))
	if watcherr != nil {
		return nil, fmt.Errorf("did not connect: %v", watcherr)
	}
	defer watchconn.Close()
	watchclient := pbwatch.NewWatchClient(watchconn)

	_, err = watchclient.Ping(ctx, &pbwatch.PingRequest{})
	if err != nil {
		return nil, fmt.Errorf("could not ping watch %v", err)
	}
	*/

	return resp, nil
}
