package extract

import (
	"context"
	"fmt"
	"go.uber.org/zap"

	"github.com/golang/snappy"
	pb "gitlab.com/churro-group/churro/rpc/loader"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"os"
)

const (
	RecordsPerPush = 10
)

func (s *Server) pushToLoader(ctx context.Context, scheme string) {

	url := fmt.Sprintf("%s:%d", s.Pi.Spec.LoaderConfig.Location.Host, s.Pi.Spec.LoaderConfig.Location.Port)
	s.logger.Debug("loader target url", zap.String("url", url))

	creds, err := credentials.NewClientTLSFromFile(s.ServiceCreds.ServiceCrt, "")
	if err != nil {
		s.logger.Error("could not process the credentials", zap.Error(err))
		os.Exit(1)
	}

	conn, err := grpc.Dial(url, grpc.WithTransportCredentials(creds))
	if err != nil {
		s.logger.Error("did not connect:", zap.Error(err))
		return
	}
	defer conn.Close()
	loaderclient := pb.NewLoaderClient(conn)

	for {
		select {
		case elem := <-s.Queue:
			s.logger.Debug("extract pushing to loader")
			encoded := snappy.Encode(nil, elem.Metadata)
			pushResponse, err := loaderclient.Push(ctx, &pb.PushRequest{DataFormat: scheme, MessageCompressed: encoded})
			if err != nil {
				s.logger.Error("error in push", zap.Error(err))
			} else {
				s.logger.Debug("pushResponse ", zap.Int32("backpressure", pushResponse.Backpressure))
				if pushResponse.Backpressure == 1 {
					backPressure = 1
				} else {
					backPressure = 0
				}
			}
		case <-ctx.Done():
			s.logger.Info("done received in pushToLoader")
			return

		}
	}

}
