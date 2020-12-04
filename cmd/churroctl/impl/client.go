package impl

import (
	"gitlab.com/churro-group/churro/internal/ctl"
	pb "gitlab.com/churro-group/churro/rpc/ctl"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func GetServiceConnection(logger *zap.SugaredLogger, serviceCrt, urlFlag string) (client pb.CtlClient, err error) {

	url := urlFlag
	if urlFlag == "" {
		url = ctl.DEFAULT_PORT
	}

	creds, err := credentials.NewClientTLSFromFile(serviceCrt, "")
	if err != nil {
		logger.Errorf("could not process the credentials %s\n", err.Error())
		return client, err
	}

	logger.Info("url ", zap.String("url", url))
	conn, err := grpc.Dial(url, grpc.WithTransportCredentials(creds))
	if err != nil {
		logger.Errorf("did not connect %s\n", err.Error())
		return client, err
	}

	client = pb.NewCtlClient(conn)

	return client, nil

}
