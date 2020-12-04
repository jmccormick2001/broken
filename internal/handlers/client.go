package handlers

import (
	"crypto/x509"
	"fmt"

	"gitlab.com/churro-group/churro/internal/ctl"
	pb "gitlab.com/churro-group/churro/rpc/ctl"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func GetServiceConnection(serviceCrt, namespace string) (client pb.CtlClient, err error) {

	url := fmt.Sprintf("churro-ctl.%s.svc.cluster.local%s", namespace, ctl.DEFAULT_PORT)

	//creds, err := credentials.NewClientTLSFromFile(serviceCrt, "")
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM([]byte(serviceCrt))
	creds := credentials.NewClientTLSFromCert(caCertPool, "")

	conn, err := grpc.Dial(url, grpc.WithTransportCredentials(creds))
	//conn, err := grpc.Dial(url, grpc.WithInsecure())
	if err != nil {
		return client, err
	}

	client = pb.NewCtlClient(conn)

	return client, nil

}
